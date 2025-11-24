import os
import json
import sqlite3
from datetime import datetime
import uuid
from typing import Any
from functools import wraps
import re

from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify, send_file, after_this_request
from werkzeug.utils import secure_filename

import pandas as pd
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass
import requests


# ----------------------------
# App configuration (no .env)
# ----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "tmp", "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Invoices archive dir (persistent) and one-time download temp dir
INVOICES_DIR = os.getenv("INVOICES_DIR") or "/opt/generated_invoices"
DOWNLOAD_TMP_DIR = os.path.join(BASE_DIR, "tmp", "downloads")
os.makedirs(INVOICES_DIR, exist_ok=True)
os.makedirs(DOWNLOAD_TMP_DIR, exist_ok=True)

# Simple auth configuration
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME") or "admin"
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD") or "123456"

# Dedicated Preise database (exact headers, single table, full overwrite)
# Resolve DB location from environment for deployment persistence; fallback for local dev
PRICING_DB_PATH = os.getenv("PRICING_DB_PATH")
if not PRICING_DB_PATH:
    configured_db_dir = os.getenv("PRICING_DB_DIR")
    if configured_db_dir:
        PRICING_DB_PATH = os.path.join(configured_db_dir, "pricing_sheet.db")
    else:
        PRICING_DB_PATH = os.path.join(BASE_DIR, "pricing_sheet.db")

# Ensure the target directory for the SQLite file exists
os.makedirs(os.path.dirname(PRICING_DB_PATH), exist_ok=True)

# Dedicated Invoices metadata database (store metadata only; PDFs stay on disk)
INVOICES_DB_PATH = os.getenv("INVOICES_DB_PATH")
if not INVOICES_DB_PATH:
    configured_inv_db_dir = os.getenv("INVOICES_DB_DIR") or os.getenv("INVOICE_DB_DIR")
    if configured_inv_db_dir:
        INVOICES_DB_PATH = os.path.join(configured_inv_db_dir, "invoices.db")
    else:
        INVOICES_DB_PATH = os.path.join(BASE_DIR, "invoices.db")
os.makedirs(os.path.dirname(INVOICES_DB_PATH), exist_ok=True)

# Dedicated Client Headers database (store default headers per client)
CLIENT_HEADERS_DB_PATH = os.getenv("CLIENT_HEADERS_DB_PATH")
if not CLIENT_HEADERS_DB_PATH:
    configured_headers_db_dir = os.getenv("CLIENT_HEADERS_DB_DIR")
    if configured_headers_db_dir:
        CLIENT_HEADERS_DB_PATH = os.path.join(configured_headers_db_dir, "client_headers.db")
    else:
        # Use same directory as pricing DB for consistency
        CLIENT_HEADERS_DB_PATH = os.path.join(os.path.dirname(PRICING_DB_PATH), "client_headers.db")
os.makedirs(os.path.dirname(CLIENT_HEADERS_DB_PATH), exist_ok=True)

# Set your n8n webhook URL here directly
WEBHOOK_URL = os.getenv("INVOICE_WEBHOOK_URL")  # e.g., "http://localhost:5678/webhook/your-path"
# Two-phase flow configuration
GENERATE_PAYLOAD_JSON_WEBHOOK_URL = os.getenv("GENERATE_PAYLOAD_JSON_WEBHOOK_URL")  # Workflow 1
GENERATE_INVOICE_WEBHOOK_URL = os.getenv("GENERATE_INVOICE_WEBHOOK_URL")  # Workflow 2
USE_TWO_PHASE_FLOW = ((os.getenv("USE_TWO_PHASE_FLOW") or "false").strip().lower() in {"1", "true", "yes", "on"})
# Read timeout minutes for webhook response (default 10 minutes to allow for complex processing)
try:
    _timeout_min_raw = os.getenv("INVOICE_WEBHOOK_TIMEOUT_MIN")
    WEBHOOK_TIMEOUT_MIN = int(_timeout_min_raw) if _timeout_min_raw else 10
except Exception:
    WEBHOOK_TIMEOUT_MIN = 10
INFINITE_WEBHOOK_TIMEOUT = (WEBHOOK_TIMEOUT_MIN == 0)
if WEBHOOK_TIMEOUT_MIN < 0:
    WEBHOOK_TIMEOUT_MIN = 10
WEBHOOK_CONNECT_TIMEOUT_SEC = 60  # Increased from 30 to 60 seconds for connection
WEBHOOK_READ_TIMEOUT_SEC = WEBHOOK_TIMEOUT_MIN * 60

ALLOWED_EXCEL_EXTENSIONS = {".xlsx", ".xlsm"}
ALLOWED_PDF_EXTENSIONS = {".pdf"}

app = Flask(__name__)
app.config["SECRET_KEY"] = "dev-secret"
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB default


# ----------------------------
# Preise DB helpers (exact schema, no metadata) + Synonyms overlay
# ----------------------------
def get_pricing_db() -> sqlite3.Connection:
    conn = sqlite3.connect(PRICING_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def pricing_table_exists(conn: sqlite3.Connection) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='preise'")
    return cur.fetchone() is not None


def synonyms_table_exists(conn: sqlite3.Connection) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='synonyms'")
    return cur.fetchone() is not None


def ensure_synonyms_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS "synonyms" (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          Customer TEXT NOT NULL,
          Name TEXT NOT NULL,          -- base name from Preise
          Synonyms TEXT NOT NULL,      -- alias to expose
          match_score REAL,            -- 0..100
          created_at TEXT NOT NULL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_syn_customer ON synonyms(Customer)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_syn_customer_syn ON synonyms(Customer, Synonyms)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_syn_customer_name ON synonyms(Customer, Name)")
    conn.commit()


def drop_pricing_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS "preise"')
    conn.commit()


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def create_pricing_table(conn: sqlite3.Connection, headers: list[str]) -> None:
    # Build CREATE TABLE with quoted identifiers preserving spaces/newlines/umlauts
    cols_sql = ", ".join([f'{_quote_ident(h)} TEXT' for h in headers])
    sql = f'CREATE TABLE {_quote_ident("preise")} ({cols_sql})'
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


def insert_pricing_rows(conn: sqlite3.Connection, headers: list[str], rows: list[list[str | None]]) -> int:
    placeholders = ", ".join(["?"] * len(headers))
    cols = ", ".join([_quote_ident(h) for h in headers])
    sql = f'INSERT INTO {_quote_ident("preise")} ({cols}) VALUES ({placeholders})'
    cur = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()
    return cur.rowcount or 0


def get_preise_columns(conn: sqlite3.Connection) -> list[str]:
    cur = conn.cursor()
    cur.execute('PRAGMA table_info("preise")')
    return [row[1] for row in cur.fetchall()]


def get_kunde_col_from_cols(cols: list[str]) -> str | None:
    for c in cols:
        if c == "Kunde_Name":
            return c
    for c in cols:
        if str(c).strip().lower() == "kunde_name":
            return c
    return None


def get_productname_col_from_cols(cols: list[str]) -> str | None:
    # Prefer exact 'Produktname'
    for c in cols:
        if c == "Produktname":
            return c
    # Fallback case-insensitive trimmed match
    for c in cols:
        if str(c).strip().lower() == "produktname":
            return c
    # As an absolute fallback, accept 'Name'
    for c in cols:
        if c == "Name" or str(c).strip().lower() == "name":
            return c
    return None


def delete_s_rows_for_customers(conn: sqlite3.Connection, customers: list[str]) -> int:
    if not customers:
        return 0
    cols = get_preise_columns(conn)
    kunde_col = get_kunde_col_from_cols(cols)
    if not kunde_col or "record_source" not in cols:
        return 0
    q = ",".join(["?"] * len(customers))
    cur = conn.cursor()
    cur.execute(
        f'DELETE FROM {_quote_ident("preise")} WHERE {_quote_ident("record_source")} = ? AND {_quote_ident(kunde_col)} IN ({q})',
        ["S", *customers]
    )
    conn.commit()
    return cur.rowcount or 0


def insert_row_dict(conn: sqlite3.Connection, row_obj: dict) -> None:
    cols = list(row_obj.keys())
    placeholders = ", ".join(["?"] * len(cols))
    cols_sql = ", ".join([_quote_ident(c) for c in cols])
    sql = f'INSERT INTO {_quote_ident("preise")} ({cols_sql}) VALUES ({placeholders})'
    cur = conn.cursor()
    cur.execute(sql, [row_obj.get(c) for c in cols])
    conn.commit()


def rebuild_synonyms_into_preise(conn: sqlite3.Connection, customers_scope: list[str] | None = None, threshold: float = 85.0) -> dict:
    # Rebuild S duplicates into preise using stored definitions in synonyms table, matching against current P rows
    ensure_synonyms_table(conn)
    cols = get_preise_columns(conn)
    kunde_col = get_kunde_col_from_cols(cols)
    name_col = get_productname_col_from_cols(cols)
    if not kunde_col or not name_col:
        return {"inserted": 0, "unmatched": 0}

    # Fetch definitions in scope
    cur = conn.cursor()
    if customers_scope:
        q = ",".join(["?"] * len(customers_scope))
        cur.execute(f'SELECT Customer, Name, Synonyms FROM "synonyms" WHERE Customer IN ({q})', customers_scope)
    else:
        cur.execute('SELECT Customer, Name, Synonyms FROM "synonyms"')
    defs = cur.fetchall()

    inserted = 0
    unmatched = 0
    # Build column set for copy
    for d in defs:
        cust = str(d[0] or "").strip()
        base = str(d[1] or "").strip()
        alias = str(d[2] or "").strip()
        if not cust or not base or not alias:
            continue
        # Find best match within P rows for this customer
        cur.execute(
            f'SELECT * FROM {_quote_ident("preise")} WHERE {_quote_ident(kunde_col)} = ? AND {_quote_ident("record_source")} = ?'
            , (cust, "P")
        )
        base_rows = [dict(row) for row in cur.fetchall()]
        best_row, best_score = _best_match_base_row(base, base_rows, name_col)
        if best_row is None or best_score < threshold:
            # Second pass relaxed
            rthr = get_relaxed_threshold()
            best_row, best_score = _best_match_base_row_relaxed(base, base_rows, name_col)
            if best_row is None or best_score < rthr:
                unmatched += 1
                continue
        # Duplicate row: copy all columns, change product name, set record_source='S'
        dup = {c: best_row.get(c) for c in cols}
        dup[name_col] = alias
        if "record_source" in cols:
            dup["record_source"] = "S"
        else:
            # If column not present, skip (schema mismatch)
            continue
        insert_row_dict(conn, dup)
        inserted += 1
    return {"inserted": inserted, "unmatched": unmatched}


def get_match_threshold() -> float:
    try:
        val = float(os.getenv("MATCH_THRESHOLD") or 80)
    except Exception:
        val = 80.0
    # Clamp sensible bounds
    if val < 0:
        val = 0.0
    if val > 100:
        val = 100.0
    return val


def get_relaxed_threshold() -> float:
    try:
        val = float(os.getenv("MATCH_THRESHOLD_RELAXED") or (get_match_threshold() - 10))
    except Exception:
        val = max(get_match_threshold() - 10.0, 0.0)
    if val < 0:
        val = 0.0
    if val > 100:
        val = 100.0
    return val


def init_db() -> None:
    # Nothing to initialize for pricing DB beyond file existence; table is recreated on import
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    try:
        pconn = get_pricing_db()
        ensure_synonyms_table(pconn)
    except Exception:
        pass
    finally:
        try:
            pconn.close()
        except Exception:
            pass
    # Initialize invoices DB (metadata table)
    try:
        conn = sqlite3.connect(INVOICES_DB_PATH)
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS "invoices" (
              id TEXT PRIMARY KEY,
              name TEXT NOT NULL,
              client TEXT,
              file TEXT NOT NULL,
              size INTEGER NOT NULL,
              created_at TEXT NOT NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_invoices_created_at ON invoices(created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_invoices_client_created ON invoices(client, created_at)")
        # Draft invoices (two-phase flow)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS "draft_invoices" (
              draft_id TEXT PRIMARY KEY,
              client_name TEXT NOT NULL,
              invoice_name TEXT,
              payload_json TEXT NOT NULL,
              title_invoice TEXT,
              header_invoice TEXT,
              footer_invoice TEXT,
              currency_exchange TEXT,
              status TEXT DEFAULT 'draft',
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              finalized_at TEXT
            )
            """
        )
        # Add footer_invoice column if it doesn't exist (for existing databases)
        try:
            cur.execute("ALTER TABLE draft_invoices ADD COLUMN footer_invoice TEXT")
            conn.commit()
        except Exception:
            pass  # Column already exists
        cur.execute("CREATE INDEX IF NOT EXISTS idx_drafts_status_created ON draft_invoices(status, created_at)")
        conn.commit()
    except Exception:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass
    # Initialize client headers DB
    try:
        hconn = get_client_headers_db()
        ensure_client_headers_table(hconn)
    except Exception:
        pass
    finally:
        try:
            hconn.close()
        except Exception:
            pass

def get_invoices_db() -> sqlite3.Connection:
    conn = sqlite3.connect(INVOICES_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def get_client_headers_db() -> sqlite3.Connection:
    conn = sqlite3.connect(CLIENT_HEADERS_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_client_headers_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS "client_headers" (
          client_name TEXT PRIMARY KEY,
          default_header TEXT NOT NULL,
          default_footer TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    # Add default_footer column if it doesn't exist (for existing databases)
    try:
        cur.execute("ALTER TABLE client_headers ADD COLUMN default_footer TEXT")
        conn.commit()
    except Exception:
        pass  # Column already exists
    conn.commit()

def get_client_header(client_name: str) -> str | None:
    """Fetch the default header for a given client, or None if not set."""
    try:
        conn = get_client_headers_db()
        ensure_client_headers_table(conn)
        cur = conn.cursor()
        cur.execute("SELECT default_header FROM client_headers WHERE client_name = ?", (client_name,))
        row = cur.fetchone()
        return row["default_header"] if row else None
    except Exception:
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass

def get_client_footer(client_name: str) -> str | None:
    """Fetch the default footer for a given client, or None if not set."""
    try:
        conn = get_client_headers_db()
        ensure_client_headers_table(conn)
        cur = conn.cursor()
        cur.execute("SELECT default_footer FROM client_headers WHERE client_name = ?", (client_name,))
        row = cur.fetchone()
        return row["default_footer"] if row else None
    except Exception:
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass

def save_client_header(client_name: str, default_header: str) -> bool:
    """Save or update the default header for a client."""
    try:
        conn = get_client_headers_db()
        ensure_client_headers_table(conn)
        cur = conn.cursor()
        now_iso = datetime.utcnow().isoformat() + "Z"
        # Check if exists
        cur.execute("SELECT client_name FROM client_headers WHERE client_name = ?", (client_name,))
        exists = cur.fetchone() is not None
        if exists:
            cur.execute(
                "UPDATE client_headers SET default_header = ?, updated_at = ? WHERE client_name = ?",
                (default_header, now_iso, client_name)
            )
        else:
            cur.execute(
                "INSERT INTO client_headers (client_name, default_header, created_at, updated_at) VALUES (?, ?, ?, ?)",
                (client_name, default_header, now_iso, now_iso)
            )
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass

def save_client_footer(client_name: str, default_footer: str) -> bool:
    """Save or update the default footer for a client."""
    try:
        conn = get_client_headers_db()
        ensure_client_headers_table(conn)
        cur = conn.cursor()
        now_iso = datetime.utcnow().isoformat() + "Z"
        # Check if exists
        cur.execute("SELECT client_name FROM client_headers WHERE client_name = ?", (client_name,))
        exists = cur.fetchone() is not None
        if exists:
            cur.execute(
                "UPDATE client_headers SET default_footer = ?, updated_at = ? WHERE client_name = ?",
                (default_footer, now_iso, client_name)
            )
        else:
            cur.execute(
                "INSERT INTO client_headers (client_name, default_footer, created_at, updated_at) VALUES (?, ?, ?, ?)",
                (client_name, default_footer, now_iso, now_iso)
            )
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass

def list_all_client_headers() -> list[dict]:
    """List all client headers and footers."""
    try:
        conn = get_client_headers_db()
        ensure_client_headers_table(conn)
        cur = conn.cursor()
        cur.execute("SELECT client_name, default_header, default_footer, created_at, updated_at FROM client_headers ORDER BY client_name ASC")
        rows = cur.fetchall()
        return [{"client_name": r["client_name"], "default_header": r["default_header"], "default_footer": r.get("default_footer"), "created_at": r["created_at"], "updated_at": r["updated_at"]} for r in rows]
    except Exception:
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass

def add_invoice_db_record(inv_id: str, name: str, client: str, rel_pdf_path: str, size_bytes: int, created_at_iso: str) -> None:
    try:
        conn = get_invoices_db()
        cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO invoices (id, name, client, file, size, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (inv_id, name, client, rel_pdf_path, size_bytes, created_at_iso),
        )
        conn.commit()
    except Exception:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ----------------------------
# Legacy parsing removed: the new app does not infer or transform SKU/prices.
# ----------------------------


## Legacy helper functions were removed on purpose.


# ----------------------------
# Preise (exact) parsing utilities
# ----------------------------
def find_preise_sheet_name(xls: pd.ExcelFile) -> str | None:
    # 1) Exact case-insensitive match "Preise"
    for s in xls.sheet_names:
        if str(s).strip().lower() == "preise":
            return s
    # 2) Name contains token "preis"
    for s in xls.sheet_names:
        name = str(s).lower().replace("_", " ").replace("-", " ")
        if "preis" in name:
            return s
    # 3) Probe sheets for a header row that includes Kunde_Name (second non-empty row)
    for s in xls.sheet_names:
        try:
            raw = xls.parse(sheet_name=s, header=None, dtype=object, nrows=5)
            # drop leading fully empty rows
            while len(raw) > 0 and raw.iloc[0].isna().all():
                raw = raw.iloc[1:]
            if raw.empty:
                continue
            header_row = list(raw.iloc[0].values)
            for v in header_row:
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    continue
                if str(v).strip().lower() == "kunde_name":
                    return s
        except Exception:
            continue
        return None


def parse_preise_sheet_exact(xls: pd.ExcelFile, sheet_name: str) -> tuple[list[str], list[list[str | None]]]:
    # Read raw to preserve values; do not attempt dtype coercion
    raw = xls.parse(sheet_name=sheet_name, header=None, dtype=object)
    # REQUIREMENT: Column names are in row 2 (1-based). Row 1 is blank and must be ignored.
    if len(raw) < 2:
        return [], []
    header_row = list(raw.iloc[1].values)
    # Build headers exactly; keep newlines, spaces; ignore None/NaN headers entirely
    headers_raw: list[str | None] = []
    for v in header_row:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            headers_raw.append(None)
        else:
            headers_raw.append(str(v))
    # Resolve duplicates: keep the LAST occurrence; collect indices to keep
    last_index_for_header: dict[str, int] = {}
    for idx, h in enumerate(headers_raw):
        if h is None or h == "":
            continue
        last_index_for_header[h] = idx
    # Construct final headers and their source indices preserving original order by last occurrence position
    kept_indices = sorted(last_index_for_header.values())
    final_headers = [headers_raw[i] for i in kept_indices]
    # Extract data rows (everything after header row)
    # Data begins after header row (row index 2, 0-based)
    data = raw.iloc[2:]
    rows: list[list[str | None]] = []
    for _, r in data.iterrows():
        # Skip fully empty rows
        vals_all = list(r.values)
        if all((v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == "") for v in vals_all):
            continue
        row_vals: list[str | None] = []
        for col_idx in kept_indices:
            v = r.iloc[col_idx] if col_idx < len(r) else None
            if v is None or (isinstance(v, float) and pd.isna(v)):
                row_vals.append(None)
            else:
                row_vals.append(str(v))
        rows.append(row_vals)
    return final_headers, rows


def list_distinct_kunde_names(conn: sqlite3.Connection, query: str | None = None) -> list[str]:
    cur = conn.cursor()
    cur.execute('PRAGMA table_info("preise")')
    cols = [row[1] for row in cur.fetchall()]
    if not cols:
        return []
    # Strictly use the 'Kunde_Name' column
    kunde_col = None
    for c in cols:
        if c == "Kunde_Name":
            kunde_col = c
            break
    if not kunde_col:
        # allow forgiving variant: trim spaces and compare case-insensitive
        for c in cols:
            if str(c).strip().lower() == "kunde_name":
                kunde_col = c
                break
    if not kunde_col:
        return []
    qcol = _quote_ident(kunde_col)
    if query:
        sql = f'SELECT DISTINCT {qcol} FROM {_quote_ident("preise")} WHERE {qcol} LIKE ? ESCAPE "\\" ORDER BY {qcol} ASC'
        cur.execute(sql, (f"%{query}%",))
    else:
        sql = f'SELECT DISTINCT {qcol} FROM {_quote_ident("preise")} ORDER BY {qcol} ASC'
        cur.execute(sql)
    out = []
    for row in cur.fetchall():
        val = row[0]
        if val is None or str(val).strip() == "":
                continue
        out.append(val)
    return out


def fetch_rows_for_kunde(conn: sqlite3.Connection, kunde_name: str) -> list[dict]:
    cur = conn.cursor()
    # Determine all headers (columns) to include
    cur.execute('PRAGMA table_info("preise")')
    col_rows = cur.fetchall()
    headers = [row[1] for row in col_rows]
    if not headers:
        return []
    # Strictly use the 'Kunde_Name' column (with a forgiving trimmed variant)
    kunde_col = None
    for c in headers:
        if c == "Kunde_Name":
            kunde_col = c
            break
    if not kunde_col:
        for c in headers:
            if str(c).strip().lower() == "kunde_name":
                kunde_col = c
                break
    if not kunde_col:
        return []
    quoted_cols = ", ".join([_quote_ident(h) for h in headers])
    sql = f'SELECT {quoted_cols} FROM {_quote_ident("preise")} WHERE {_quote_ident(kunde_col)} = ?'
    cur.execute(sql, (kunde_name,))
    result = []
    for row in cur.fetchall():
        obj = {}
        for i, h in enumerate(headers):
            obj[h] = row[i]
        result.append(obj)
    return result


def fetch_synonyms_for_customer(conn: sqlite3.Connection, customer: str) -> list[sqlite3.Row]:
    ensure_synonyms_table(conn)
    cur = conn.cursor()
    cur.execute('SELECT id, Customer, Name, Synonyms, match_score, created_at FROM "synonyms" WHERE Customer = ?', (customer,))
    rows = cur.fetchall()
    return rows


def clear_synonyms_for_customers(conn: sqlite3.Connection, customers: list[str]) -> int:
    if not customers:
        return 0
    ensure_synonyms_table(conn)
    cur = conn.cursor()
    q = ",".join(["?"] * len(customers))
    cur.execute(f'DELETE FROM "synonyms" WHERE Customer IN ({q})', customers)
    conn.commit()
    return cur.rowcount or 0


def insert_synonym_rows(conn: sqlite3.Connection, rows: list[tuple[str, str, str, float, str]]) -> int:
    # rows: (Customer, Name, Synonyms, match_score, created_at)
    if not rows:
        return 0
    ensure_synonyms_table(conn)
    cur = conn.cursor()
    cur.executemany(
        'INSERT INTO "synonyms" (Customer, Name, Synonyms, match_score, created_at) VALUES (?, ?, ?, ?, ?)',
        rows,
    )
    conn.commit()
    return cur.rowcount or 0


# ----------------------------
# i18n / language
# ----------------------------
TRANSLATIONS = {
    "en": {
        "brand": "Generate Invoice",
        "nav_feeddata": "Feed data",
        "nav_invoicecreation": "Invoice creation",
        "nav_clientheaders": "Client Meta",
        "feed_title": "Upload price sheet",
        "client_headers_title": "Manage Client Meta Information",
        "client_headers_select_client": "Select Client",
        "client_headers_tab_header": "Headers",
        "client_headers_tab_footer": "Footers",
        "client_headers_default_header": "Default Header",
        "client_headers_default_footer": "Default Footer",
        "client_headers_placeholder": "Enter the default header for this client...",
        "client_footers_placeholder": "Enter the default footer for this client...",
        "client_headers_save": "Save Header",
        "client_footers_save": "Save Footer",
        "client_headers_clear": "Clear",
        "client_headers_success": "Header saved successfully for client '{client}'.",
        "client_footers_success": "Footer saved successfully for client '{client}'.",
        "client_headers_error": "Error saving header: {error}",
        "client_footers_error": "Error saving footer: {error}",
        "client_headers_list_title": "Existing Client Headers",
        "client_footers_list_title": "Existing Client Footers",
        "client_headers_no_data": "No client headers configured yet.",
        "client_footers_no_data": "No client footers configured yet.",
        "client_name": "Client name",
        "excel_label": "Excel (.xlsx / .xlsm)",
        "currency": "Currency",
        "valid_from": "Valid from",
        "valid_to": "Valid to",
        "overwrite_hint": "Each sheet tab will be imported as a separate client named by its sheet. Strict overwrite per sheet.",
        "import_button": "Import",
        "invoice_title": "Invoice creation",
        "dropdown_label": "Customer (from Preise)",
        "dropdown_label_search": "Customer (from Preise) + Search",
        "search_placeholder": "Search or choose...",
        "delivery_label": "Delivery notes (PDF, multiple)",
        "generate_button": "Generate invoice",
        "select_placeholder": "Please select",
        "flash_missing_file": "Please choose an Excel file.",
        "flash_client_required": "'Client name' is required.",
        "flash_excel_only": "Only .xlsx/.xlsm files are supported.",
        "flash_import_success": "Imported price sheet for '{client}' with {count} items.",
        "flash_import_error": "Import error: {error}",
        "webhook_not_set": "Webhook URL is not configured. Please set it in app.py.",
        "flash_webhook_ok": "Webhook request sent successfully.",
        "flash_webhook_fail": "Webhook error: {status}",
        "flash_webhook_send_error": "Error sending to webhook: {error}",
        "invoice_name": "Invoice name",
        "invoices": "Invoices",
        "please_wait": "Please wait...",
        "preview": "Preview",
        "download": "Download",
        "rename": "Rename",
        "filters": "Filters",
        "from": "From",
        "to": "To",
        "sort_newest": "Newest",
        "sort_oldest": "Oldest",
        "no_invoices": "No invoices yet.",
        "login": "Login",
        "logout": "Logout",
        "login_title": "Login",
        "username": "Username",
        "password": "Password",
        "login_button": "Login",
        "invalid_credentials": "Invalid credentials",
        "apply": "Apply",
        "delete": "Delete",
        "invoice_name_placeholder": "enter the name of invoice",
        "title_label": "Title",
        "title_placeholder": "e.g., November 2025 Sales Summary",
        "header_label": "Header",
        "header_placeholder": "Paste or write the header paragraph here",
        "eta_label": "Time elapsed:",
        "name_available": "Name available",
        "name_taken": "Invoice with this name already exists. Try adding a date (e.g., 20251014) or a number.",
        "request_failed": "Request failed",
        "error_generic": "Error",
        "syn_upload_title": "Upload synonym sheet",
        "syn_excel_label": "Excel (.xlsx / .xlsm / .xls)",
        "syn_overwrite_hint": "Full-replace S data for customers present in this file.",
        "syn_import_button": "Import Synonyms",
        "download_preise": "Download current price sheet (XLSX)",
        "prev": "Prev",
        "next": "Next",
    },
    "de": {
        "brand": "Rechnung erstellen",
        "nav_feeddata": "Einspeisen",
        "nav_invoicecreation": "Rechnungserstellung",
        "nav_clientheaders": "Kunden-Meta",
        "feed_title": "Preisliste hochladen",
        "client_headers_title": "Kunden-Meta-Informationen verwalten",
        "client_headers_select_client": "Kunde auswählen",
        "client_headers_tab_header": "Header",
        "client_headers_tab_footer": "Footer",
        "client_headers_default_header": "Standard-Header",
        "client_headers_default_footer": "Standard-Footer",
        "client_headers_placeholder": "Geben Sie den Standard-Header für diesen Kunden ein...",
        "client_footers_placeholder": "Geben Sie den Standard-Footer für diesen Kunden ein...",
        "client_headers_save": "Header speichern",
        "client_footers_save": "Footer speichern",
        "client_headers_clear": "Löschen",
        "client_headers_success": "Header erfolgreich gespeichert für Kunde '{client}'.",
        "client_footers_success": "Footer erfolgreich gespeichert für Kunde '{client}'.",
        "client_headers_error": "Fehler beim Speichern des Headers: {error}",
        "client_footers_error": "Fehler beim Speichern des Footers: {error}",
        "client_headers_list_title": "Vorhandene Kundenheader",
        "client_footers_list_title": "Vorhandene Kundenfooter",
        "client_headers_no_data": "Noch keine Kundenheader konfiguriert.",
        "client_footers_no_data": "Noch keine Kundenfooter konfiguriert.",
        "client_name": "Client name",
        "excel_label": "Excel (.xlsx / .xlsm)",
        "currency": "Währung",
        "valid_from": "Gültig ab",
        "valid_to": "Gültig bis",
        "overwrite_hint": "Jeder Tabellenreiter wird als eigener Client (Name = Blattname) importiert. Striktes Überschreiben pro Blatt.",
        "import_button": "Importieren",
        "invoice_title": "Rechnungserstellung",
        "dropdown_label": "Kunde (aus Preise)",
        "dropdown_label_search": "Kunde (aus Preise) + Suche",
        "search_placeholder": "Suche oder wählen...",
        "delivery_label": "Lieferscheine (PDF, mehrere möglich)",
        "generate_button": "Rechnung erstellen",
        "select_placeholder": "Bitte auswählen",
        "flash_missing_file": "Bitte eine Excel-Datei auswählen.",
        "flash_client_required": "'Client name' ist erforderlich.",
        "flash_excel_only": "Nur .xlsx/.xlsm Dateien werden unterstützt.",
        "flash_import_success": "Preisliste für '{client}' importiert: {count} Positionen.",
        "flash_import_error": "Fehler beim Import: {error}",
        "webhook_not_set": "Webhook URL ist nicht konfiguriert. Bitte in app.py setzen.",
        "flash_webhook_ok": "Webhook-Anfrage erfolgreich gesendet.",
        "flash_webhook_fail": "Webhook Fehler: {status}",
        "flash_webhook_send_error": "Fehler beim Senden an Webhook: {error}",
        "invoice_name": "Rechnungsname",
        "invoices": "Rechnungen",
        "please_wait": "Bitte warten...",
        "preview": "Vorschau",
        "download": "Herunterladen",
        "rename": "Umbenennen",
        "filters": "Filter",
        "from": "Von",
        "to": "Bis",
        "sort_newest": "Neueste",
        "sort_oldest": "Älteste",
        "no_invoices": "Noch keine Rechnungen.",
        "login": "Anmelden",
        "logout": "Abmelden",
        "login_title": "Anmeldung",
        "username": "Benutzername",
        "password": "Passwort",
        "login_button": "Anmelden",
        "invalid_credentials": "Ungültige Zugangsdaten",
        "apply": "Anwenden",
        "delete": "Löschen",
        "invoice_name_placeholder": "Name der Rechnung eingeben",
        "title_label": "Titel",
        "title_placeholder": "z.\u202fB. Verkaufsübersicht November 2025",
        "header_label": "Kopfzeile",
        "header_placeholder": "Fügen Sie hier den Kopftext ein oder schreiben Sie ihn",
        "eta_label": "Geschätzte Restzeit:",
        "name_available": "Name verfügbar",
        "name_taken": "Eine Rechnung mit diesem Namen existiert bereits. Fügen Sie ein Datum (z.\u202fB. 20251014) oder eine Zahl hinzu.",
        "request_failed": "Anfrage fehlgeschlagen",
        "error_generic": "Fehler",
        "syn_upload_title": "Synonymliste hochladen",
        "syn_excel_label": "Excel (.xlsx / .xlsm / .xls)",
        "syn_overwrite_hint": "Ersetzt S-Daten für die im Upload enthaltenen Kunden.",
        "syn_import_button": "Synonyme importieren",
        "download_preise": "Aktuelle Preisliste herunterladen (XLSX)",
        "prev": "Zurück",
        "next": "Weiter",
    },
}


def get_lang() -> str:
    lang = session.get("lang", "en")
    return "de" if lang == "de" else "en"


def tr(key: str, **kwargs) -> str:
    lang = get_lang()
    text = TRANSLATIONS.get(lang, {}).get(key, key)
    try:
        return text.format(**kwargs)
    except Exception:
        return text


@app.context_processor
def inject_i18n():
    return {"t": tr, "lang": get_lang(), "is_authed": bool(session.get("auth"))}

# Ensure DBs/tables are initialized on import (works with Gunicorn)
try:
    init_db()
except Exception:
    pass


@app.get("/set-lang")
def set_lang():
    lang = (request.args.get("lang") or "").lower()
    if lang not in {"en", "de"}:
        lang = "en"
    session["lang"] = lang
    ref = request.headers.get("Referer")
    return redirect(ref or url_for("index"))


# ----------------------------
# Routes
# ----------------------------

def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("auth"):
            nxt = request.path
            return redirect(url_for("login_get", next=nxt))
        return view(*args, **kwargs)
    return wrapped


@app.get("/login")
def login_get():
    if session.get("auth"):
        return redirect(url_for("invoicecreation_get"))
    return render_template("login.html", next=request.args.get("next", ""))


@app.post("/login")
def login_post():
    username = (request.form.get("username") or "").strip()
    password = (request.form.get("password") or "").strip()
    if username == ADMIN_USERNAME and password == ADMIN_PASSWORD:
        session["auth"] = True
        dest = request.args.get("next") or url_for("invoicecreation_get")
        return redirect(dest)
    flash(tr("invalid_credentials"), "error")
    return redirect(url_for("login_get"))


@app.get("/logout")
def logout():
    session.pop("auth", None)
    return redirect(url_for("login_get"))
@app.route("/")
def index():
    if session.get("auth"):
        return redirect(url_for("invoicecreation_get"))
    return redirect(url_for("login_get"))

@app.get("/health")
def health():
    return "ok", 200


@app.get("/feeddata")
@login_required
def feeddata_get():
    return render_template("feeddata.html")


@app.get("/clientheaders")
@login_required
def clientheaders_get():
    # Get all clients from pricing DB for the dropdown
    q = (request.args.get("q") or "").strip()
    try:
        pconn = get_pricing_db()
        if not pricing_table_exists(pconn):
            clients = []
        else:
            clients = list_distinct_kunde_names(pconn, q if q else None)
        pconn.close()
    except Exception:
        clients = []
    
    # Get existing client headers
    existing_headers = list_all_client_headers()
    
    return render_template("clientheaders.html", clients=clients, existing_headers=existing_headers, q=q)


@app.get("/preise/download")
@login_required
def preise_download():
    # Export the entire current Preise table (all columns, all rows) to XLSX
    try:
        pconn = get_pricing_db()
        if not pricing_table_exists(pconn):
            pconn.close()
            return "No pricing data", 404
        cur = pconn.cursor()
        cur.execute('PRAGMA table_info("preise")')
        cols = [row[1] for row in cur.fetchall()]
        if not cols:
            pconn.close()
            return "No pricing data", 404
        qcols = ", ".join([_quote_ident(c) for c in cols])
        cur.execute(f'SELECT {qcols} FROM {_quote_ident("preise")}')
        rows = cur.fetchall()
        pconn.close()

        # Build DataFrame and write to a temp file
        data = [[r[c] for c in cols] for r in rows]
        df = pd.DataFrame(data, columns=cols)
        tmp_xlsx = os.path.join(DOWNLOAD_TMP_DIR, f"preise_export_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx")
        with pd.ExcelWriter(tmp_xlsx, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Preise')
        return send_file(tmp_xlsx, as_attachment=True, download_name='preise_latest.xlsx', mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.post("/feeddata")
@login_required
def feeddata_post():
    if "file" not in request.files:
        flash(tr("flash_missing_file"), "error")
        return redirect(url_for("feeddata_get"))

    excel_file = request.files["file"]
    currency = (request.form.get("currency") or "").strip() or None
    valid_from = (request.form.get("valid_from") or "").strip() or None
    valid_to = (request.form.get("valid_to") or "").strip() or None

    # No explicit client name: each worksheet becomes a client by its sheet name

    filename = secure_filename(excel_file.filename or "")
    ext = os.path.splitext(filename)[1].lower()
    if ext not in ALLOWED_EXCEL_EXTENSIONS:
        flash(tr("flash_excel_only"), "error")
        return redirect(url_for("feeddata_get"))

    temp_path = os.path.join(UPLOAD_DIR, filename)
    excel_file.save(temp_path)

    ok_details = []
    fail_details = []

    try:
        xls = pd.ExcelFile(temp_path)
        # Preise-only import into dedicated pricing_sheet.db with exact headers
        preise_sheet = find_preise_sheet_name(xls)
        if not preise_sheet:
            raise ValueError("Sheet 'Preise' not found (case-insensitive).")

        headers, rows = parse_preise_sheet_exact(xls, preise_sheet)
        if not headers:
            raise ValueError("No headers found in 'Preise' sheet.")
        # Sanity check: ensure 'Kunde_Name' column exists after our duplicate-resolution logic
        if not any((h == "Kunde_Name" or str(h).strip().lower() == "kunde_name") for h in headers):
            raise ValueError("'Kunde_Name' column not found in header row.")
        # Add record_source column to headers and set 'P' for all imported rows
        if not any(h == "record_source" for h in headers):
            headers = list(headers) + ["record_source"]
            rows = [list(r) + ["P"] for r in rows]

        # Create table and insert (full overwrite of P+S, but we will rebuild S right after)
        pconn = get_pricing_db()
        drop_pricing_table(pconn)
        create_pricing_table(pconn, headers)
        inserted = insert_pricing_rows(pconn, headers, rows)

        # Reapply synonyms definitions into the freshly imported table as S rows
        try:
            stats = rebuild_synonyms_into_preise(pconn, customers_scope=None, threshold=get_match_threshold())
        except Exception:
            stats = {"inserted": 0, "unmatched": 0}
        finally:
            pconn.close()

        flash(f"Preise sheet imported with {inserted} rows. Synonyms added: {stats.get('inserted',0)}.", "success")
        return redirect(url_for("feeddata_get"))

    except Exception as e:
        flash(tr("flash_import_error", error=str(e)), "error")
        return redirect(url_for("feeddata_get"))
    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


@app.get("/invoicecreation")
@login_required
def invoicecreation_get():
    q = (request.args.get("q") or "").strip()
    try:
        pconn = get_pricing_db()
        # If table not present yet, no clients
        if not pricing_table_exists(pconn):
            clients = []
        else:
            clients = list_distinct_kunde_names(pconn, q if q else None)
        pconn.close()
    except Exception:
        clients = []
    return render_template("invoicecreation.html", clients=clients, q=q)


def build_pricing_json_for_client(client_name: str) -> list[dict]:
    # Build combined P + S rows for the client
    pconn = get_pricing_db()
    try:
        if not pricing_table_exists(pconn):
            return []
        base_rows = fetch_rows_for_kunde(pconn, client_name)
        # Determine the product name column
        name_col = None
        if base_rows:
            for k in base_rows[0].keys():
                if k == "Name" or str(k).strip().lower() == "name":
                    name_col = k
                    break
        # Start with all P rows as-is (no extra fields to keep payload stable)
        out: list[dict] = [dict(r) for r in base_rows]
        # Produce S duplicates from synonyms table if we can resolve name column
        if name_col:
            syn_rows = fetch_synonyms_for_customer(pconn, client_name)
            # Map base name -> list of base rows (handle possible duplicates)
            from collections import defaultdict
            base_map: dict[str, list[dict]] = defaultdict(list)
            for r in base_rows:
                key = str(r.get(name_col) or "").strip()
                if key:
                    base_map[key].append(r)
            for s in syn_rows:
                base_name = str(s["Name"] or "").strip()
                alias_name = str(s["Synonyms"] or "").strip()
                if not alias_name:
                    continue
                bases = base_map.get(base_name) or []
                for b in bases:
                    dup = dict(b)
                    dup[name_col] = alias_name
                    out.append(dup)
        return out
    finally:
        pconn.close()


def _load_invoices_meta() -> dict[str, Any]:
    meta_path = os.path.join(INVOICES_DIR, "invoices_meta.json")
    if not os.path.exists(meta_path):
        return {"items": []}
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"items": []}


def _save_invoices_meta(meta: dict[str, Any]) -> None:
    meta_path = os.path.join(INVOICES_DIR, "invoices_meta.json")
    tmp_path = meta_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, meta_path)


def _add_invoice_record(name: str, client_name: str, rel_pdf_path: str, size_bytes: int) -> dict[str, Any]:
    meta = _load_invoices_meta()
    record = {
        "id": str(uuid.uuid4()),
        "name": name,
        "client": client_name,
        "file": rel_pdf_path,  # relative to INVOICES_DIR
        "size": size_bytes,
        "created_at": datetime.utcnow().isoformat() + "Z",
    }
    meta.setdefault("items", []).insert(0, record)
    _save_invoices_meta(meta)
    return record


def _find_invoice_record(rec_id: str) -> dict[str, Any] | None:
    meta = _load_invoices_meta()
    for it in meta.get("items", []):
        if it.get("id") == rec_id:
            return it
    return None


def _update_invoice_name(rec_id: str, new_name: str) -> bool:
    meta = _load_invoices_meta()
    changed = False
    for it in meta.get("items", []):
        if it.get("id") == rec_id:
            it["name"] = new_name
            changed = True
            break
    if changed:
        _save_invoices_meta(meta)
    return changed


# ----------------------------
# Helpers
# ----------------------------
def strip_trailing_pdf(name: str) -> str:
    s = name or ""
    if len(s) >= 4 and s.lower().endswith(".pdf"):
        return s[:-4]
    return s

def convert_newlines_to_br(text: str | None) -> str | None:
    if text is None:
        return None
    # Normalize CRLF/CR to LF first, then convert to <br>
    s = str(text).replace("\r\n", "\n").replace("\r", "\n")
    return s.replace("\n", "<br>")

def _bexio_headers() -> dict[str, str]:
    api_key = os.getenv("BEXIO_API_KEY")
    if not api_key:
        # Keep silent failure minimal; caller can handle exception or 401
        raise RuntimeError("BEXIO_API_KEY is not configured")
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

def fetch_bexio_article_description(article_id: int) -> str | None:
    """
    Fetches the Bexio article and returns the raw HTML in 'intern_description'.
    No parsing or transformation applied.
    """
    try:
        url = f"https://api.bexio.com/2.0/article/{article_id}"
        resp = requests.get(url, headers=_bexio_headers(), timeout=(30, 60))
        if not (200 <= resp.status_code < 300):
            return None
        data = resp.json()
        if isinstance(data, dict):
            return data.get("intern_description")
        return None
    except Exception:
        return None

def _fetch_bexio_article_by_id(article_id: int) -> dict | None:
    """
    Returns the full article dict from Bexio by numeric ID.
    """
    try:
        url = f"https://api.bexio.com/2.0/article/{article_id}"
        resp = requests.get(url, headers=_bexio_headers(), timeout=(30, 60))
        if not (200 <= resp.status_code < 300):
            return None
        data = resp.json()
        return data if isinstance(data, dict) else None
    except Exception:
        return None

def _fetch_bexio_article_by_intern_code(intern_code: str) -> dict | None:
    """
    Resolve article by exact intern_code using the search endpoint.
    Returns the first exact match dict or None.
    """
    code = (intern_code or "").strip()
    if not code:
        return None
    try:
        url = "https://api.bexio.com/2.0/article/search"
        body = [
            {"field": "intern_code", "value": code, "criteria": "="}
        ]
        resp = requests.post(url, headers=_bexio_headers(), json=body, timeout=(30, 60))
        if not (200 <= resp.status_code < 300):
            return None
        data = resp.json()
        if isinstance(data, list) and data:
            for it in data:
                try:
                    if str((it or {}).get("intern_code", "")).strip() == code:
                        return it
                except Exception:
                    continue
            return data[0]
        return None
    except Exception:
        return None

def _fetch_bexio_unit_name(unit_id: int) -> str | None:
    """
    Resolve unit name (e.g., kg, Stk) from Bexio's unit endpoint by unit_id.
    """
    try:
        url = f"https://api.bexio.com/2.0/unit/{int(unit_id)}"
        resp = requests.get(url, headers=_bexio_headers(), timeout=(30, 30))
        if not (200 <= resp.status_code < 300):
            return None
        data = resp.json()
        if isinstance(data, dict):
            return data.get("name")
        return None
    except Exception:
        return None

def _enrich_payload_with_bexio(payload_obj: dict | list) -> None:
    """
    Mutates payload_obj in place:
      - For Bexio invoice positions with 'text' field containing "Product code: XXX",
        extracts the intern_code, fetches article from Bexio, and replaces 'text' with intern_description.
      - For legacy items with direct 'intern_code' OR 'article_id'/'product_id' keys,
        fetch article details from Bexio and set: 'intern_name', 'intern_description', 'unit_id', 'unit_name'
    Best-effort; skips silently on errors.
    """
    processed_codes: set[str] = set()
    processed_ids: set[int] = set()
    unit_cache: dict[int, str | None] = {}
    article_cache_by_code: dict[str, dict | None] = {}
    article_cache_by_id: dict[int, dict | None] = {}

    def extract_product_code_from_text(text: str) -> str | None:
        """Extract product code from text field like 'Product code: 80GY6AOPKc1012'"""
        if not isinstance(text, str):
            return None
        match = re.search(r'Product\s+code:\s*([A-Za-z0-9\-_.]+)', text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return None

    def parse_html_description_to_pairs(html: str) -> list[dict]:
        """
        Parse HTML description into list of key-value pairs with formatting info.
        Returns: [{'key': 'Weight', 'value': '7 kg', 'isStrong': False}, ...]
        """
        if not html or not isinstance(html, str):
            return []
        
        pairs = []
        # Split by <br> or <br /> tags
        lines = re.split(r'<br\s*/?>', html, flags=re.IGNORECASE)
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Check if line is wrapped in <strong>
            is_strong = False
            if re.match(r'<strong>', line, re.IGNORECASE):
                is_strong = True
                # Remove strong tags to get plain text
                line = re.sub(r'</?strong>', '', line, flags=re.IGNORECASE)
            
            # Remove any other HTML tags
            line = re.sub(r'<[^>]+>', '', line).strip()
            
            if not line:
                continue
            
            # Split by first colon to get key-value
            if ':' in line:
                key, value = line.split(':', 1)
                pairs.append({
                    'key': key.strip(),
                    'value': value.strip(),
                    'isStrong': is_strong
                })
            else:
                # Line without colon, treat as note
                pairs.append({
                    'key': 'Note',
                    'value': line,
                    'isStrong': is_strong
                })
        
        return pairs

    def rebuild_html_from_pairs(title: str, pairs: list[dict], product_code: str = None) -> str:
        """
        Rebuild HTML text field from title, pairs, and optional product code.
        Returns formatted HTML string with Product code prioritized right after title.
        """
        parts = []
        
        # Add title in strong tag
        if title:
            parts.append(f"<strong>{title}</strong>")
        
        # Add product code RIGHT AFTER title (prioritized)
        if product_code:
            parts.append(f"Product code: {product_code}")
        
        # Separate pairs into product_code pairs and others
        product_code_pairs = []
        other_pairs = []
        
        for pair in pairs:
            key = pair.get('key', '').strip()
            if key.lower() == 'product code':
                product_code_pairs.append(pair)
            else:
                other_pairs.append(pair)
        
        # Add product code pairs first (if not already added via product_code param)
        if not product_code:
            for pair in product_code_pairs:
                key = pair.get('key', '').strip()
                value = pair.get('value', '').strip()
                allow_blank = pair.get('allow_blank', False)
                
                if not key:
                    continue
                
                if (value is None or value == '') and not allow_blank:
                    continue
                
                if value is None:
                    value = ''
                
                line = f"{key}: {value}"
                
                if pair.get('isStrong'):
                    parts.append(f"<strong>{line}</strong>")
                else:
                    parts.append(line)
        
        # Add all other key-value pairs
        for pair in other_pairs:
            key = pair.get('key', '').strip()
            value = pair.get('value', '').strip()
            allow_blank = pair.get('allow_blank', False)
            
            if not key:
                continue
            
            # If value is None or empty, skip it UNLESS allow_blank is True
            # allow_blank is used for fields like Unit that should show even when blank
            if (value is None or value == '') and not allow_blank:
                continue
            
            # For blank values that are allowed, use empty string
            if value is None:
                value = ''
            
            line = f"{key}: {value}"
            
            # Wrap in strong if needed
            if pair.get('isStrong'):
                parts.append(f"<strong>{line}</strong>")
            else:
                parts.append(line)
        
        return "<br />".join(parts)

    def handle_bexio_position(d: dict) -> None:
        """Handle Bexio invoice position with text field containing Product code"""
        text_val = d.get("text")
        if not isinstance(text_val, str):
            return
        
        # FIRST: Extract and preserve ONLY specific fields from Workflow 1
        original_pairs = parse_html_description_to_pairs(text_val)
        preserved_fields = []
        # ONLY preserve these fields from delivery note
        fields_to_preserve = {'mhd', 'gross weight'}
        for pair in original_pairs:
            key_lower = (pair.get('key') or '').lower().strip()
            # Only keep MHD and Gross Weight from Workflow 1
            if key_lower in fields_to_preserve:
                preserved_fields.append(pair)
        
        # Extract product code from text
        intern_code = extract_product_code_from_text(text_val)
        if not intern_code:
            return
        
        # Fetch article from cache or API
        if intern_code in article_cache_by_code:
            article = article_cache_by_code[intern_code]
        else:
            article = _fetch_bexio_article_by_intern_code(intern_code)
            article_cache_by_code[intern_code] = article
            processed_codes.add(intern_code)
        
        # Replace text field with intern_description from Bexio and add metadata
        if isinstance(article, dict):
            intern_name = article.get("intern_name")
            intern_desc = article.get("intern_description")
            unit_id = article.get("unit_id")
            
            # Store intern_name as separate field for UI to use as title
            if intern_name is not None and isinstance(intern_name, str) and intern_name.strip():
                d["intern_name"] = intern_name
            
            # Fetch unit name from Bexio if unit_id exists
            unit_name = None
            if unit_id is not None:
                try:
                    uid = int(unit_id)
                    if uid not in unit_cache:
                        unit_cache[uid] = _fetch_bexio_unit_name(uid)
                    unit_name = unit_cache[uid]
                except Exception:
                    pass
            
            # Parse intern_description into key-value pairs
            pairs = parse_html_description_to_pairs(intern_desc or "")
            
            # Remove unwanted fields from Bexio response
            fields_to_remove = {'gross kg', 'kg', 'note'}
            pairs = [pair for pair in pairs if (pair.get('key') or '').lower().strip() not in fields_to_remove]
            
            # Update or add Unit field with fetched unit name (even if None/blank)
            # This ensures Unit field is always present and editable by user
            unit_found = False
            for pair in pairs:
                if pair['key'].lower() == 'unit':
                    pair['value'] = unit_name or ''  # Use empty string if None
                    pair['allow_blank'] = True  # Mark that this field can be blank
                    unit_found = True
                    break
            # If Unit field doesn't exist, add it
            if not unit_found:
                pairs.append({
                    'key': 'Unit',
                    'value': unit_name or '',  # Use empty string if None
                    'isStrong': False,
                    'allow_blank': True  # Mark that this field can be blank
                })
            
            # APPEND preserved fields from original text (MHD, Gross Weight, etc.)
            pairs.extend(preserved_fields)
            
            # Rebuild text field with title, updated pairs (including preserved fields), and product code
            d["text"] = rebuild_html_from_pairs(intern_name, pairs, intern_code)
            
            # Store intern_code as separate field for reference
            d["intern_code"] = intern_code

    def handle_legacy_item(d: dict) -> None:
        """Handle legacy items with direct intern_code or article_id keys"""
        article = None
        code_val = d.get("intern_code")
        if isinstance(code_val, str) and code_val.strip():
            code_key = code_val.strip()
            if code_key not in processed_codes:
                processed_codes.add(code_key)
                article = article_cache_by_code.get(code_key)
                if article is None:
                    article = _fetch_bexio_article_by_intern_code(code_key)
                    article_cache_by_code[code_key] = article
            else:
                article = article_cache_by_code.get(code_key)
        else:
            id_key = None
            for k in ("article_id", "product_id", "id"):
                if k in d:
                    id_key = k
                    break
            if id_key is not None:
                try:
                    aid = int(str(d.get(id_key)).strip())
                except Exception:
                    aid = None
                if isinstance(aid, int):
                    if aid not in processed_ids:
                        processed_ids.add(aid)
                        article = article_cache_by_id.get(aid)
                        if article is None:
                            article = _fetch_bexio_article_by_id(aid)
                            article_cache_by_id[aid] = article
                    else:
                        article = article_cache_by_id.get(aid)

        if isinstance(article, dict):
            if article.get("intern_name") is not None:
                d["intern_name"] = article.get("intern_name")
            if article.get("intern_description") is not None:
                d["intern_description"] = article.get("intern_description")
            unit_id = article.get("unit_id")
            if unit_id is None:
                unit_id = article.get("unit_code")
            if unit_id is None:
                unit_id = d.get("unit_id")
            if unit_id is None:
                unit_id = d.get("unit_code")
            if unit_id is not None:
                d["unit_id"] = unit_id
                try:
                    uid = int(unit_id)
                    if uid not in unit_cache:
                        unit_cache[uid] = _fetch_bexio_unit_name(uid)
                    if unit_cache[uid] is not None:
                        d["unit_name"] = unit_cache[uid]
                except Exception:
                    pass

    def walk(obj, depth=0):
        if depth > 6:
            return
        if isinstance(obj, dict):
            # Check if this is a Bexio position (has 'text' field with Product code)
            if "text" in obj and isinstance(obj.get("text"), str):
                text_content = obj.get("text", "")
                if "Product code:" in text_content or "product code:" in text_content.lower():
                    # This is a Bexio position with embedded product code
                    handle_bexio_position(obj)
                    # Don't recurse into this object's values after handling
                    return
            
            # Check if this is a legacy item with direct keys
            looks_like_legacy_item = any(k in obj for k in ("intern_code", "article_id", "product_id"))
            if looks_like_legacy_item:
                handle_legacy_item(obj)
            
            # Recurse into nested structures
            for v in obj.values():
                walk(v, depth + 1)
        elif isinstance(obj, list):
            for el in obj:
                walk(el, depth + 1)

    try:
        walk(payload_obj, 0)
    except Exception:
        return


@app.post("/api/generate_invoice")
@login_required
def api_generate_invoice():
    client_name = (request.form.get("client_name") or "").strip()
    invoice_name = (request.form.get("invoice_name") or "").strip()
    title_invoice = request.form.get("titleInvoice")  # pass-through as-is
    header_invoice = request.form.get("headerInvoice")  # pass-through as-is
    footer_invoice = request.form.get("footerInvoice")  # pass-through as-is
    currency_exchange_raw = request.form.get("currency_exchange")
    if not client_name:
        flash("Bitte einen Kunden auswählen.", "error")
        return jsonify({"error": "client_name required"}), 400

    delivery_notes = request.files.getlist("delivery_notes")
    valid_pdfs = []
    for f in delivery_notes:
        if not f or not (f.filename or "").lower().endswith(".pdf"):
            continue
        valid_pdfs.append(f)

    # Two-phase: if enabled, we will call payload workflow first and redirect to review page.
    two_phase_enabled = bool(USE_TWO_PHASE_FLOW and GENERATE_PAYLOAD_JSON_WEBHOOK_URL)
    if not two_phase_enabled:
        if not WEBHOOK_URL:
            return jsonify({"error": tr("webhook_not_set")}), 400

    # Build exact-key array from Preise table filtered by Kunde_Name (augmented with synonyms)
    try:
        pconn = get_pricing_db()
        if not pricing_table_exists(pconn):
            pconn.close()
            return jsonify({"error": "no pricing data"}), 400
        rows = build_pricing_json_for_client(client_name)
        pconn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    # Build multipart form-data to emit N items under the same field name `data`
    # and N matching binary parts under `binary[<index>]`, plus a single `schema` field.
    data_fields: list[tuple[str, str]] = []
    file_parts: list[tuple[str, tuple[str, Any, str]]] = []

    # Attach the pricing rows once as a standalone schema field
    data_fields.append(("schema", json.dumps(rows, ensure_ascii=False)))
    # Include Invoice_name for downstream (exactly as user typed, minus trailing .pdf)
    try:
        invoice_name_raw = invoice_name
        invoice_name_no_pdf = strip_trailing_pdf(invoice_name_raw)
        if invoice_name_no_pdf:
            data_fields.append(("Invoice_name", invoice_name_no_pdf))
    except Exception:
        pass
    # Include optional Title/Header/Footer fields
    if title_invoice is not None and str(title_invoice).strip() != "":
        data_fields.append(("titleInvoice", str(title_invoice)))
    if header_invoice is not None and str(header_invoice).strip() != "":
        data_fields.append(("headerInvoice", convert_newlines_to_br(str(header_invoice))))
    if footer_invoice is not None and str(footer_invoice).strip() != "":
        data_fields.append(("footerInvoice", convert_newlines_to_br(str(footer_invoice))))
    # Attach currency exchange block if provided by frontend
    if currency_exchange_raw:
        try:
            # Validate JSON minimally and enforce base semantics
            cx = json.loads(currency_exchange_raw)
            if isinstance(cx, dict):
                # Ensure base set to CHF and CHF rate=1 when code is CHF
                cx.setdefault("base", "CHF")
                if cx.get("code") == "CHF":
                    cx["rate"] = 1.0
                data_fields.append(("currency_exchange", json.dumps(cx, ensure_ascii=False)))
        except Exception:
            # If invalid, omit silently; webhook can proceed without FX
            pass

    if valid_pdfs:
        for idx, pdf in enumerate(valid_pdfs):
            safe_name = secure_filename(pdf.filename or "delivery_note.pdf")
            # Per-item JSON under repeated field name pattern data[<index>]
            item_payload = {
                "kunde": client_name,
                "filename": safe_name,
                "index": idx,
            }
            data_fields.append((f"data[{idx}]", json.dumps(item_payload, ensure_ascii=False)))
            # Matching binary part under binary[<index>]
            file_parts.append((f"binary[{idx}]", (safe_name, pdf.stream, "application/pdf")))
        # Optional: include count to aid parsing on receiver side
        data_fields.append(("count", str(len(valid_pdfs))))
    else:
        # No PDFs: send a single logical item with schema only
        item_payload = {
            "kunde": client_name,
            "filename": None,
            "index": 0,
        }
        data_fields.append(("data[0]", json.dumps(item_payload, ensure_ascii=False)))
        data_fields.append(("count", "1"))

    try:
        # Separate connect/read timeouts to allow longer processing on n8n
        timeout_arg = None if INFINITE_WEBHOOK_TIMEOUT else (WEBHOOK_CONNECT_TIMEOUT_SEC, WEBHOOK_READ_TIMEOUT_SEC)
        if two_phase_enabled:
            # Phase 1: request JSON payload from dedicated workflow
            resp = requests.post(GENERATE_PAYLOAD_JSON_WEBHOOK_URL, data=data_fields, files=file_parts, timeout=timeout_arg)
            ok = 200 <= resp.status_code < 300
            if not ok:
                snippet = (resp.text or "")[:300]
                ctype = resp.headers.get("Content-Type", "")
                msg = tr("flash_webhook_fail", status=resp.status_code)
                return jsonify({"error": f"{msg}. Upstream Content-Type={ctype}. Body snippet: {snippet}"}), 502
            # Ensure JSON body
            try:
                payload_obj = resp.json()
            except Exception:
                ctype = resp.headers.get("Content-Type", "")
                head = (resp.content or b"")[:4]
                head_hex = head.hex()
                return jsonify({"error": f"Invalid JSON payload returned. Content-Type={ctype}. First bytes={head_hex}"}), 502
            # Enrich payload in-memory with Bexio article details before persisting the draft
            try:
                _enrich_payload_with_bexio(payload_obj)
            except Exception:
                # Best-effort enrichment; do not block draft creation
                pass
            # Persist draft into SQL DB
            draft_id = str(uuid.uuid4())
            now_iso = datetime.utcnow().isoformat() + "Z"
            safe_invoice_name = strip_trailing_pdf(invoice_name) if invoice_name else None
            try:
                conn = get_invoices_db()
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT OR REPLACE INTO draft_invoices (draft_id, client_name, invoice_name, payload_json, title_invoice, header_invoice, footer_invoice, currency_exchange, status, created_at, updated_at, finalized_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'draft', ?, ?, NULL)
                    """,
                    (
                        draft_id,
                        client_name,
                        safe_invoice_name,
                        json.dumps(payload_obj, ensure_ascii=False),
                        title_invoice,
                        header_invoice,
                        footer_invoice,
                        (currency_exchange_raw or None),
                        now_iso,
                        now_iso,
                    ),
                )
                conn.commit()
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
            # Redirect URL for review page
            return jsonify({
                "draft_id": draft_id,
                "redirect_url": url_for("review_invoice", draft_id=draft_id),
            })
        # Single-phase legacy flow
        resp = requests.post(WEBHOOK_URL, data=data_fields, files=file_parts, timeout=timeout_arg)
        ok = 200 <= resp.status_code < 300
        # Validate non-empty PDF
        content = resp.content or b""
        looks_pdf = (len(content) > 0 and content[:4] == b"%PDF")
        if not ok or not looks_pdf:
            msg = tr("flash_webhook_fail", status=resp.status_code) if not ok else "Invalid or empty PDF returned"
            return jsonify({"error": msg}), 502

        # Determine filename from response headers or fallback
        disp = resp.headers.get("Content-Disposition", "")
        fallback_name = "invoice.pdf"
        if "filename=" in disp:
            try:
                fallback_name = disp.split("filename=")[1].strip('"') or fallback_name
            except Exception:
                pass
        final_name = invoice_name or fallback_name
        safe_final = secure_filename(final_name)
        if not safe_final.lower().endswith(".pdf"):
            safe_final += ".pdf"

        # Save archive copy
        archive_filename = f"{uuid.uuid4()}.pdf"
        archive_rel = archive_filename
        archive_path = os.path.join(INVOICES_DIR, archive_filename)
        with open(archive_path, "wb") as f:
            f.write(resp.content)
        size_bytes = os.path.getsize(archive_path)

        record = _add_invoice_record(safe_final, client_name, archive_rel, size_bytes)
        # Also persist metadata to invoices DB (for the new DB-driven view)
        try:
            add_invoice_db_record(record["id"], record["name"], record["client"], record["file"], record["size"], record["created_at"])
        except Exception:
            pass

        # Create one-time download temp copy
        tmp_path = os.path.join(DOWNLOAD_TMP_DIR, f"{record['id']}.pdf")
        with open(tmp_path, "wb") as f:
            f.write(resp.content)

        return jsonify({
            "id": record["id"],
            "name": record["name"],
            "preview_url": url_for("preview_invoice", invoice_id=record["id"]),
            "download_url": url_for("download_invoice_once", invoice_id=record["id"]),
            "created_at": record["created_at"],
            "size": record["size"],
        })
    except Exception as e:
        return jsonify({"error": tr("flash_webhook_send_error", error=str(e))}), 500


def _normalize_text(s: str) -> str:
    # Lowercase, trim, remove diacritics, collapse whitespace and punctuation
    import unicodedata
    s = (s or "").strip().lower()
    # Normalize and strip accents
    s = unicodedata.normalize('NFKD', s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace("ß", "ss")
    # Normalize simple fraction patterns like 1_1 or 1-1 to 1/1 before punctuation handling
    import re
    s = re.sub(r"(\d)[_\-](\d)", r"\1/\2", s)
    # Replace separators/punct with spaces (preserve '/' to keep fractions like 1/4 intact)
    for ch in [",", ";", ":", ".", "(", ")", "[", "]", "{", "}", "\\", "-", "_", "+", "*", "|", "~", "!", "?", "'", '"']:
        s = s.replace(ch, " ")
    # Collapse whitespace
    s = " ".join(s.split())
    # German transliterations already handled via diacritic strip + ß→ss; also map umlaut spellings
    s = s.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue")
    return s


def _fuzzy_ratio(a: str, b: str) -> float:
    # Lightweight ratio 0..100 using difflib
    import difflib
    return round(difflib.SequenceMatcher(None, _normalize_text(a), _normalize_text(b)).ratio() * 100, 2)


def _tokenize(s: str) -> set[str]:
    txt = _normalize_text(s)
    # Lightweight token synonym map (domain-aware)
    token_map = {
        "laib": "wheel",
        "rad": "wheel",
        "wheel": "wheel",
        "meule": "wheel",
        "kart": "karton",
        "karton": "karton",
        "kartonage": "karton",
        "keil": "wedge",
        "wedge": "wedge",
        "bloc": "block",
        "blocs": "block",
        "block": "block",
        "eckig": "square",
        "square": "square",
        "rund": "wheel",
        "mild-wurzig": "mildwurzig",
        "mild-würzig": "mildwurzig",
        "mildwurzig": "mildwurzig",
        "doux": "mild",
        "reserve": "reserve",
        "alpage": "alpage",
        "mois": "months",
        "monat": "months",
        "monate": "months",
        "mte": "months",
        "mt": "months",
        "portion": "portion",
        "portions": "portion",
        "rouleaux": "rolls",
        "rouleau": "rolls",
        "rolls": "rolls",
    }
    tokens = []
    for t in txt.split():
        t2 = token_map.get(t, t)
        tokens.append(t2)
    tokens = set(tokens)
    # Drop very short tokens and common packaging/unit stopwords
    stop = {
        "kg", "g", "gr", "gram", "stk", "st", "pc", "pcs", "pk", "pack", "ml", "l", "x", "a", "à", "per",
        "karton", "box", "tray", "case",
        "bio", "aop", "igp",
    }
    return {t for t in tokens if len(t) > 1 and t not in stop}


def _token_set_score(a: str, b: str) -> float:
    ta, tb = _tokenize(a), _tokenize(b)
    if not ta or not tb:
        return 0.0
    inter = len(ta & tb)
    score = (2 * inter) / (len(ta) + len(tb))
    return round(score * 100, 2)


def _trigram_jaccard(a: str, b: str) -> float:
    def grams(s: str) -> set[str]:
        t = _normalize_text(s).replace(" ", "")
        if len(t) < 3:
            return {t} if t else set()
        return {t[i:i+3] for i in range(len(t)-2)}
    ga, gb = grams(a), grams(b)
    if not ga or not gb:
        return 0.0
    inter = len(ga & gb)
    uni = len(ga | gb)
    return round((inter / uni) * 100, 2)


def _best_match_base_row(base_name: str, base_rows: list[dict], name_col: str) -> tuple[dict | None, float]:
    # Anchor-based blocking: require at least one shared token if possible
    base_tokens = _tokenize(base_name)
    best = None
    best_score = -1.0
    for r in base_rows:
        cand = str(r.get(name_col) or "")
        if not cand:
            continue
        cand_tokens = _tokenize(cand)
        shares_anchor = bool(base_tokens & cand_tokens)
        s1 = _fuzzy_ratio(base_name, cand)
        s2 = _token_set_score(base_name, cand)
        s3 = _trigram_jaccard(base_name, cand)
        try:
            # Optional Jaro-Winkler via jellyfish if available
            import jellyfish
            s4 = round(jellyfish.jaro_winkler_similarity(_normalize_text(base_name), _normalize_text(cand)) * 100, 2)
        except Exception:
            s4 = 0.0
        # Choose the best across metrics
        score = max(s1, s2, s3, s4)
        # Slightly penalize if no shared anchor tokens
        if not shares_anchor:
            score = score * 0.9
        if score > best_score:
            best_score = score
            best = r
    return best, best_score


def _best_match_base_row_relaxed(base_name: str, base_rows: list[dict], name_col: str) -> tuple[dict | None, float]:
    # Relaxed: no anchor token penalty; emphasize JW and trigram
    best = None
    best_score = -1.0
    for r in base_rows:
        cand = str(r.get(name_col) or "")
        if not cand:
            continue
        s1 = _fuzzy_ratio(base_name, cand)
        s2 = _token_set_score(base_name, cand)
        s3 = _trigram_jaccard(base_name, cand)
        try:
            import jellyfish
            s4 = round(jellyfish.jaro_winkler_similarity(_normalize_text(base_name), _normalize_text(cand)) * 100, 2)
        except Exception:
            s4 = 0.0
        # Substring containment boost for cross-language/format variants
        bn = _normalize_text(base_name)
        cn = _normalize_text(cand)
        contain = (bn in cn) or (cn in bn)
        score = max(s1, s2, s3, s4)
        if contain and s2 >= 60.0:
            score = max(score, 90.0)
        if score > best_score:
            best_score = score
            best = r
    return best, best_score


@app.post("/synonyms/upload")
@login_required
def synonyms_upload():
    if "file" not in request.files:
        return jsonify({"error": tr("flash_missing_file")}), 400
    excel_file = request.files["file"]
    filename = secure_filename(excel_file.filename or "")
    ext = os.path.splitext(filename)[1].lower()
    if ext not in ALLOWED_EXCEL_EXTENSIONS:
        return jsonify({"error": tr("flash_excel_only")}), 400

    temp_path = os.path.join(UPLOAD_DIR, filename)
    excel_file.save(temp_path)

    threshold = get_match_threshold()
    deleted_total = 0
    inserted_total = 0
    unmatched_total = 0
    customers_in_file: set[str] = set()

    try:
        df = pd.read_excel(temp_path, dtype=object)
        # Expect columns: Customer, Name, Synonyms (case-insensitive, trimmed)
        colmap = {}
        for c in df.columns:
            key = str(c).strip()
            low = key.lower()
            if low == "customer":
                colmap["Customer"] = c
            elif low == "name":
                colmap["Name"] = c
            elif low == "synonyms" or low == "synonym":
                colmap["Synonyms"] = c
        missing = [k for k in ("Customer", "Name", "Synonyms") if k not in colmap]
        if missing:
            return jsonify({"error": f"Missing required columns: {', '.join(missing)}"}), 400

        # Collect rows
        syn_input: list[tuple[str, str, str]] = []  # (Customer, Name, Synonyms)
        for _, r in df.iterrows():
            cust = str(r[colmap["Customer"]]).strip() if pd.notna(r[colmap["Customer"]]) else ""
            base = str(r[colmap["Name"]]).strip() if pd.notna(r[colmap["Name"]]) else ""
            alias = str(r[colmap["Synonyms"]]).strip() if pd.notna(r[colmap["Synonyms"]]) else ""
            if not cust or not base or not alias:
                continue
            syn_input.append((cust, base, alias))
            customers_in_file.add(cust)

        pconn = get_pricing_db()
        try:
            ensure_synonyms_table(pconn)
            # Delete S rows from Preise for customers in file
            deleted_total = delete_s_rows_for_customers(pconn, sorted(customers_in_file))

            # Also clear and re-store definitions for those customers
            _ = clear_synonyms_for_customers(pconn, sorted(customers_in_file))

            now_iso = datetime.utcnow().isoformat() + "Z"
            batch_defs: list[tuple[str, str, str, float, str]] = []

            # We will simultaneously rebuild S rows into Preise using the same matching as rebuild_synonyms_into_preise
            cols = get_preise_columns(pconn)
            kunde_col = get_kunde_col_from_cols(cols)
            name_col = get_productname_col_from_cols(cols)
            if not kunde_col or not name_col:
                return jsonify({"error": "Preise table missing Kunde_Name or Produktname."}), 400

            # Cache P rows per customer
            from collections import defaultdict
            cache_base_rows: dict[str, list[dict]] = {}
            for cust in customers_in_file:
                cache_base_rows[cust] = [r for r in fetch_rows_for_kunde(pconn, cust) if True]

            for cust, base, alias in syn_input:
                base_rows = cache_base_rows.get(cust, [])
                if not base_rows:
                    unmatched_total += 1
                    continue
                best_row, best_score = _best_match_base_row(base, base_rows, name_col)
                if best_row is None or best_score < threshold:
                    # Second pass relaxed
                    rthr = get_relaxed_threshold()
                    best_row, best_score = _best_match_base_row_relaxed(base, base_rows, name_col)
                    if best_row is None or best_score < rthr:
                        unmatched_total += 1
                        continue
                # Save definition
                batch_defs.append((cust, str(best_row.get(name_col) or ""), alias, float(best_score), now_iso))
                # Insert duplicate S row into Preise
                dup = dict(best_row)
                dup[name_col] = alias
                dup["record_source"] = "S"
                insert_row_dict(pconn, dup)

            inserted_total = insert_synonym_rows(pconn, batch_defs)
        finally:
            pconn.close()

        flash(f"Synonyms imported for {len(customers_in_file)} customers. Added: {int(inserted_total)}, unmatched: {int(unmatched_total)}.", "success")
        return redirect(url_for("feeddata_get"))
    except Exception as e:
        flash(str(e), "error")
        return redirect(url_for("feeddata_get"))
    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


@app.get("/api/invoices/check-name")
@login_required
def api_check_invoice_name():
    raw = request.args.get("name") or ""
    # Keep case and internal spaces; only strip trailing whitespace and trailing .pdf
    display = strip_trailing_pdf(raw.strip())
    # Check availability against DB using the sanitized final filename policy (secure_filename + .pdf)
    candidate_base = secure_filename(display) if display else ""
    candidate_pdf = (candidate_base + ".pdf") if candidate_base and not candidate_base.lower().endswith('.pdf') else candidate_base

    available = True
    try:
        conn = get_invoices_db()
        cur = conn.cursor()
        if candidate_pdf:
            # Case-insensitive uniqueness: abbey and ABBEY considered the same
            cur.execute("SELECT 1 FROM invoices WHERE lower(name) = lower(?) LIMIT 1", (candidate_pdf,))
            row = cur.fetchone()
            available = (row is None)
        else:
            # Empty names are considered available but not suggested
            available = True
    except Exception:
        available = True
    finally:
        try:
            conn.close()
        except Exception:
            pass

    # Suggestions: date-first, then numeric suffixes; ensure availability after sanitization
    suggestions: list[str] = []
    base_for_suggestion = display
    if base_for_suggestion:
        today = datetime.utcnow().strftime("%Y%m%d")
        candidates = [f"{base_for_suggestion}_{today}"]
        # numeric fallback 2..5
        for i in range(2, 6):
            candidates.append(f"{base_for_suggestion}_{i}")

        try:
            conn = get_invoices_db()
            cur = conn.cursor()
            for cand in candidates:
                if len(suggestions) >= 3:
                    break
                cand_base = secure_filename(strip_trailing_pdf(cand))
                cand_pdf = cand_base if cand_base.lower().endswith('.pdf') else (cand_base + '.pdf')
                cur.execute("SELECT 1 FROM invoices WHERE lower(name) = lower(?) LIMIT 1", (cand_pdf,))
                if cur.fetchone() is None:
                    suggestions.append(cand)
        except Exception:
            # On error, still return computed suggestions without DB guarantee
            suggestions = candidates[:3]
        finally:
            try:
                conn.close()
            except Exception:
                pass

    return jsonify({
        "name": display,
        "available": available,
        "suggestions": suggestions,
    })
@app.get("/preview/<invoice_id>")
@login_required
def preview_invoice(invoice_id: str):
    # Prefer DB record first
    try:
        conn = get_invoices_db()
        cur = conn.cursor()
        cur.execute("SELECT id, name, file FROM invoices WHERE id = ?", (invoice_id,))
        r = cur.fetchone()
        if r:
            pdf_path = os.path.join(INVOICES_DIR, r["file"])
            if not os.path.exists(pdf_path):
                return "Not found", 404
            return send_file(pdf_path, mimetype="application/pdf", as_attachment=False, download_name=r["name"])
    except Exception:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass
    # Fallback to legacy JSON meta
    rec = _find_invoice_record(invoice_id)
    if not rec:
        return "Not found", 404
    pdf_path = os.path.join(INVOICES_DIR, rec["file"])
    if not os.path.exists(pdf_path):
        return "Not found", 404
    return send_file(pdf_path, mimetype="application/pdf", as_attachment=False, download_name=rec["name"])


@app.get("/download-once/<invoice_id>")
@login_required
def download_invoice_once(invoice_id: str):
    # Serve from temp and delete after response is processed
    tmp_path = os.path.join(DOWNLOAD_TMP_DIR, f"{invoice_id}.pdf")
    rec = _find_invoice_record(invoice_id)
    if not rec or not os.path.exists(tmp_path):
        return "Not found", 404

    @after_this_request
    def _cleanup(response):
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        return response

    return send_file(tmp_path, mimetype="application/pdf", as_attachment=True, download_name=rec["name"])


@app.get("/download/<invoice_id>")
@login_required
def download_invoice(invoice_id: str):
    # Stable download from archive using current meta name
    # Prefer DB first
    try:
        conn = get_invoices_db()
        cur = conn.cursor()
        cur.execute("SELECT id, name, file FROM invoices WHERE id = ?", (invoice_id,))
        r = cur.fetchone()
        if r:
            pdf_path = os.path.join(INVOICES_DIR, r["file"])
            if not os.path.exists(pdf_path):
                return "Not found", 404
            return send_file(pdf_path, mimetype="application/pdf", as_attachment=True, download_name=r["name"])
    except Exception:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass
    # Fallback to legacy JSON meta
    rec = _find_invoice_record(invoice_id)
    if not rec:
        return "Not found", 404
    pdf_path = os.path.join(INVOICES_DIR, rec["file"])
    if not os.path.exists(pdf_path):
        return "Not found", 404
    return send_file(pdf_path, mimetype="application/pdf", as_attachment=True, download_name=rec["name"])


@app.get("/invoices-legacy")
@login_required
def invoices_dashboard():
    # Filters: date range and sort
    sort = (request.args.get("sort") or "newest").lower()
    date_from = (request.args.get("from") or "").strip()
    date_to = (request.args.get("to") or "").strip()
    page = max(int(request.args.get("page", 1)), 1)
    page_size = 7

    meta = _load_invoices_meta()
    items = meta.get("items", [])

    def _in_range(it: dict[str, Any]) -> bool:
        ts = (it.get("created_at") or "")[:10]
        if date_from and ts < date_from[:10]:
            return False
        if date_to and ts > date_to[:10]:
            return False
        return True

    items = [it for it in items if _in_range(it)]
    reverse = (sort != "oldest")
    items.sort(key=lambda x: x.get("created_at", ""), reverse=reverse)

    total = len(items)
    start = (page - 1) * page_size
    end = start + page_size
    page_items = items[start:end]

    # Build prev/next URLs safely (Jinja does not support **kwargs unpack)
    total_pages = (total // page_size) + (1 if total % page_size else 0)
    def _build_url(target_page: int) -> str:
        return url_for(
            "invoices_dashboard",
            page=target_page,
            sort=sort,
            **({"from": date_from} if date_from else {}),
            **({"to": date_to} if date_to else {}),
        )
    prev_url = _build_url(page - 1) if page > 1 else None
    next_url = _build_url(page + 1) if page < total_pages else None

    return render_template(
        "invoices.html",
        items=page_items,
        page=page,
        page_size=page_size,
        total=total,
        sort=sort,
        date_from=date_from,
        date_to=date_to,
        prev_url=prev_url,
        next_url=next_url,
        total_pages=total_pages,
    )


@app.get("/api/invoices-legacy")
@login_required
def api_invoices_list():
    sort = (request.args.get("sort") or "newest").lower()
    date_from = (request.args.get("from") or "").strip()
    date_to = (request.args.get("to") or "").strip()
    page = max(int(request.args.get("page", 1)), 1)
    page_size = max(int(request.args.get("page_size", 7)), 1)

    meta = _load_invoices_meta()
    items = meta.get("items", [])

    def _in_range(it: dict[str, Any]) -> bool:
        ts = (it.get("created_at") or "")[:10]
        if date_from and ts < date_from[:10]:
            return False
        if date_to and ts > date_to[:10]:
            return False
        return True

    items = [it for it in items if _in_range(it)]
    reverse = (sort != "oldest")
    items.sort(key=lambda x: x.get("created_at", ""), reverse=reverse)

    total = len(items)
    start = (page - 1) * page_size
    end = start + page_size
    page_items = items[start:end]
    # Include URLs for convenience
    for it in page_items:
        it["preview_url"] = url_for("preview_invoice", invoice_id=it["id"]) 
        it["download_url"] = url_for("download_invoice_once", invoice_id=it["id"]) 
    return jsonify({"total": total, "page": page, "items": page_items})


@app.post("/api/invoices-legacy/rename")
@login_required
def api_invoices_rename():
    data = request.get_json(silent=True) or {}
    rec_id = (data.get("id") or "").strip()
    new_name = (data.get("name") or "").strip()
    if not rec_id or not new_name:
        return jsonify({"error": "id and name required"}), 400
    ok = _update_invoice_name(rec_id, secure_filename(new_name if new_name.lower().endswith('.pdf') else new_name + '.pdf'))
    if not ok:
        return jsonify({"error": "not found"}), 404
    return jsonify({"ok": True})


# ----------------------------
# JSON APIs to support search/autocomplete and direct retrieval
# ----------------------------
@app.get("/api/customers")
def api_customers():
    q = (request.args.get("q") or "").strip()
    try:
        pconn = get_pricing_db()
        if not pricing_table_exists(pconn):
            pconn.close()
            return jsonify([])
        customers = list_distinct_kunde_names(pconn, q if q else None)
        pconn.close()
        return jsonify(customers)
    except Exception:
        return jsonify([]), 500


# ----------------------------
# Client Headers APIs
# ----------------------------
@app.get("/api/client-headers")
@login_required
def api_list_client_headers():
    """List all client headers."""
    headers = list_all_client_headers()
    return jsonify(headers)


@app.get("/api/client-headers/<client_name>")
@login_required
def api_get_client_header(client_name: str):
    """Get the default header for a specific client."""
    header = get_client_header(client_name)
    if header is None:
        return jsonify({"client_name": client_name, "default_header": None}), 404
    return jsonify({"client_name": client_name, "default_header": header})


@app.post("/api/client-headers")
@login_required
def api_save_client_header():
    """Save or update a client's default header."""
    data = request.get_json(silent=True) or {}
    client_name = (data.get("client_name") or "").strip()
    default_header = (data.get("default_header") or "").strip()
    
    if not client_name:
        return jsonify({"error": "client_name is required"}), 400
    if not default_header:
        return jsonify({"error": "default_header is required"}), 400
    
    success = save_client_header(client_name, default_header)
    if success:
        flash(tr("client_headers_success", client=client_name), "success")
        return jsonify({"ok": True, "client_name": client_name})
    else:
        return jsonify({"error": tr("client_headers_error", error="Database error")}), 500


@app.get("/api/client-footers/<client_name>")
@login_required
def api_get_client_footer(client_name: str):
    """Get the default footer for a specific client."""
    footer = get_client_footer(client_name)
    if footer is None:
        return jsonify({"client_name": client_name, "default_footer": None}), 404
    return jsonify({"client_name": client_name, "default_footer": footer})


@app.post("/api/client-footers")
@login_required
def api_save_client_footer():
    """Save or update a client's default footer."""
    data = request.get_json(silent=True) or {}
    client_name = (data.get("client_name") or "").strip()
    default_footer = (data.get("default_footer") or "").strip()
    
    if not client_name:
        return jsonify({"error": "client_name is required"}), 400
    if not default_footer:
        return jsonify({"error": "default_footer is required"}), 400
    
    success = save_client_footer(client_name, default_footer)
    if success:
        flash(tr("client_footers_success", client=client_name), "success")
        return jsonify({"ok": True, "client_name": client_name})
    else:
        return jsonify({"error": tr("client_footers_error", error="Database error")}), 500


# ----------------------------
# Invoices (DB-backed) pages and APIs
# ----------------------------
@app.get("/invoices")
@login_required
def invoices_db_dashboard():
    sort = (request.args.get("sort") or "newest").lower()
    date_from = (request.args.get("from") or "").strip()
    date_to = (request.args.get("to") or "").strip()
    page = max(int(request.args.get("page", 1)), 1)
    page_size = 7

    conn = get_invoices_db()
    try:
        where = []
        params: list[Any] = []
        if date_from:
            where.append("substr(created_at,1,10) >= ?")
            params.append(date_from[:10])
        if date_to:
            where.append("substr(created_at,1,10) <= ?")
            params.append(date_to[:10])
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM invoices {where_sql}", params)
        row = cur.fetchone()
        total = int(row[0]) if row is not None else 0

        order = "DESC" if sort != "oldest" else "ASC"
        offset = (page - 1) * page_size
        cur.execute(
            f"SELECT id, name, client, created_at, size, file FROM invoices {where_sql} ORDER BY created_at {order} LIMIT ? OFFSET ?",
            params + [page_size, offset],
        )
        rows = cur.fetchall()
        items = [
            {
                "id": r["id"],
                "name": r["name"],
                "client": r["client"],
                "created_at": r["created_at"],
                "size": r["size"],
                "file": r["file"],
            }
            for r in rows
        ]
    finally:
        conn.close()

    total_pages = (total // page_size) + (1 if total % page_size else 0)
    def _build_url(target_page: int) -> str:
        return url_for(
            "invoices_db_dashboard",
            page=target_page,
            sort=sort,
            **({"from": date_from} if date_from else {}),
            **({"to": date_to} if date_to else {}),
        )
    prev_url = _build_url(page - 1) if page > 1 else None
    next_url = _build_url(page + 1) if page < total_pages else None

    return render_template(
        "invoices_db.html",
        items=items,
        page=page,
        page_size=page_size,
        total=total,
        sort=sort,
        date_from=date_from,
        date_to=date_to,
        prev_url=prev_url,
        next_url=next_url,
        total_pages=total_pages,
    )


@app.get("/api/invoices")
@login_required
def api_invoices_db_list():
    sort = (request.args.get("sort") or "newest").lower()
    date_from = (request.args.get("from") or "").strip()
    date_to = (request.args.get("to") or "").strip()
    page = max(int(request.args.get("page", 1)), 1)
    page_size = max(int(request.args.get("page_size", 7)), 1)

    conn = get_invoices_db()
    try:
        where = []
        params: list[Any] = []
        if date_from:
            where.append("substr(created_at,1,10) >= ?")
            params.append(date_from[:10])
        if date_to:
            where.append("substr(created_at,1,10) <= ?")
            params.append(date_to[:10])
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM invoices {where_sql}", params)
        row = cur.fetchone()
        total = int(row[0]) if row is not None else 0

        order = "DESC" if sort != "oldest" else "ASC"
        offset = (page - 1) * page_size
        cur.execute(
            f"SELECT id, name, client, created_at, size FROM invoices {where_sql} ORDER BY created_at {order} LIMIT ? OFFSET ?",
            params + [page_size, offset],
        )
        items = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

    for it in items:
        it["preview_url"] = url_for("preview_invoice", invoice_id=it["id"]) 
        it["download_url"] = url_for("download_invoice", invoice_id=it["id"]) 
    return jsonify({"total": total, "page": page, "items": items})


@app.post("/api/invoices/rename")
@login_required
def api_invoices_db_rename():
    data = request.get_json(silent=True) or {}
    rec_id = (data.get("id") or "").strip()
    new_name = (data.get("name") or "").strip()
    if not rec_id or not new_name:
        return jsonify({"error": "id and name required"}), 400
    safe = secure_filename(new_name if new_name.lower().endswith('.pdf') else new_name + '.pdf')
    conn = get_invoices_db()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE invoices SET name = ? WHERE id = ?", (safe, rec_id))
        if cur.rowcount == 0:
            return jsonify({"error": "not found"}), 404
        conn.commit()
        return jsonify({"ok": True})
    finally:
        conn.close()


@app.post("/api/invoices/delete")
@login_required
def api_invoices_db_delete():
    data = request.get_json(silent=True) or {}
    rec_id = (data.get("id") or "").strip()
    if not rec_id:
        return jsonify({"error": "id required"}), 400
    conn = get_invoices_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT file FROM invoices WHERE id = ?", (rec_id,))
        r = cur.fetchone()
        if not r:
            return jsonify({"error": "not found"}), 404
        file_rel = r["file"]
        # Remove file if exists
        try:
            fpath = os.path.join(INVOICES_DIR, file_rel)
            if os.path.exists(fpath):
                os.remove(fpath)
        except Exception:
            pass
        # Delete DB row
        cur.execute("DELETE FROM invoices WHERE id = ?", (rec_id,))
        conn.commit()
        # Also remove from legacy JSON meta if present
        meta = _load_invoices_meta()
        meta["items"] = [it for it in meta.get("items", []) if it.get("id") != rec_id]
        _save_invoices_meta(meta)
        return jsonify({"ok": True})
    finally:
        conn.close()


@app.get("/api/prices")
def api_prices():
    kunde = (request.args.get("kunde") or "").strip()
    if not kunde:
        return jsonify([])
    try:
        pconn = get_pricing_db()
        if not pricing_table_exists(pconn):
            pconn.close()
            return jsonify([])
        rows = fetch_rows_for_kunde(pconn, kunde)
        pconn.close()
        return jsonify(rows)
    except Exception:
        return jsonify([]), 500


# ----------------------------
# Bexio Article APIs (non-invasive)
# ----------------------------
@app.get("/api/bexio/article/<int:article_id>/description")
@login_required
def api_bexio_article_description(article_id: int):
    """
    Returns the raw HTML product description from Bexio's article as JSON.
    Response body:
      { "article_id": <id>, "intern_description_html": "<div>...</div>" }
    """
    html = fetch_bexio_article_description(article_id)
    if html is None:
        return jsonify({"article_id": article_id, "intern_description_html": None}), 404
    return jsonify({"article_id": article_id, "intern_description_html": html})


@app.post("/api/bexio/articles/descriptions")
@login_required
def api_bexio_articles_descriptions():
    """
    Batch endpoint to resolve descriptions for a list of article IDs.
    Request JSON:
      { "ids": [544, 123, ...] }
    Response JSON:
      { "items": [ { "article_id": 544, "intern_description_html": "<div>...</div>" }, ... ] }
    """
    data = request.get_json(silent=True) or {}
    ids_raw = data.get("ids") or data.get("product_ids") or []
    if not isinstance(ids_raw, list) or not ids_raw:
        return jsonify({"items": []})
    results = []
    for v in ids_raw:
        try:
            aid = int(str(v).strip())
        except Exception:
            continue
        html = fetch_bexio_article_description(aid)
        results.append({
            "article_id": aid,
            "intern_description_html": html,
        })
    return jsonify({"items": results})


# ----------------------------
# Two-phase flow: draft review and finalize
# ----------------------------
@app.get("/review-invoice/<draft_id>")
@login_required
def review_invoice(draft_id: str):
    return render_template("review_invoice.html", draft_id=draft_id)


@app.get("/api/draft/<draft_id>")
@login_required
def api_get_draft(draft_id: str):
    conn = get_invoices_db()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT draft_id, client_name, invoice_name, payload_json, title_invoice, header_invoice, footer_invoice, currency_exchange, status, created_at, updated_at, finalized_at FROM draft_invoices WHERE draft_id = ?",
            (draft_id,)
        )
        r = cur.fetchone()
        if not r:
            return jsonify({"error": "not found"}), 404
        try:
            payload_obj = json.loads(r[3] or "{}")
        except Exception:
            payload_obj = {}
        # Payload is already enriched when saved; no need to re-enrich on fetch
        return jsonify({
            "draft_id": r[0],
            "client_name": r[1],
            "invoice_name": r[2],
            "payload": payload_obj,
            "title_invoice": r[4],
            "header_invoice": r[5],
            "footer_invoice": r[6],
            "currency_exchange": r[7],
            "status": r[8],
            "created_at": r[9],
            "updated_at": r[10],
            "finalized_at": r[11],
        })
    finally:
        conn.close()


@app.put("/api/draft/<draft_id>")
@login_required
def api_update_draft(draft_id: str):
    data = request.get_json(silent=True) or {}
    payload_obj = data.get("payload")
    invoice_name_new = data.get("invoice_name")
    title_new = data.get("title_invoice")
    header_new = data.get("header_invoice")
    footer_new = data.get("footer_invoice")
    currency_exchange_new = data.get("currency_exchange")
    if payload_obj is None and invoice_name_new is None and title_new is None and header_new is None and footer_new is None and currency_exchange_new is None:
        return jsonify({"error": "nothing to update"}), 400
    now_iso = datetime.utcnow().isoformat() + "Z"
    conn = get_invoices_db()
    try:
        cur = conn.cursor()
        # Fetch existing
        cur.execute("SELECT invoice_name, title_invoice, header_invoice, footer_invoice, currency_exchange FROM draft_invoices WHERE draft_id = ?", (draft_id,))
        existing = cur.fetchone()
        if not existing:
            return jsonify({"error": "not found"}), 404
        invoice_name_final = strip_trailing_pdf((invoice_name_new or existing[0]) or "") or None
        title_final = title_new if title_new is not None else existing[1]
        header_final = header_new if header_new is not None else existing[2]
        footer_final = footer_new if footer_new is not None else existing[3]
        currency_final = currency_exchange_new if currency_exchange_new is not None else existing[4]
        cur.execute(
            """
            UPDATE draft_invoices
            SET payload_json = COALESCE(?, payload_json),
                invoice_name = ?,
                title_invoice = ?,
                header_invoice = ?,
                footer_invoice = ?,
                currency_exchange = ?,
                updated_at = ?
            WHERE draft_id = ?
            """,
            (
                (json.dumps(payload_obj, ensure_ascii=False) if payload_obj is not None else None),
                invoice_name_final,
                title_final,
                header_final,
                footer_final,
                currency_final,
                now_iso,
                draft_id,
            )
        )
        if cur.rowcount == 0:
            return jsonify({"error": "not found"}), 404
        conn.commit()
        return jsonify({"ok": True})
    finally:
        conn.close()


@app.post("/api/finalize_invoice")
@login_required
def api_finalize_invoice():
    if not (USE_TWO_PHASE_FLOW and GENERATE_INVOICE_WEBHOOK_URL):
        return jsonify({"error": "two-phase flow disabled"}), 400
    data = request.get_json(silent=True) or {}
    draft_id = (data.get("draft_id") or "").strip()
    if not draft_id:
        return jsonify({"error": "draft_id required"}), 400
    conn = get_invoices_db()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT client_name, invoice_name, payload_json, title_invoice, header_invoice, footer_invoice, currency_exchange FROM draft_invoices WHERE draft_id = ?",
            (draft_id,)
        )
        r = cur.fetchone()
        if not r:
            return jsonify({"error": "not found"}), 404
        client_name = r[0]
        invoice_name = r[1]
        payload_json = r[2] or "{}"
        title_invoice = r[3]
        header_invoice = r[4]
        footer_invoice = r[5]
        currency_exchange_raw = r[6]
    finally:
        conn.close()

    # Send payload JSON to second workflow
    try:
        payload_obj = json.loads(payload_json)
    except Exception:
        payload_obj = {}
    
    # Clean payload: Remove intern_code and intern_name from positions (Bexio doesn't support these fields)
    def clean_payload_for_bexio(obj):
        """Remove intern_code and intern_name from all positions before sending to Bexio."""
        if isinstance(obj, dict):
            # Remove intern_code and intern_name from this dict
            obj.pop('intern_code', None)
            obj.pop('intern_name', None)
            # Recursively clean nested dicts
            for key, value in obj.items():
                if isinstance(value, (dict, list)):
                    clean_payload_for_bexio(value)
        elif isinstance(obj, list):
            # Recursively clean each item in list
            for item in obj:
                clean_payload_for_bexio(item)
        return obj
    
    # Apply cleanup
    payload_obj = clean_payload_for_bexio(payload_obj)
    
    try:
        timeout_arg = None if INFINITE_WEBHOOK_TIMEOUT else (WEBHOOK_CONNECT_TIMEOUT_SEC, WEBHOOK_READ_TIMEOUT_SEC)
        # Provide metadata alongside payload as headers or query params is not ideal; include in a wrapper
        # but keep the user payload untouched as body
        resp = requests.post(GENERATE_INVOICE_WEBHOOK_URL, json=payload_obj, timeout=timeout_arg)
        ok = 200 <= resp.status_code < 300
        content = resp.content or b""
        looks_pdf = (len(content) > 0 and content[:4] == b"%PDF")
        if not ok or not looks_pdf:
            msg = tr("flash_webhook_fail", status=resp.status_code) if not ok else "Invalid or empty PDF returned"
            return jsonify({"error": msg}), 502

        disp = resp.headers.get("Content-Disposition", "")
        fallback_name = "invoice.pdf"
        if "filename=" in disp:
            try:
                fallback_name = disp.split("filename=")[1].strip('"') or fallback_name
            except Exception:
                pass
        final_name = invoice_name or fallback_name
        safe_final = secure_filename(final_name)
        if not safe_final.lower().endswith(".pdf"):
            safe_final += ".pdf"

        archive_filename = f"{uuid.uuid4()}.pdf"
        archive_rel = archive_filename
        archive_path = os.path.join(INVOICES_DIR, archive_filename)
        with open(archive_path, "wb") as f:
            f.write(resp.content)
        size_bytes = os.path.getsize(archive_path)

        record = _add_invoice_record(safe_final, client_name, archive_rel, size_bytes)
        try:
            add_invoice_db_record(record["id"], record["name"], record["client"], record["file"], record["size"], record["created_at"])
        except Exception:
            pass

        tmp_path = os.path.join(DOWNLOAD_TMP_DIR, f"{record['id']}.pdf")
        with open(tmp_path, "wb") as f:
            f.write(resp.content)

        # Mark draft as finalized
        try:
            conn2 = get_invoices_db()
            cur2 = conn2.cursor()
            now_iso = datetime.utcnow().isoformat() + "Z"
            cur2.execute("UPDATE draft_invoices SET status = 'finalized', finalized_at = ?, updated_at = ? WHERE draft_id = ?", (now_iso, now_iso, draft_id))
            conn2.commit()
        finally:
            try:
                conn2.close()
            except Exception:
                pass

        return jsonify({
            "id": record["id"],
            "name": record["name"],
            "preview_url": url_for("preview_invoice", invoice_id=record["id"]),
            "download_url": url_for("download_invoice_once", invoice_id=record["id"]),
            "created_at": record["created_at"],
            "size": record["size"],
        })
    except Exception as e:
        return jsonify({"error": tr("flash_webhook_send_error", error=str(e))}), 500


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000, debug=True)


