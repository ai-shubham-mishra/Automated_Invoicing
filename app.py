import os
import json
import sqlite3
from datetime import datetime
import uuid
from typing import Any
from functools import wraps

from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify, send_file, after_this_request
from werkzeug.utils import secure_filename

import pandas as pd
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

# Set your n8n webhook URL here directly
WEBHOOK_URL = os.getenv("INVOICE_WEBHOOK_URL")  # e.g., "http://localhost:5678/webhook/your-path"
# Read timeout minutes for webhook response (default 5 minutes)
try:
    _timeout_min_raw = os.getenv("INVOICE_WEBHOOK_TIMEOUT_MIN")
    WEBHOOK_TIMEOUT_MIN = int(_timeout_min_raw) if _timeout_min_raw else 5
except Exception:
    WEBHOOK_TIMEOUT_MIN = 5
if WEBHOOK_TIMEOUT_MIN <= 0:
    WEBHOOK_TIMEOUT_MIN = 5
WEBHOOK_CONNECT_TIMEOUT_SEC = 30
WEBHOOK_READ_TIMEOUT_SEC = WEBHOOK_TIMEOUT_MIN * 60

ALLOWED_EXCEL_EXTENSIONS = {".xlsx", ".xlsm"}
ALLOWED_PDF_EXTENSIONS = {".pdf"}

app = Flask(__name__)
app.config["SECRET_KEY"] = "dev-secret"
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB default


# ----------------------------
# Preise DB helpers (exact schema, no metadata)
# ----------------------------
def get_pricing_db() -> sqlite3.Connection:
    conn = sqlite3.connect(PRICING_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def pricing_table_exists(conn: sqlite3.Connection) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='preise'")
    return cur.fetchone() is not None


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


def init_db() -> None:
    # Nothing to initialize for pricing DB beyond file existence; table is recreated on import
    os.makedirs(UPLOAD_DIR, exist_ok=True)
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
        conn.commit()
    except Exception:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass

def get_invoices_db() -> sqlite3.Connection:
    conn = sqlite3.connect(INVOICES_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

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


# ----------------------------
# i18n / language
# ----------------------------
TRANSLATIONS = {
    "en": {
        "brand": "Generate Invoice",
        "nav_feeddata": "Feed data",
        "nav_invoicecreation": "Invoice creation",
        "feed_title": "Upload price sheet",
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
        "prev": "Prev",
        "next": "Next",
    },
    "de": {
        "brand": "Rechnung erstellen",
        "nav_feeddata": "Einspeisen",
        "nav_invoicecreation": "Rechnungserstellung",
        "feed_title": "Preisliste hochladen",
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
        # Create table and insert (full overwrite)
        pconn = get_pricing_db()
        drop_pricing_table(pconn)
        create_pricing_table(pconn, headers)
        inserted = insert_pricing_rows(pconn, headers, rows)
        pconn.close()

        flash(f"Preise sheet imported with {inserted} rows.", "success")
        return redirect(url_for("invoicecreation_get"))

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
    # For compatibility if called elsewhere: return exact rows from Preise DB
    pconn = get_pricing_db()
    try:
        if not pricing_table_exists(pconn):
            return []
        return fetch_rows_for_kunde(pconn, client_name)
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


@app.post("/api/generate_invoice")
@login_required
def api_generate_invoice():
    client_name = (request.form.get("client_name") or "").strip()
    invoice_name = (request.form.get("invoice_name") or "").strip()
    if not client_name:
        flash("Bitte einen Kunden auswählen.", "error")
        return jsonify({"error": "client_name required"}), 400

    delivery_notes = request.files.getlist("delivery_notes")
    valid_pdfs = []
    for f in delivery_notes:
        if not f or not (f.filename or "").lower().endswith(".pdf"):
            continue
        valid_pdfs.append(f)

    if not WEBHOOK_URL:
        return jsonify({"error": tr("webhook_not_set")}), 400

    # Build exact-key array from Preise table filtered by Kunde_Name
    try:
        pconn = get_pricing_db()
        if not pricing_table_exists(pconn):
            pconn.close()
            return jsonify({"error": "no pricing data"}), 400
        rows = fetch_rows_for_kunde(pconn, client_name)
        pconn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    # Build multipart form-data to emit N items under the same field name `data`
    # and N matching binary parts under `binary[<index>]`, plus a single `schema` field.
    data_fields: list[tuple[str, str]] = []
    file_parts: list[tuple[str, tuple[str, Any, str]]] = []

    # Attach the pricing rows once as a standalone schema field
    data_fields.append(("schema", json.dumps(rows, ensure_ascii=False)))

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
        resp = requests.post(
            WEBHOOK_URL,
            data=data_fields,
            files=file_parts,
            timeout=(WEBHOOK_CONNECT_TIMEOUT_SEC, WEBHOOK_READ_TIMEOUT_SEC),
        )
        ok = 200 <= resp.status_code < 300
        if not ok:
            return jsonify({"error": tr("flash_webhook_fail", status=resp.status_code)}), 502

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


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000, debug=True)


