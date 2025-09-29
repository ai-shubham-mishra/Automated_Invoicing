import os
import json
import sqlite3
from datetime import datetime

from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
from werkzeug.utils import secure_filename

import pandas as pd
import requests


# ----------------------------
# App configuration (no .env)
# ----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "tmp", "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Dedicated Preise database (exact headers, single table, full overwrite)
PRICING_DB_PATH = os.path.join(BASE_DIR, "pricing_sheet.db")

# Set your n8n webhook URL here directly
WEBHOOK_URL = os.getenv("INVOICE_WEBHOOK_URL")  # e.g., "http://localhost:5678/webhook/your-path"

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
        "brand": "Price Lists",
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
    },
    "de": {
        "brand": "Preislisten",
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
    return {"t": tr, "lang": get_lang()}


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
@app.route("/")
def index():
    return redirect(url_for("feeddata_get"))


@app.get("/feeddata")
def feeddata_get():
    return render_template("feeddata.html")


@app.post("/feeddata")
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


@app.post("/invoicecreation")
def invoicecreation_post():
    client_name = (request.form.get("client_name") or "").strip()
    if not client_name:
        flash("Bitte einen Kunden auswählen.", "error")
        return redirect(url_for("invoicecreation_get"))

    delivery_notes = request.files.getlist("delivery_notes")
    valid_pdfs = []
    for f in delivery_notes:
        if not f or not (f.filename or "").lower().endswith(".pdf"):
            continue
        valid_pdfs.append(f)

    if not WEBHOOK_URL:
        flash(tr("webhook_not_set"), "error")
        return redirect(url_for("invoicecreation_get"))

    # Build exact-key array from Preise table filtered by Kunde_Name
    try:
        pconn = get_pricing_db()
        if not pricing_table_exists(pconn):
            pconn.close()
            flash("Keine Preise-Daten vorhanden.", "error")
            return redirect(url_for("invoicecreation_get"))
        rows = fetch_rows_for_kunde(pconn, client_name)
        pconn.close()
    except Exception as e:
        flash(str(e), "error")
        return redirect(url_for("invoicecreation_get"))

    # Build multipart form-data: JSON schema as string field, PDFs as files
    files = []
    # Send only the array as JSON string; no wrapper keys
    form_data = {
        "schema": json.dumps(rows, ensure_ascii=False)
    }
    for pdf in valid_pdfs:
        safe_name = secure_filename(pdf.filename or "delivery_note.pdf")
        files.append(("delivery_notes", (safe_name, pdf.stream, "application/pdf")))

    try:
        resp = requests.post(WEBHOOK_URL, data=form_data, files=files, timeout=60)
        ok = 200 <= resp.status_code < 300
        if ok:
            flash(tr("flash_webhook_ok"), "success")
        else:
            flash(tr("flash_webhook_fail", status=resp.status_code), "error")
    except Exception as e:
        flash(tr("flash_webhook_send_error", error=str(e)), "error")

    return redirect(url_for("invoicecreation_get"))


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


