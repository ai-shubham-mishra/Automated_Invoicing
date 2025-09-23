import os
import json
import sqlite3
from datetime import datetime

from flask import Flask, render_template, request, redirect, url_for, flash, session
from werkzeug.utils import secure_filename

import pandas as pd
import requests


# ----------------------------
# App configuration (no .env)
# ----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "tmp", "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

DB_PATH = os.path.join(BASE_DIR, "invoicing_db")  # sqlite file named invoicing_db

# Set your n8n webhook URL here directly
WEBHOOK_URL = os.getenv("INVOICE_WEBHOOK_URL")  # e.g., "http://localhost:5678/webhook/your-path"

ALLOWED_EXCEL_EXTENSIONS = {".xlsx", ".xlsm"}
ALLOWED_PDF_EXTENSIONS = {".pdf"}

app = Flask(__name__)
app.config["SECRET_KEY"] = "dev-secret"
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB default


# ----------------------------
# Database helpers
# ----------------------------
def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # enforce foreign keys & cascades
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db() -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    # price_sheets: strict unique per client_name
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS price_sheets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_name TEXT NOT NULL UNIQUE,
            sheet_name TEXT NOT NULL,
            currency TEXT,
            valid_from TEXT,
            valid_to TEXT,
            metadata_json TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )
    # price_sheet_items: items for a sheet
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS price_sheet_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sheet_id INTEGER NOT NULL,
            sku TEXT NOT NULL,
            name TEXT NOT NULL,
            unit TEXT NOT NULL,
            price REAL NOT NULL,
            vat REAL,
            discounts_json TEXT,
            notes TEXT,
            extra_json TEXT,
            FOREIGN KEY(sheet_id) REFERENCES price_sheets(id) ON DELETE CASCADE
        );
        """
    )
    # Minimal migration: ensure extra_json exists
    cur.execute("PRAGMA table_info(price_sheet_items);")
    cols = [r[1] for r in cur.fetchall()]
    if "extra_json" not in cols:
        try:
            cur.execute("ALTER TABLE price_sheet_items ADD COLUMN extra_json TEXT;")
        except Exception:
            pass
    conn.commit()
    conn.close()


# ----------------------------
# Excel parsing
# ----------------------------
GERMAN_HEADER_MAP = {
    "artikelnummer": "sku",
    "artnr": "sku",
    "bezeichnung": "name",
    "produkt": "name",
    "einheit": "unit",
    "preis": "price",
    "listenpreis": "price",
    "mwst": "vat",
    "ust": "vat",
    "rabatt": "discounts",
    "rabattstaffel": "discounts",
    "notizen": "notes",
}


def normalize_header(col: str) -> str:
    key = str(col).strip().lower()
    return GERMAN_HEADER_MAP.get(key, key)


def normalize_key(col: str) -> str:
    s = str(col).strip().lower()
    for ch in ["(", ")", "[", "]", "/", "\\", ".", ",", ":", ";", "-", "_"]:
        s = s.replace(ch, " ")
    s = " ".join(s.split())
    return s


SKU_SYNONYMS = {
    "sku", "artikelnummer", "artnr", "artikelnr", "artikel nr", "art nr", "art. nr",
    "artikel", "code", "produktcode", "product code", "item code", "nummer", "nr"
}
NAME_SYNONYMS = {
    "bezeichnung", "produkt", "artikelbezeichnung", "produktname", "name", "warenbezeichnung",
    "description", "beschreibung", "artikel"
}
UNIT_SYNONYMS = {
    "einheit", "me", "ve", "maßeinheit", "masseinheit", "unit", "uom"
}
PRICE_SYNONYMS = {
    "preis", "listenpreis", "vk", "vk preis", "verkaufspreis", "netto", "netto preis",
    "nettopreis", "brutto", "brutto preis", "bruttopreis", "price", "unit price", "einzelpreis"
}
VAT_SYNONYMS = {"mwst", "ust", "vat", "tax", "steuer"}
DISCOUNT_SYNONYMS = {
    "rabatt", "rabattstaffel", "staffel", "staffelpreis", "mengenrabatt", "discount"
}
NOTES_SYNONYMS = {"notizen", "hinweise", "notes", "bemerkung", "bemerkungen"}


def find_column_by_synonyms(df: pd.DataFrame, synonyms: set[str], numeric: bool | None = None) -> str | None:
    candidates = []
    for col in df.columns:
        key = normalize_key(col)
        if key in synonyms:
            candidates.append(col)
    if not candidates:
        return None
    if numeric is None:
        return candidates[0]
    # prefer numeric columns if requested
    for col in candidates:
        series = df[col]
        numeric_count = 0
        total = len(series)
        for v in series:
            if parse_decimal(v) is not None:
                numeric_count += 1
        if total > 0 and numeric_count / max(1, total) > 0.5:
            return col
    return candidates[0]


def guess_best_numeric_column(df: pd.DataFrame) -> str | None:
    best_col = None
    best_score = -1.0
    for col in df.columns:
        series = df[col]
        numeric_count = 0
        positive_count = 0
        for v in series:
            val = parse_decimal(v)
            if val is not None:
                numeric_count += 1
                if val > 0:
                    positive_count += 1
        score = numeric_count + 0.5 * positive_count
        if score > best_score:
            best_score = score
            best_col = col
    return best_col


def infer_columns(df: pd.DataFrame) -> dict:
    mapping: dict[str, str | None] = {"sku": None, "name": None, "unit": None, "price": None, "vat": None, "discounts": None, "notes": None, "category": None, "pack": None}
    # try direct synonyms
    mapping["sku"] = find_column_by_synonyms(df, SKU_SYNONYMS)
    mapping["name"] = find_column_by_synonyms(df, NAME_SYNONYMS)
    mapping["unit"] = find_column_by_synonyms(df, UNIT_SYNONYMS)
    col_price = find_column_by_synonyms(df, PRICE_SYNONYMS, numeric=True)
    mapping["price"] = col_price or guess_best_numeric_column(df)
    mapping["vat"] = find_column_by_synonyms(df, VAT_SYNONYMS, numeric=True)
    mapping["discounts"] = find_column_by_synonyms(df, DISCOUNT_SYNONYMS)
    mapping["notes"] = find_column_by_synonyms(df, NOTES_SYNONYMS)
    # optional enrichers
    # category-like
    for col in df.columns:
        key = normalize_key(col)
        if key in {"kategorie", "warengruppe", "category", "gruppe"}:
            mapping["category"] = col
        if key in {"packung", "packungseinheit", "pack", "pack size", "ve"}:
            mapping["pack"] = col
    return mapping


def parse_decimal(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s:
        return None
    s = s.replace("€", "").replace(" ", "")
    # convert German decimal comma to dot
    s = s.replace(".", "").replace(",", ".") if "," in s and s.count(",") == 1 else s
    try:
        return float(s)
    except ValueError:
        return None


def canonicalize_unit(unit_value: str) -> str:
    if not unit_value:
        return ""
    u = str(unit_value).strip().lower()
    if u in {"kg", "kilogramm"}:
        return "kg"
    if u in {"stk", "stück", "stueck", "piece"}:
        return "piece"
    if u in {"l", "liter"}:
        return "l"
    return unit_value


def parse_discounts(value) -> str:
    # store as JSON string; parsing simple patterns like "ab 100kg: 5,00€"
    if value is None or str(value).strip() == "":
        return None
    text = str(value)
    # naive parse -> keep as raw text in an array for now
    payload = {"raw": text}
    return json.dumps(payload, ensure_ascii=False)


def is_string_with_letters(value) -> bool:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    s = str(value).strip()
    if not s:
        return False
    return any(ch.isalpha() for ch in s)


def detect_header_row(raw_df: pd.DataFrame, max_scan_rows: int = 10) -> int:
    # Heuristic: choose the row with the most text cells and synonym hits, penalize numeric rows
    max_rows = min(len(raw_df), max_scan_rows)
    best_idx = 0
    best_score = float("-inf")
    all_synonyms = SKU_SYNONYMS | NAME_SYNONYMS | UNIT_SYNONYMS | PRICE_SYNONYMS | VAT_SYNONYMS | DISCOUNT_SYNONYMS | NOTES_SYNONYMS
    for idx in range(max_rows):
        row = raw_df.iloc[idx]
        text_count = 0
        numeric_like = 0
        synonym_hits = 0
        for v in row:
            if is_string_with_letters(v):
                text_count += 1
                key = normalize_key(v)
                if key in all_synonyms:
                    synonym_hits += 1
            else:
                if parse_decimal(v) is not None:
                    numeric_like += 1
        score = text_count + 2 * synonym_hits - 0.5 * numeric_like
        if score > best_score:
            best_score = score
            best_idx = idx
    return best_idx


def build_dataframe_with_detected_header(xls: pd.ExcelFile, sheet_name: str) -> pd.DataFrame:
    raw = xls.parse(sheet_name=sheet_name, header=None, dtype=object)
    # Drop completely empty rows at top/bottom to help detection
    raw = raw.dropna(how="all")
    if raw.empty:
        return pd.DataFrame()
    header_row_idx = detect_header_row(raw)
    # Create columns
    header_values = list(raw.iloc[header_row_idx].values)
    columns = []
    used = {}
    for i, v in enumerate(header_values):
        name = str(v).strip() if v is not None and not (isinstance(v, float) and pd.isna(v)) else f"col_{i+1}"
        if not name:
            name = f"col_{i+1}"
        # ensure uniqueness
        base = name
        count = used.get(base, 0)
        if count:
            name = f"{base}_{count+1}"
        used[base] = count + 1
        columns.append(name)
    data = raw.iloc[header_row_idx + 1 :].copy()
    data.columns = columns[: len(data.columns)]
    # Normalize: drop fully empty rows and columns
    data = data.dropna(how="all")
    data = data.dropna(axis=1, how="all")
    return data


def parse_pack_info(value) -> tuple[float | None, str | None, float | None]:
    if value is None:
        return None, None, None
    s = str(value).strip().lower()
    # examples: "karton 10kg", "6 x 2kg", "12x1l", "ve 10 kg"
    import re
    m = re.search(r"(\d+)\s*[x×]\s*(\d*[\.,]?\d+)\s*(kg|g|l|ml|stk|st[üu]ck)?", s)
    if m:
        count = float(m.group(1))
        size = parse_decimal(m.group(2))
        unit = m.group(3) or "piece"
        factor = None
        if size is not None and unit in {"kg", "g", "l", "ml"}:
            factor = count * size
        return size, unit, factor
    m2 = re.search(r"(\d*[\.,]?\d+)\s*(kg|g|l|ml)", s)
    if m2:
        size = parse_decimal(m2.group(1))
        unit = m2.group(2)
        return size, unit, size
    return None, None, None


def dataframe_to_items(df: pd.DataFrame) -> list[dict]:
    # dynamic: infer columns by heuristics; keep original headers
    inferred = infer_columns(df)

    sku_col = inferred["sku"]
    name_col = inferred["name"]
    unit_col = inferred["unit"]
    price_col = inferred["price"]
    vat_col = inferred["vat"]
    discounts_col = inferred["discounts"]
    notes_col = inferred["notes"]
    category_col = inferred.get("category")
    pack_col = inferred.get("pack")

    if not price_col:
        raise ValueError("Required column missing: price")
    # name can be synthesized from sku; sku can be synthesized from name+row index

    items: list[dict] = []
    for idx, row in df.iterrows():
        raw_price = row.get(price_col)
        price = parse_decimal(raw_price)
        if price is None:
            continue

        raw_name = row.get(name_col) if name_col else None
        name = str(raw_name).strip() if raw_name is not None else ""

        raw_sku = row.get(sku_col) if sku_col else None
        sku = str(raw_sku).strip() if raw_sku is not None else ""

        # synthesize when missing
        if not sku and name:
            base = "".join(ch for ch in name.lower() if ch.isalnum())[:16]
            sku = f"AUTO-{base}-{idx+1}"
        if not name and sku:
            name = sku
        if not name and not sku:
            # cannot accept unnamed/unidentified item
            continue

        raw_unit = row.get(unit_col) if unit_col else None
        unit = canonicalize_unit(raw_unit) if raw_unit is not None else "piece"
        if not unit:
            unit = "piece"

        vat = parse_decimal(row.get(vat_col)) if vat_col else None
        discounts_json = parse_discounts(row.get(discounts_col)) if discounts_col else None
        notes = str(row.get(notes_col)).strip() if notes_col and row.get(notes_col) is not None else None

        category = str(row.get(category_col)).strip() if category_col and row.get(category_col) is not None else None
        pack_size, pack_unit, conversion_factor = parse_pack_info(row.get(pack_col)) if pack_col else (None, None, None)

        # preserve ALL original columns exactly as in the sheet
        original_map = {}
        for col in df.columns:
            val = row.get(col)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                continue
            original_map[str(col)] = str(val)

        # additional non-destructive extras
        extra = {}
        if category:
            extra["category"] = category
        if pack_size is not None or pack_unit is not None or conversion_factor is not None:
            extra["pack_size"] = pack_size
            extra["pack_unit"] = pack_unit
            extra["conversion_factor"] = conversion_factor

        items.append(
            {
                "sku": sku,
                "name": name,
                "unit": unit,
                "price": price,
                "vat": vat,
                "discounts_json": discounts_json,
                "notes": notes,
                "category": category,
                "pack_size": pack_size,
                "pack_unit": pack_unit,
                "conversion_factor": conversion_factor,
                "original": original_map if original_map else None,
                "extra": extra if extra else None,
            }
        )
    if not items:
        raise ValueError("No valid rows found in the spreadsheet after validation.")
    return items


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
        "dropdown_label": "Price sheet (Client)",
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
        "dropdown_label": "Preisliste (Client)",
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
        for sheet_name in xls.sheet_names:
            try:
                df = build_dataframe_with_detected_header(xls, sheet_name)
                items = dataframe_to_items(df)

                now = datetime.utcnow().isoformat() + "Z"
                metadata = {
                    "source_file": filename,
                    "imported_at": now,
                    "sheet_name": sheet_name,
                }

                client_name = str(sheet_name).strip()
                conn = get_db_connection()
                cur = conn.cursor()

                # strict overwrite: remove existing sheet for this client
                cur.execute("SELECT id FROM price_sheets WHERE client_name = ?", (client_name,))
                row = cur.fetchone()
                if row:
                    sheet_id = row["id"]
                    cur.execute("DELETE FROM price_sheet_items WHERE sheet_id = ?", (sheet_id,))
                    cur.execute("DELETE FROM price_sheets WHERE id = ?", (sheet_id,))

                cur.execute(
                    """
                    INSERT INTO price_sheets (client_name, sheet_name, currency, valid_from, valid_to, metadata_json, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        client_name,
                        sheet_name,
                        currency,
                        valid_from,
                        valid_to,
                        json.dumps(metadata, ensure_ascii=False),
                        now,
                        now,
                    ),
                )
                new_sheet_id = cur.lastrowid

                for item in items:
                    cur.execute(
                        """
                        INSERT INTO price_sheet_items (sheet_id, sku, name, unit, price, vat, discounts_json, notes, extra_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            new_sheet_id,
                            item["sku"],
                            item["name"],
                            item["unit"],
                            item["price"],
                            item["vat"],
                            item["discounts_json"],
                            item["notes"],
                            json.dumps({
                                "category": item.get("category"),
                                "pack_size": item.get("pack_size"),
                                "pack_unit": item.get("pack_unit"),
                                "conversion_factor": item.get("conversion_factor"),
                                "original": item.get("original"),
                                "extra": item.get("extra"),
                            }, ensure_ascii=False),
                        ),
                    )

                conn.commit()
                conn.close()
                ok_details.append(f"{client_name} ({len(items)})")
            except Exception as se:
                fail_details.append(f"{sheet_name}: {se}")

        # Summaries
        if ok_details:
            flash(f"Imported {len(ok_details)} sheet(s). {len(fail_details)} failed.", "success")
            flash(f"OK: {', '.join(ok_details)}", "success")
        if fail_details:
            flash(f"Errors: {', '.join(fail_details)}", "error")
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
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT client_name FROM price_sheets ORDER BY client_name ASC")
    clients = [row[0] for row in cur.fetchall()]
    conn.close()
    return render_template("invoicecreation.html", clients=clients)


def build_pricing_json_for_client(client_name: str) -> dict:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, sheet_name, currency, valid_from, valid_to, metadata_json FROM price_sheets WHERE client_name = ?",
        (client_name,),
    )
    sheet = cur.fetchone()
    if not sheet:
        conn.close()
        raise ValueError("Client not found or no price sheet uploaded.")
    sheet_id = sheet["id"]
    cur.execute(
        "SELECT sku, name, unit, price, vat, discounts_json, notes, extra_json FROM price_sheet_items WHERE sheet_id = ?",
        (sheet_id,),
    )
    items = []
    for row in cur.fetchall():
        discounts = json.loads(row["discounts_json"]) if row["discounts_json"] else None
        extra = json.loads(row["extra_json"]) if row["extra_json"] else None

        # Prefer original sheet columns at top-level for each item
        if isinstance(extra, dict) and isinstance(extra.get("original"), dict):
            item_obj = dict(extra["original"])  # copy to avoid mutation
        else:
            # Fallback to normalized fields if original not available (older imports)
            item_obj = {
                "sku": row["sku"],
                "name": row["name"],
                "unit": row["unit"],
                "price": row["price"],
                "vat": row["vat"],
                "discounts": discounts,
                "notes": row["notes"],
            }

        # Merge selected enrichments if present (category/pack info/extra map)
        if isinstance(extra, dict):
            for key in ("category", "pack_size", "pack_unit", "conversion_factor"):
                if key in extra and extra[key] is not None and key not in item_obj:
                    item_obj[key] = extra[key]
            if extra.get("extra") and "extra" not in item_obj:
                item_obj["extra"] = extra["extra"]

        items.append(item_obj)
    payload = {
        "client_name": client_name,
        "sheet_name": sheet["sheet_name"],
        "currency": sheet["currency"],
        "valid_from": sheet["valid_from"],
        "valid_to": sheet["valid_to"],
        "items": items,
        "metadata": json.loads(sheet["metadata_json"]) if sheet["metadata_json"] else None,
    }
    conn.close()
    return payload


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

    try:
        pricing_json = build_pricing_json_for_client(client_name)
    except Exception as e:
        flash(str(e), "error")
        return redirect(url_for("invoicecreation_get"))

    # Build multipart form-data: JSON schema as string field, PDFs as files
    files = []
    form_data = {
        "schema": json.dumps(pricing_json, ensure_ascii=False)
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


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000, debug=True)


