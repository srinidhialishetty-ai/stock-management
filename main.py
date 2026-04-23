import json
import os
import re
import secrets
import smtplib
import string
import xml.etree.ElementTree as ET
import base64
from datetime import datetime, timedelta
from email.message import EmailMessage
from io import BytesIO
from pathlib import Path
from uuid import uuid4
from zipfile import ZipFile

from flask import Flask, flash, jsonify, redirect, render_template, request, send_from_directory, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

from db import create_user, ensure_core_tables, fetch_all, fetch_one, get_all_users, get_inventory_rows_for_role, get_user_by_username, insert_inventory_rows, run_schema, safe_execute, try_get_user_by_username
from utils.analytics_utils import build_chart_payload, build_dashboard_metrics, build_insights, build_inventory_summary
from utils.qr_utils import build_product_qr, decode_qr_payload

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", str(BASE_DIR / "static" / "uploads")))
QR_DIR = Path(os.getenv("QR_DIR", str(BASE_DIR / "static" / "qr")))
ADMIN_PROOF_DIR = BASE_DIR / "static" / "admin_request_proofs"
SCHEMA_PATH = BASE_DIR / "schema.sql"
ALLOWED_EXCEL_EXTENSIONS = {"xlsx"}
ALLOWED_PROOF_EXTENSIONS = {"png", "jpg", "jpeg", "pdf", "txt", "doc", "docx"}
CATEGORIES = ["Kurtis", "Sarees", "Leggings", "Tops", "Dupattas", "Accessories"]
REQUIRED_EXCEL_FIELDS = ["product_name", "price", "quantity"]
EXCEL_MAPPING_FIELDS = ["product_name", "category", "price", "quantity", "date"]
EXCEL_HEADER_ALIASES = {
    "product_name": [
        "product_name",
        "product name",
        "product",
        "item_name",
        "item name",
        "item",
        "material_name",
    ],
    "category": [
        "category",
        "product_category",
        "product category",
        "category_name",
        "category name",
        "department",
        "dept",
        "type",
        "item_type",
        "item type",
        "product_type",
        "product type",
    ],
    "price": [
        "price",
        "cost_price_per_unit_usd",
        "cost price per unit usd",
        "cost_price_per_unit",
        "cost price per unit",
        "unit_price",
        "unit price",
        "product_price",
        "product price",
        "cost",
        "selling_price",
        "selling price",
        "rate",
        "amount",
    ],
    "quantity": [
        "hand_in_stock",
        "hand in stock",
        "hand-in-stock",
        "opening_stock",
        "opening stock",
        "quantity",
        "stock",
        "stock_quantity",
        "stock quantity",
        "qty",
        "available_qty",
        "available qty",
        "units",
        "available_units",
        "available units",
    ],
    "date": [
        "date",
        "transaction_date",
        "transaction date",
        "created_at",
        "created at",
        "order_date",
        "order date",
    ],
}
HEADER_KEYWORDS = {"product", "product name", "stock", "quantity", "price", "date"}
ANALYTICS_DEMO_NAME_MARKERS = {"demo", "sample", "test", "stable user item", "qr demo product"}
DEMO_USERS = {
    "admin": {"password": "admin123", "role": "admin"},
    "user1": {"password": "pass123", "role": "user"},
    "user2": {"password": "pass123", "role": "user"},
    "user3": {"password": "pass123", "role": "user"},
    "manager": {"password": "manager123", "role": "manager"},
}
IS_PRODUCTION = os.getenv("RENDER") == "true" or os.getenv("FLASK_ENV", "").lower() == "production"

app = Flask(__name__)
# UPDATED: Require env secret in production, but keep a local development fallback.
app.secret_key = os.getenv("FLASK_SECRET_KEY") or ("dev-stock-management-secret" if not IS_PRODUCTION else None)
app.config["UPLOAD_FOLDER"] = str(UPLOAD_DIR)
app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024
app.config["ADMIN_CODE_EXPIRY_HOURS"] = int(os.getenv("ADMIN_CODE_EXPIRY_HOURS", "72"))

UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
QR_DIR.mkdir(parents=True, exist_ok=True)
ADMIN_PROOF_DIR.mkdir(parents=True, exist_ok=True)


def login_required(view_func):
    from functools import wraps

    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if "role" not in session:
            flash("Please log in to continue.", "warning")
            return redirect(url_for("login"))
        return view_func(*args, **kwargs)

    return wrapped


def admin_required(view_func):
    from functools import wraps

    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if "role" not in session:
            flash("Please log in to continue.", "warning")
            return redirect(url_for("login"))
        if session.get("role") != "admin":
            flash("Access denied. This tool is available only to administrators.", "error")
            return redirect(url_for("user_dashboard" if session.get("role") == "user" else "guest_dashboard"))
        return view_func(*args, **kwargs)

    return wrapped


def admin_or_manager_required(view_func):
    from functools import wraps

    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if "role" not in session:
            flash("Please log in to continue.", "warning")
            return redirect(url_for("login"))
        if session.get("role") not in {"admin", "manager"}:
            flash("Access denied for this area.", "error")
            return redirect_for_role(session.get("role"))
        return view_func(*args, **kwargs)

    return wrapped


@app.context_processor
def inject_globals():
    cart = cart_summary() if session.get("role") == "user" else {"count": 0, "subtotal": 0, "items": []}
    return {
        "current_user": {
            "user_id": session.get("user_id"),
            "username": session.get("username"),
            "role": session.get("role"),
        },
        "cart_state": cart,
    }


def initialize_app():
    # UPDATED: Ensure local storage folders always exist on startup.
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    QR_DIR.mkdir(parents=True, exist_ok=True)
    ADMIN_PROOF_DIR.mkdir(parents=True, exist_ok=True)
    if IS_PRODUCTION and not app.secret_key:
        raise RuntimeError("FLASK_SECRET_KEY must be set for production deployment.")
    if SCHEMA_PATH.exists():
        with open(SCHEMA_PATH, "r", encoding="utf-8") as schema_file:
            run_schema(schema_file.read())
    ensure_core_tables()


@app.route("/media/qr/<path:filename>")
def qr_media(filename):
    return send_from_directory(QR_DIR, filename)


@app.route("/media/uploads/<path:filename>")
def upload_media(filename):
    return send_from_directory(UPLOAD_DIR, filename)


@app.route("/media/admin-request-proofs/<path:filename>")
@login_required
@admin_required
def admin_request_proof_media(filename):
    return send_from_directory(ADMIN_PROOF_DIR, filename)


def allowed_file(filename, allowed_extensions):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in allowed_extensions


def normalize_email(value):
    return (value or "").strip().lower()


def generate_admin_access_code(length=8):
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def save_admin_request_proof(file_storage):
    if not file_storage or not file_storage.filename:
        return ""
    if not allowed_file(file_storage.filename, ALLOWED_PROOF_EXTENSIONS):
        raise ValueError("Proof file must be PNG, JPG, PDF, TXT, DOC, or DOCX.")
    filename = secure_filename(file_storage.filename)
    stored_name = f"{uuid4().hex}_{filename}"
    destination = ADMIN_PROOF_DIR / stored_name
    file_storage.save(destination)
    return stored_name


def smtp_is_configured():
    return bool(os.getenv("SMTP_HOST") and os.getenv("SMTP_FROM_EMAIL"))


def send_admin_access_code_email(recipient_email, access_code, expires_at):
    if not smtp_is_configured():
        return False, "Email delivery is not configured."

    message = EmailMessage()
    message["Subject"] = "Your admin access code"
    message["From"] = os.getenv("SMTP_FROM_EMAIL")
    message["To"] = recipient_email
    expiry_label = expires_at.strftime("%Y-%m-%d %H:%M UTC") if expires_at else "No expiry"
    message.set_content(
        "\n".join(
            [
                "Your admin access request was approved.",
                f"Access code: {access_code}",
                f"Expires: {expiry_label}",
                "This code is single-use.",
            ]
        )
    )

    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_username = os.getenv("SMTP_USERNAME", "")
    smtp_password = os.getenv("SMTP_PASSWORD", "")
    use_tls = os.getenv("SMTP_USE_TLS", "true").lower() in {"1", "true", "yes"}

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as server:
            server.ehlo()
            if use_tls:
                server.starttls()
                server.ehlo()
            if smtp_username:
                server.login(smtp_username, smtp_password)
            server.send_message(message)
        return True, "Access code sent by email."
    except Exception:
        return False, "The code was generated, but email delivery failed."


def get_admin_requests(limit=None):
    limit_clause = f"FETCH FIRST {int(limit)} ROWS ONLY" if limit else ""
    return fetch_all(
        f"""
        SELECT
            r.id,
            r.name,
            r.email,
            r.username,
            r.reason,
            r.business_name,
            r.proof,
            r.proof_file_path,
            r.status,
            r.reviewed_by,
            r.reviewed_at,
            r.created_at,
            (
                SELECT c.code
                FROM admin_access_codes c
                WHERE c.request_id = r.id AND c.status = 'active' AND c.used_at IS NULL
                ORDER BY c.created_at DESC
                FETCH FIRST 1 ROWS ONLY
            ) AS latest_code,
            (
                SELECT c.expires_at
                FROM admin_access_codes c
                WHERE c.request_id = r.id AND c.status = 'active' AND c.used_at IS NULL
                ORDER BY c.created_at DESC
                FETCH FIRST 1 ROWS ONLY
            ) AS latest_code_expires_at
        FROM admin_requests r
        ORDER BY r.created_at DESC, r.id DESC
        {limit_clause}
        """
    )


def get_admin_request_by_id(request_id):
    return fetch_one(
        """
        SELECT id, name, email, username, reason, business_name, proof, proof_file_path, status, reviewed_by, reviewed_at, created_at
        FROM admin_requests
        WHERE id = :request_id
        """,
        {"request_id": request_id},
    )


def get_generated_admin_code(code):
    return fetch_one(
        """
        SELECT id, request_id, email, code, status, expires_at, used_at, generated_by, created_at
        FROM admin_access_codes
        WHERE UPPER(code) = UPPER(:code)
        """,
        {"code": code},
    )


def deactivate_admin_codes_for_request(request_id):
    safe_execute(
        """
        UPDATE admin_access_codes
        SET status = 'inactive'
        WHERE request_id = :request_id AND status = 'active' AND used_at IS NULL
        """,
        {"request_id": request_id},
    )


def create_admin_access_code(request_row):
    request_id = request_row["ID"]
    deactivate_admin_codes_for_request(request_id)

    code = ""
    for _ in range(8):
        candidate = generate_admin_access_code(length=8)
        if not get_generated_admin_code(candidate):
            code = candidate
            break
    if not code:
        raise RuntimeError("Could not generate a unique admin access code.")

    expires_at = datetime.utcnow() + timedelta(hours=app.config["ADMIN_CODE_EXPIRY_HOURS"])
    safe_execute(
        """
        INSERT INTO admin_access_codes (request_id, email, code, status, expires_at, generated_by)
        VALUES (:request_id, :email, :code, :status, :expires_at, :generated_by)
        """,
        {
            "request_id": request_id,
            "email": request_row["EMAIL"],
            "code": code,
            "status": "active",
            "expires_at": expires_at.strftime("%Y-%m-%d %H:%M:%S"),
            "generated_by": session.get("user_id"),
        },
    )
    safe_execute(
        """
        UPDATE admin_requests
        SET status = 'approved', reviewed_by = :reviewed_by, reviewed_at = CURRENT_TIMESTAMP
        WHERE id = :request_id
        """,
        {"reviewed_by": session.get("user_id"), "request_id": request_id},
    )
    return code, expires_at


def reject_admin_request(request_id):
    safe_execute(
        """
        UPDATE admin_requests
        SET status = 'rejected', reviewed_by = :reviewed_by, reviewed_at = CURRENT_TIMESTAMP
        WHERE id = :request_id
        """,
        {"reviewed_by": session.get("user_id"), "request_id": request_id},
    )
    safe_execute(
        """
        UPDATE admin_access_codes
        SET status = 'inactive'
        WHERE request_id = :request_id AND status = 'active' AND used_at IS NULL
        """,
        {"request_id": request_id},
    )


def validate_generated_admin_code(code, email=""):
    code_row = get_generated_admin_code(code)
    if not code_row:
        return None
    if (code_row.get("STATUS") or "").lower() != "active":
        return None
    if code_row.get("USED_AT"):
        return None
    expires_at = code_row.get("EXPIRES_AT")
    if expires_at:
        try:
            expiry_dt = datetime.fromisoformat(str(expires_at).replace("T", " "))
            if expiry_dt < datetime.utcnow():
                return None
        except ValueError:
            pass
    code_email = normalize_email(code_row.get("EMAIL"))
    provided_email = normalize_email(email)
    if provided_email and code_email and provided_email != code_email:
        return None
    return code_row


def mark_generated_admin_code_used(code_id):
    safe_execute(
        """
        UPDATE admin_access_codes
        SET status = 'used', used_at = CURRENT_TIMESTAMP
        WHERE id = :code_id
        """,
        {"code_id": code_id},
    )


def parse_decimal(value, field_name):
    try:
        return float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name} must be a valid number.")


def parse_int(value, field_name):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name} must be a valid whole number.")
    if parsed < 0:
        raise ValueError(f"{field_name} cannot be negative.")
    return parsed


def clean_category_value(value, fallback=False):
    if value is None:
        return "Uncategorized" if fallback else None
    category = str(value).strip()
    if not category or category.lower() in {"nan", "none", "null"}:
        return "Uncategorized" if fallback else None
    return re.sub(r"\s+", " ", category)


def category_for_storage(value):
    return clean_category_value(value, fallback=True)


def normalize_upload_rows(raw_rows):
    normalized = []
    for row in raw_rows:
        product_name = str(row.get("product_name", "")).strip()
        category = clean_category_value(row.get("category"))
        if not product_name:
            continue
        normalized.append(
            {
                "product_name": product_name,
                "category": category,
                "price": float(row.get("price", 0) or 0),
                "quantity": int(row.get("quantity", 0) or 0),
                "reorder_level": int(float(row.get("reorder_level", 10) or 10)),
                "supplier_name": str(row.get("supplier_name", "") or "").strip() or None,
                "branch_name": str(row.get("branch_name", "") or "").strip() or None,
                "image_url": str(row.get("image_url", "") or "").strip() or None,
                "date": str(row.get("date", "") or datetime.now().strftime("%Y-%m-%d")).strip(),
            }
        )
    return normalized


def normalize_excel_header(header):
    normalized = str(header).strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized


def read_excel_dataframe(file_path):
    return read_excel_table(file_path)


def read_excel_table(file_path):
    try:
        import pandas as pd
        return read_excel_with_detected_header(pd, file_path)
    except ImportError:
        return read_xlsx_without_pandas(file_path)
    except Exception:
        return read_xlsx_without_pandas(file_path)


def header_match_score(values):
    score = 0
    all_aliases = set()
    for aliases in EXCEL_HEADER_ALIASES.values():
        all_aliases.update(normalize_excel_header(alias) for alias in aliases)
    for value in values:
        raw_value = str(value).strip().lower()
        normalized = normalize_excel_header(value)
        if normalized in all_aliases:
            score += 3
        for keyword in HEADER_KEYWORDS:
            if keyword in raw_value:
                score += 1
    return score


def detect_header_row_from_matrix(rows):
    best_index = 0
    best_score = -1
    for index, row in enumerate(rows[:15]):
        non_empty_values = [value for value in row if str(value).strip() and str(value).lower() != "nan"]
        if len(non_empty_values) < 2:
            continue
        score = header_match_score(non_empty_values)
        if score > best_score:
            best_score = score
            best_index = index
    return best_index if best_score > 0 else 0


def read_excel_with_detected_header(pd, file_path):
    df_raw = pd.read_excel(file_path, header=None, engine="openpyxl")
    rows = df_raw.fillna("").values.tolist()
    if not rows:
        return None, "The Excel file appears to be empty."

    header_index = detect_header_row_from_matrix(rows)
    df = pd.read_excel(file_path, header=header_index, engine="openpyxl")
    df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")
    headers = [str(value).strip() for value in df.columns if not str(value).lower().startswith("unnamed")]
    df = df[[column for column in df.columns if not str(column).lower().startswith("unnamed")]]
    records = df.fillna("").to_dict("records")

    if not records:
        return None, "We found headers, but no usable data rows below them."
    return {
        "headers": headers,
        "records": records,
        "metadata": {
            "detected_header_row": header_index + 1,
            "raw_headers": headers,
            "normalized_headers": [normalize_excel_header(header) for header in headers],
            "preview_records": records[:5],
        },
    }, None


def read_xlsx_without_pandas(file_path):
    try:
        with ZipFile(file_path) as archive:
            shared_strings = []
            if "xl/sharedStrings.xml" in archive.namelist():
                root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
                namespace = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
                for item in root.findall("x:si", namespace):
                    text_parts = [node.text or "" for node in item.findall(".//x:t", namespace)]
                    shared_strings.append("".join(text_parts))

            sheet_root = ET.fromstring(archive.read("xl/worksheets/sheet1.xml"))
            namespace = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
            rows = []
            for row in sheet_root.findall(".//x:row", namespace):
                values = []
                for cell in row.findall("x:c", namespace):
                    cell_type = cell.attrib.get("t")
                    value = ""
                    if cell_type == "inlineStr":
                        text_node = cell.find(".//x:t", namespace)
                        value = text_node.text if text_node is not None else ""
                    else:
                        value_node = cell.find("x:v", namespace)
                        if value_node is not None:
                            value = value_node.text or ""
                            if cell_type == "s" and value.isdigit():
                                value = shared_strings[int(value)]
                    values.append(value)
                rows.append(values)

        if not rows:
            return None, "The Excel file appears to be empty."
        header_index = detect_header_row_from_matrix(rows)
        headers = [str(header) for header in rows[header_index]]
        keep_indexes = [
            index for index, header in enumerate(headers)
            if header and not str(header).lower().startswith("unnamed")
        ]
        headers = [headers[index] for index in keep_indexes]
        records = []
        for row in rows[header_index + 1:]:
            if not any(str(value).strip() for value in row):
                continue
            required_width = (max(keep_indexes) + 1) if keep_indexes else len(row)
            padded = row + [""] * (required_width - len(row))
            records.append({headers[position]: padded[index] for position, index in enumerate(keep_indexes)})
        if not records:
            return None, "We found headers, but no usable data rows below them."
        return {
            "headers": headers,
            "records": records,
            "metadata": {
                "detected_header_row": header_index + 1,
                "raw_headers": headers,
                "normalized_headers": [normalize_excel_header(header) for header in headers],
                "preview_records": records[:5],
            },
        }, None
    except Exception:
        return None, "We could not read that Excel file. Please upload a valid .xlsx file."


def build_header_context(dataframe):
    if isinstance(dataframe, dict):
        uploaded_headers = dataframe["headers"]
        normalized_to_original = {}
        for column in uploaded_headers:
            normalized = normalize_excel_header(column)
            if normalized and normalized not in normalized_to_original:
                normalized_to_original[normalized] = column
        return uploaded_headers, normalized_to_original

    uploaded_headers = [str(column) for column in dataframe.columns]
    normalized_to_original = {}
    for column in dataframe.columns:
        normalized = normalize_excel_header(column)
        if normalized and normalized not in normalized_to_original:
            normalized_to_original[normalized] = column
    return uploaded_headers, normalized_to_original


def auto_map_excel_headers(normalized_to_original):
    mapped = {}
    for field, aliases in EXCEL_HEADER_ALIASES.items():
        for alias in aliases:
            normalized_alias = normalize_excel_header(alias)
            if normalized_alias in normalized_to_original:
                mapped[field] = normalized_to_original[normalized_alias]
                break
    return mapped


def get_mapped_value(row, mapping, field_name, default=None):
    column = mapping.get(field_name)
    if not column:
        return default
    return row.get(column, default)


def sample_category_values(dataframe, mapping, limit=5):
    category_column = mapping.get("category")
    if not category_column:
        return []
    if isinstance(dataframe, dict):
        return [
            clean_category_value(row.get(category_column))
            for row in dataframe.get("records", [])[:limit]
        ]
    return [
        clean_category_value(row.get(category_column))
        for _, row in dataframe.head(limit).iterrows()
    ]


def log_category_mapping_debug(dataframe, uploaded_headers, mapping, rows=None):
    metadata = dataframe.get("metadata", {}) if isinstance(dataframe, dict) else {}
    print("Upload headers detected:", uploaded_headers)
    print("Upload headers normalized:", metadata.get("normalized_headers", [normalize_excel_header(header) for header in uploaded_headers]))
    print("Mapped category source column:", mapping.get("category") or "NOT FOUND")
    print("First category values read:", sample_category_values(dataframe, mapping))
    if rows is not None:
        print("Category save confirmation:", [row.get("category") for row in rows[:5]])


def prepare_rows_for_save(rows):
    prepared = []
    raw_categories = [row.get("category") for row in rows[:8]]
    for row in rows:
        cleaned_category = clean_category_value(row.get("category"))
        prepared.append(
            {
                **row,
                "product_name": str(row.get("product_name", "")).strip(),
                "category": cleaned_category if cleaned_category else "Uncategorized",
                "price": float(row.get("price", 0) or 0),
                "quantity": int(row.get("quantity", 0) or 0),
                "reorder_level": int(float(row.get("reorder_level", 10) or 10)),
                "supplier_name": str(row.get("supplier_name", "") or "").strip() or None,
                "branch_name": str(row.get("branch_name", "") or "").strip() or None,
                "image_url": str(row.get("image_url", "") or "").strip() or None,
                "date": str(row.get("date", "") or datetime.now().strftime("%Y-%m-%d")).strip(),
            }
        )
    print("Raw category values before save:", raw_categories)
    print("Cleaned category values before insert:", [row.get("category") for row in prepared[:8]])
    return prepared


def rows_from_mapped_dataframe(dataframe, mapping):
    if isinstance(dataframe, dict):
        raw_records = dataframe["records"]
        rows = []
        for row in raw_records:
            try:
                rows.append(
                    {
                        "product_name": str(row[mapping["product_name"]]).strip(),
                        "category": get_mapped_value(row, mapping, "category"),
                        "price": float(row[mapping["price"]]),
                        "quantity": int(float(row[mapping["quantity"]])),
                        "reorder_level": float(get_mapped_value(row, mapping, "reorder_level", 10) or 10),
                        "supplier_name": get_mapped_value(row, mapping, "supplier_name"),
                        "branch_name": get_mapped_value(row, mapping, "branch_name"),
                        "image_url": get_mapped_value(row, mapping, "image_url"),
                        "date": str(get_mapped_value(row, mapping, "date", datetime.now().strftime("%Y-%m-%d")))[:10],
                    }
                )
            except Exception:
                continue
        return normalize_upload_rows(rows)

    rows = []
    for _, row in dataframe.iterrows():
        try:
            rows.append(
                {
                    "product_name": str(row[mapping["product_name"]]).strip(),
                    "category": get_mapped_value(row, mapping, "category"),
                    "price": float(row[mapping["price"]]),
                    "quantity": int(row[mapping["quantity"]]),
                    "reorder_level": float(get_mapped_value(row, mapping, "reorder_level", 10) or 10),
                    "supplier_name": get_mapped_value(row, mapping, "supplier_name"),
                    "branch_name": get_mapped_value(row, mapping, "branch_name"),
                    "image_url": get_mapped_value(row, mapping, "image_url"),
                    "date": str(get_mapped_value(row, mapping, "date", datetime.now().strftime("%Y-%m-%d")))[:10],
                }
            )
        except Exception:
            continue
    return normalize_upload_rows(rows)


def parse_excel_upload(file_path):
    dataframe, error = read_excel_dataframe(file_path)
    if error:
        return {"status": "error", "message": error, "rows": []}

    uploaded_headers, normalized_to_original = build_header_context(dataframe)
    mapping = auto_map_excel_headers(normalized_to_original)
    metadata = dataframe.get("metadata", {}) if isinstance(dataframe, dict) else {}
    metadata["mapped_columns"] = {field: str(column) for field, column in mapping.items()}
    if "category" not in mapping:
        metadata["warnings"] = ["Category column not found. Some items were grouped as Uncategorized."]
    log_category_mapping_debug(dataframe, uploaded_headers, mapping)
    missing = [field for field in REQUIRED_EXCEL_FIELDS if field not in mapping]
    if missing:
        return {
            "status": "mapping_required",
            "message": "Some Excel columns need manual mapping before import.",
            "uploaded_headers": uploaded_headers,
            "missing_fields": missing,
            "auto_mapping": {field: str(column) for field, column in mapping.items()},
            "metadata": metadata,
            "rows": [],
        }

    rows = rows_from_mapped_dataframe(dataframe, mapping)
    if not rows:
        return {"status": "error", "message": "The file did not contain any usable inventory rows.", "rows": []}
    log_category_mapping_debug(dataframe, uploaded_headers, mapping, rows)
    metadata["rows_processed"] = len(dataframe.get("records", [])) if isinstance(dataframe, dict) else len(rows)
    metadata["valid_rows"] = len(rows)
    metadata["failed_rows"] = max(metadata["rows_processed"] - len(rows), 0)
    metadata["parsed_preview"] = rows[:8]
    return {"status": "success", "message": "Excel data parsed successfully.", "rows": rows, "metadata": metadata}


def parse_excel_upload_with_manual_mapping(file_path, mapping):
    dataframe, error = read_excel_dataframe(file_path)
    if error:
        return False, error, []

    clean_mapping = {field: mapping.get(field) for field in EXCEL_MAPPING_FIELDS if mapping.get(field)}
    missing = [field for field in REQUIRED_EXCEL_FIELDS if not clean_mapping.get(field)]
    if missing:
        return False, f"Please map these required fields: {', '.join(missing)}.", []

    rows = rows_from_mapped_dataframe(dataframe, clean_mapping)
    if not rows:
        return False, "The mapped columns did not produce any usable inventory rows.", []
    metadata = dataframe.get("metadata", {}) if isinstance(dataframe, dict) else {}
    metadata["mapped_columns"] = {field: str(column) for field, column in clean_mapping.items()}
    if "category" not in clean_mapping:
        metadata["warnings"] = ["Category column not found. Some items were grouped as Uncategorized."]
    uploaded_headers, _normalized_to_original = build_header_context(dataframe)
    log_category_mapping_debug(dataframe, uploaded_headers, clean_mapping, rows)
    metadata["rows_processed"] = len(dataframe.get("records", [])) if isinstance(dataframe, dict) else len(rows)
    metadata["valid_rows"] = len(rows)
    metadata["failed_rows"] = max(metadata["rows_processed"] - len(rows), 0)
    metadata["parsed_preview"] = rows[:8]
    return True, "Excel data mapped and parsed successfully.", rows, metadata


def store_rows_in_products_and_sales(rows):
    for row in rows:
        category = category_for_storage(row.get("category"))
        product_success, _message, _ = safe_execute(
            """
            INSERT INTO products (name, category, price, quantity, reorder_level, supplier_name, branch_name, image_url)
            VALUES (:name, :category, :price, :quantity, :reorder_level, :supplier_name, :branch_name, :image_url)
            """,
            {
                "name": row["product_name"],
                "category": category,
                "price": row["price"],
                "quantity": row["quantity"],
                "reorder_level": row.get("reorder_level", 10),
                "supplier_name": row.get("supplier_name"),
                "branch_name": row.get("branch_name"),
                "image_url": row.get("image_url"),
            },
        )
        if not product_success:
            safe_execute(
                """
                UPDATE products
                SET price = :price,
                    quantity = :quantity,
                    reorder_level = :reorder_level,
                    supplier_name = :supplier_name,
                    branch_name = :branch_name,
                    image_url = :image_url
                WHERE name = :name AND category = :category
                """,
                {
                    "name": row["product_name"],
                    "category": category,
                    "price": row["price"],
                    "quantity": row["quantity"],
                    "reorder_level": row.get("reorder_level", 10),
                    "supplier_name": row.get("supplier_name"),
                    "branch_name": row.get("branch_name"),
                    "image_url": row.get("image_url"),
                },
            )


def save_uploaded_rows(rows, report_batch=None):
    rows = prepare_rows_for_save(rows)
    if report_batch:
        rows = [{**row, "report_id": report_batch["report_id"]} for row in rows]
    if session.get("role") == "guest":
        guest_rows = session.get("guest_inventory_rows", [])
        guest_rows.extend(rows)
        session["guest_inventory_rows"] = guest_rows
        print("Final stored category values:", [row.get("category") for row in rows[:8]])
        return True, f"{len(rows)} rows uploaded for guest analytics. They will not be saved permanently."

    success, message = insert_inventory_rows(session.get("user_id"), rows)
    if not success:
        return False, message
    store_rows_in_products_and_sales(rows)
    if report_batch:
        stored_count = fetch_one(
            """
            SELECT COUNT(*) AS row_count
            FROM inventory_data
            WHERE report_id = :report_id
            """,
            {"report_id": report_batch["report_id"]},
        )
        print("Rows stored for uploaded report:", stored_count.get("ROW_COUNT", 0) if stored_count else 0)
    print("Final stored category values:", [row.get("category") for row in rows[:8]])
    return True, message


def make_upload_report(filename, status, message, rows, metadata=None, report_batch=None):
    metadata = metadata or {}
    analytics_rows = [
        {
            "ID": index + 1,
            "USER_ID": session.get("user_id"),
            "USERNAME": session.get("username"),
            "PRODUCT_NAME": row["product_name"],
            "CATEGORY": row["category"],
            "PRICE": row["price"],
            "QUANTITY": row["quantity"],
            "ENTRY_DATE": row["date"],
            "CREATED_AT": row["date"],
        }
        for index, row in enumerate(rows)
    ]
    report = {
        "id": report_batch.get("report_id") if report_batch else uuid4().hex,
        "filename": filename,
        "source_file_name": report_batch.get("source_file_name") if report_batch else filename,
        "status": status,
        "message": message,
        "created_at": datetime.now().strftime("%d %b %Y, %I:%M %p"),
        "created_by": session.get("username", "Admin"),
        "uploaded_report_id": report_batch.get("report_id") if report_batch else None,
        "share_token": report_batch.get("token") if report_batch else None,
        "catalog_title": report_batch.get("title") if report_batch else None,
        "rows_processed": metadata.get("rows_processed", len(rows)),
        "valid_rows": metadata.get("valid_rows", len(rows)),
        "failed_rows": metadata.get("failed_rows", max(metadata.get("rows_processed", len(rows)) - len(rows), 0)),
        "detected_header_row": metadata.get("detected_header_row"),
        "raw_headers": metadata.get("raw_headers", []),
        "normalized_headers": metadata.get("normalized_headers", []),
        "mapped_columns": metadata.get("mapped_columns", {}),
        "warnings": metadata.get("warnings", []),
        "preview_rows": metadata.get("parsed_preview", rows[:8]),
        "charts": build_lightweight_analytics(analytics_rows).get("charts", {}),
        "qr_filename": None,
        "view_token": report_batch.get("token") if report_batch else None,
    }
    # UPDATED: Persist the latest successful report by report_id instead of in-memory storage.
    if report_batch and report_batch.get("report_id"):
        session["last_upload_report_id"] = report_batch["report_id"]
    return report


def build_dataframe_analytics(rows):
    try:
        import pandas as pd
    except ImportError:
        return build_lightweight_analytics(rows)

    if not rows:
        return {
            "available": True,
            "charts": {},
            "insights": ["Upload inventory records to generate analytics."],
            "fast_items": [],
            "slow_items": [],
            "low_stock": [],
            "monthly_sales": [],
            "profit_total": 0,
        }

    df = pd.DataFrame(rows)
    df["PRODUCT_NAME"] = df["PRODUCT_NAME"].fillna("Unknown")
    df["CATEGORY"] = df["CATEGORY"].fillna("Uncategorized")
    df["PRICE"] = pd.to_numeric(df["PRICE"], errors="coerce").fillna(0)
    df["QUANTITY"] = pd.to_numeric(df["QUANTITY"], errors="coerce").fillna(0).astype(int)
    df["ENTRY_DATE"] = pd.to_datetime(df["ENTRY_DATE"].fillna(df.get("CREATED_AT")), errors="coerce")
    df["MONTH"] = df["ENTRY_DATE"].dt.to_period("M").astype(str).fillna("No date")
    df["INVENTORY_VALUE"] = df["PRICE"] * df["QUANTITY"]
    df["ESTIMATED_COST"] = df["PRICE"] * 0.7
    df["ESTIMATED_PROFIT"] = (df["PRICE"] - df["ESTIMATED_COST"]) * df["QUANTITY"]

    product_sales = (
        df.groupby("PRODUCT_NAME", as_index=False)["QUANTITY"]
        .sum()
        .sort_values("QUANTITY", ascending=False)
    )
    fast_items = product_sales.head(5).to_dict("records")
    slow_items = product_sales.sort_values("QUANTITY", ascending=True).head(5).to_dict("records")
    low_stock = df[df["QUANTITY"] < 10].sort_values("QUANTITY").head(10).to_dict("records")
    monthly_sales = df.groupby("MONTH", as_index=False)["QUANTITY"].sum().to_dict("records")
    category_sales = df.groupby("CATEGORY", as_index=False)["QUANTITY"].sum().sort_values("QUANTITY", ascending=False)

    insights = []
    if fast_items:
        insights.append(f"{fast_items[0]['PRODUCT_NAME']} is a fast-moving item based on highest sales quantity.")
    if slow_items:
        insights.append(f"{slow_items[0]['PRODUCT_NAME']} is slow-moving and may need promotion or bundling.")
    if not category_sales.empty:
        insights.append(f"{category_sales.iloc[0]['CATEGORY']} has the highest sales volume.")
    if low_stock:
        insights.append(f"{low_stock[0]['PRODUCT_NAME']} has low stock and needs restocking.")

    return {
        "available": True,
        "charts": build_matplotlib_charts(df, product_sales, category_sales),
        "insights": insights,
        "fast_items": fast_items,
        "slow_items": slow_items,
        "low_stock": low_stock,
        "monthly_sales": monthly_sales,
        "profit_total": float(df["ESTIMATED_PROFIT"].sum()),
    }


def build_lightweight_analytics(rows):
    if not rows:
        return {
            "available": True,
            "charts": {},
            "insights": ["Upload inventory records to generate analytics."],
            "fast_items": [],
            "slow_items": [],
            "low_stock": [],
            "monthly_sales": [],
            "profit_total": 0,
        }

    product_totals = {}
    category_totals = {}
    month_totals = {}
    low_stock = []
    profit_total = 0
    for row in rows:
        product = row.get("PRODUCT_NAME", "Unknown")
        category = row.get("CATEGORY", "Uncategorized")
        quantity = int(row.get("QUANTITY", 0) or 0)
        price = float(row.get("PRICE", 0) or 0)
        month = str(row.get("ENTRY_DATE") or row.get("CREATED_AT") or "No date")[:7]
        product_totals[product] = product_totals.get(product, 0) + quantity
        category_totals[category] = category_totals.get(category, 0) + quantity
        month_totals[month] = month_totals.get(month, 0) + quantity
        profit_total += (price - (price * 0.7)) * quantity
        if quantity < 10:
            low_stock.append({"PRODUCT_NAME": product, "QUANTITY": quantity})

    fast_items = [
        {"PRODUCT_NAME": name, "QUANTITY": qty}
        for name, qty in sorted(product_totals.items(), key=lambda item: item[1], reverse=True)[:5]
    ]
    slow_items = [
        {"PRODUCT_NAME": name, "QUANTITY": qty}
        for name, qty in sorted(product_totals.items(), key=lambda item: item[1])[:5]
    ]
    monthly_sales = [
        {"MONTH": month, "QUANTITY": qty}
        for month, qty in sorted(month_totals.items())
    ]
    insights = []
    if fast_items:
        insights.append(f"{fast_items[0]['PRODUCT_NAME']} is a fast-moving item based on highest quantity.")
    if slow_items:
        insights.append(f"{slow_items[0]['PRODUCT_NAME']} is slow-moving and may need promotion.")
    if category_totals:
        top_category = max(category_totals.items(), key=lambda item: item[1])[0]
        insights.append(f"{top_category} has the highest stock/sales volume.")
    if low_stock:
        insights.append(f"{low_stock[0]['PRODUCT_NAME']} has low stock and needs restocking.")

    return {
        "available": True,
        "charts": {
            "top_products": build_svg_bar_chart(product_totals, "Top Products"),
            "monthly_sales": build_svg_line_chart(month_totals, "Monthly Sales Trend"),
            "low_stock": build_svg_bar_chart({item["PRODUCT_NAME"]: item["QUANTITY"] for item in low_stock[:5]}, "Low Stock Items"),
            "category_distribution": build_svg_pie_placeholder(category_totals, "Category Distribution"),
        },
        "insights": insights,
        "fast_items": fast_items,
        "slow_items": slow_items,
        "low_stock": low_stock,
        "monthly_sales": monthly_sales,
        "profit_total": profit_total,
    }


def svg_data_uri(svg):
    encoded = base64.b64encode(svg.encode("utf-8")).decode("utf-8")
    return f"data:image/svg+xml;base64,{encoded}"


def build_svg_bar_chart(values, title):
    items = sorted(values.items(), key=lambda item: item[1], reverse=True)[:5]
    max_value = max([value for _label, value in items], default=1) or 1
    bars = []
    labels = []
    for index, (label, value) in enumerate(items):
        height = int((value / max_value) * 180)
        x = 60 + index * 100
        y = 245 - height
        bars.append(f'<rect x="{x}" y="{y}" width="54" height="{height}" rx="8" fill="#8B5E3C"/>')
        labels.append(f'<text x="{x + 27}" y="280" text-anchor="middle" font-size="11" fill="#5C5C5C">{str(label)[:12]}</text>')
        labels.append(f'<text x="{x + 27}" y="{y - 8}" text-anchor="middle" font-size="12" fill="#2B2B2B">{value}</text>')
    svg = f'''
    <svg xmlns="http://www.w3.org/2000/svg" width="620" height="320" viewBox="0 0 620 320">
      <rect width="620" height="320" rx="24" fill="#fffaf1"/>
      <text x="32" y="38" font-size="22" font-weight="700" fill="#2B2B2B">{title}</text>
      <line x1="42" y1="250" x2="580" y2="250" stroke="#E3D8C4"/>
      {''.join(bars)}
      {''.join(labels)}
    </svg>
    '''
    return svg_data_uri(svg)


def build_svg_line_chart(values, title):
    items = sorted(values.items())
    max_value = max([value for _label, value in items], default=1) or 1
    points = []
    labels = []
    for index, (label, value) in enumerate(items[:6]):
        x = 60 + index * 90
        y = 245 - int((value / max_value) * 170)
        points.append((x, y))
        labels.append(f'<text x="{x}" y="280" text-anchor="middle" font-size="11" fill="#5C5C5C">{str(label)[-7:]}</text>')
    polyline = " ".join(f"{x},{y}" for x, y in points)
    circles = "".join(f'<circle cx="{x}" cy="{y}" r="5" fill="#6F7D5C"/>' for x, y in points)
    svg = f'''
    <svg xmlns="http://www.w3.org/2000/svg" width="620" height="320" viewBox="0 0 620 320">
      <rect width="620" height="320" rx="24" fill="#fffaf1"/>
      <text x="32" y="38" font-size="22" font-weight="700" fill="#2B2B2B">{title}</text>
      <line x1="42" y1="250" x2="580" y2="250" stroke="#E3D8C4"/>
      <polyline points="{polyline}" fill="none" stroke="#6F7D5C" stroke-width="4"/>
      {circles}
      {''.join(labels)}
    </svg>
    '''
    return svg_data_uri(svg)


def build_svg_pie_placeholder(values, title):
    total = sum(values.values()) or 1
    top_items = sorted(values.items(), key=lambda item: item[1], reverse=True)[:5]
    legend = []
    colors = ["#8B5E3C", "#C08A5D", "#6F7D5C", "#B77B52", "#D2B595"]
    for index, (label, value) in enumerate(top_items):
        y = 88 + index * 34
        percent = round((value / total) * 100, 1)
        legend.append(f'<rect x="340" y="{y - 14}" width="18" height="18" rx="4" fill="{colors[index % len(colors)]}"/>')
        legend.append(f'<text x="368" y="{y}" font-size="14" fill="#2B2B2B">{label}: {percent}%</text>')
    svg = f'''
    <svg xmlns="http://www.w3.org/2000/svg" width="620" height="320" viewBox="0 0 620 320">
      <rect width="620" height="320" rx="24" fill="#fffaf1"/>
      <text x="32" y="38" font-size="22" font-weight="700" fill="#2B2B2B">{title}</text>
      <circle cx="170" cy="165" r="86" fill="#8B5E3C"/>
      <path d="M170 165 L170 79 A86 86 0 0 1 248 201 Z" fill="#C08A5D"/>
      <path d="M170 165 L248 201 A86 86 0 0 1 116 231 Z" fill="#6F7D5C"/>
      {''.join(legend)}
    </svg>
    '''
    return svg_data_uri(svg)


def figure_to_base64(fig):
    buffer = BytesIO()
    fig.tight_layout()
    fig.savefig(buffer, format="png", dpi=130, bbox_inches="tight")
    buffer.seek(0)
    encoded = base64.b64encode(buffer.getvalue()).decode("utf-8")
    return f"data:image/png;base64,{encoded}"


def build_matplotlib_charts(df, product_sales, category_sales):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return {}

    charts = {}
    palette = ["#8B5E3C", "#C08A5D", "#6F7D5C", "#B77B52", "#D2B595"]

    top_products = product_sales.head(5)
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.bar(top_products["PRODUCT_NAME"], top_products["QUANTITY"], color=palette[: len(top_products)])
    ax.set_title("Top Products")
    ax.set_ylabel("Sales Quantity")
    ax.tick_params(axis="x", rotation=25)
    charts["top_products"] = figure_to_base64(fig)
    plt.close(fig)

    monthly = df.groupby("MONTH", as_index=False)["QUANTITY"].sum()
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.plot(monthly["MONTH"], monthly["QUANTITY"], marker="o", color="#6F7D5C")
    ax.set_title("Monthly Sales Trend")
    ax.set_ylabel("Quantity")
    ax.tick_params(axis="x", rotation=25)
    charts["monthly_sales"] = figure_to_base64(fig)
    plt.close(fig)

    low_stock = df[df["QUANTITY"] < 10].sort_values("QUANTITY").head(5)
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.bar(low_stock["PRODUCT_NAME"], low_stock["QUANTITY"], color="#C08A5D")
    ax.set_title("Low Stock Items")
    ax.set_ylabel("Stock Quantity")
    ax.tick_params(axis="x", rotation=25)
    charts["low_stock"] = figure_to_base64(fig)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(6, 4))
    if not category_sales.empty:
        ax.pie(category_sales["QUANTITY"], labels=category_sales["CATEGORY"], autopct="%1.1f%%", colors=palette)
    ax.set_title("Category Distribution")
    charts["category_distribution"] = figure_to_base64(fig)
    plt.close(fig)

    return charts


def get_active_rows():
    role = session.get("role")
    if role == "guest":
        guest_rows = session.get("guest_inventory_rows", [])
        return [
            {
                "ID": index + 1,
                "USER_ID": None,
                "USERNAME": "Guest",
                "PRODUCT_NAME": row["product_name"],
                "CATEGORY": row["category"],
                "PRICE": row["price"],
                "QUANTITY": row["quantity"],
                "ENTRY_DATE": row["date"],
                "CREATED_AT": row["date"],
            }
            for index, row in enumerate(guest_rows)
        ]
    return get_inventory_rows_for_role(role, session.get("user_id"))


def is_real_business_name(name):
    clean_name = str(name or "").strip().lower()
    if not clean_name:
        return False
    return not any(marker in clean_name for marker in ANALYTICS_DEMO_NAME_MARKERS)


def filter_real_inventory_rows(rows):
    return [row for row in rows if is_real_business_name(row.get("PRODUCT_NAME"))]


def cleaned_analytics_category(value):
    return clean_category_value(value) or "Uncategorized"


def get_sales_rows_for_analytics():
    if session.get("role") not in {"admin", "manager"}:
        return []
    rows = fetch_all(
        """
        SELECT p.name AS product_name,
               p.category AS category,
               s.quantity AS quantity,
               s.sale_date AS sale_date,
               s.total_amount AS total_amount
        FROM sales s
        JOIN products p ON p.product_id = s.product_id
        ORDER BY s.sale_date ASC, s.sale_id ASC
        """
    )
    order_rows = fetch_all(
        """
        SELECT oi.product_name, oi.category, oi.quantity, o.created_at AS sale_date, oi.line_total AS total_amount
        FROM order_items oi
        JOIN orders o ON o.order_id = oi.order_id
        ORDER BY o.created_at ASC, oi.order_item_id ASC
        """
    )
    return [
        row for row in (rows + order_rows)
        if is_real_business_name(row.get("PRODUCT_NAME"))
    ]


def get_sales_rows_for_report(report_id):
    if not report_id:
        return []
    rows = fetch_all(
        """
        SELECT oi.product_name, oi.category, oi.quantity, o.created_at AS sale_date, oi.line_total AS total_amount
        FROM order_items oi
        JOIN orders o ON o.order_id = oi.order_id
        WHERE o.report_id = :report_id
        ORDER BY o.created_at ASC, oi.order_item_id ASC
        """,
        {"report_id": report_id},
    )
    return [row for row in rows if is_real_business_name(row.get("PRODUCT_NAME"))]


def build_business_analytics(inventory_rows, sales_rows):
    inventory_by_product = {}
    category_stock = {}
    category_value = {}
    low_stock = []

    for row in inventory_rows:
        product_name = str(row.get("PRODUCT_NAME", "")).strip()
        category = cleaned_analytics_category(row.get("CATEGORY"))
        quantity = int(float(row.get("QUANTITY", 0) or 0))
        price = float(row.get("PRICE", 0) or 0)
        value = price * quantity
        product = inventory_by_product.setdefault(
            product_name,
            {"product_name": product_name, "category": category, "stock": 0, "sold": 0, "value": 0, "price": price},
        )
        product["stock"] += quantity
        product["value"] += value
        product["price"] = max(product["price"], price)
        category_stock[category] = category_stock.get(category, 0) + quantity
        category_value[category] = category_value.get(category, 0) + value

    sales_by_product = {}
    trend_by_date = {}
    total_orders = 0
    for row in sales_rows:
        product_name = str(row.get("PRODUCT_NAME", "")).strip()
        if not product_name:
            continue
        quantity = int(float(row.get("QUANTITY", 0) or 0))
        total_orders += quantity
        sales_by_product[product_name] = sales_by_product.get(product_name, 0) + quantity
        sale_date = row.get("SALE_DATE") or row.get("CREATED_AT") or "No date"
        if hasattr(sale_date, "strftime"):
            sale_key = sale_date.strftime("%Y-%m-%d")
        else:
            sale_key = str(sale_date)[:10] if sale_date else "No date"
        trend_by_date[sale_key] = trend_by_date.get(sale_key, 0) + quantity

    for product_name, sold in sales_by_product.items():
        if product_name in inventory_by_product:
            inventory_by_product[product_name]["sold"] = sold
        else:
            inventory_by_product[product_name] = {
                "product_name": product_name,
                "category": "Uncategorized",
                "stock": 0,
                "sold": sold,
                "value": 0,
                "price": 0,
            }

    products = list(inventory_by_product.values())
    fast_items = sorted(
        [item for item in products if item["sold"] > 0],
        key=lambda item: item["sold"],
        reverse=True,
    )[:10]
    slow_items = sorted(
        [item for item in products if item["stock"] > 0],
        key=lambda item: (item["sold"], -item["stock"]),
    )[:5]
    low_stock = sorted(
        [item for item in products if 0 <= item["stock"] < 10],
        key=lambda item: item["stock"],
    )[:8]
    valuable_products = sorted(products, key=lambda item: item["value"], reverse=True)[:5]
    category_value_rows = [
        {"category": category, "value": value, "stock": category_stock.get(category, 0)}
        for category, value in sorted(category_value.items(), key=lambda item: item[1], reverse=True)
    ]
    print("Analytics category stock counts:", category_stock)
    print("Analytics category inventory values:", {category: round(value, 2) for category, value in category_value.items()})

    top_selling_product = fast_items[0]["product_name"] if fast_items else "No orders yet"
    total_inventory_value = sum(item["value"] for item in products)
    insights = []
    if fast_items:
        insights.append(f"{fast_items[0]['product_name']} is the fastest-moving item with {fast_items[0]['sold']} units sold.")
    if slow_items:
        insights.append(f"{slow_items[0]['product_name']} is slow-moving with {slow_items[0]['sold']} sold and {slow_items[0]['stock']} still in stock.")
    if low_stock:
        insights.append(f"{low_stock[0]['product_name']} needs restocking soon. Only {low_stock[0]['stock']} units remain.")
    if category_stock:
        strongest_category = max(category_stock.items(), key=lambda item: item[1])
        insights.append(f"{strongest_category[0]} has the highest stock concentration with {strongest_category[1]} units.")
    if valuable_products:
        insights.append(f"{valuable_products[0]['product_name']} carries the highest inventory value at Rs. {valuable_products[0]['value']:.2f}.")
    if not insights:
        insights.append("Upload real inventory and order data to generate business recommendations.")

    return {
        "has_data": bool(products),
        "summary": {
            "total_products": len(products),
            "total_inventory_value": total_inventory_value,
            "low_stock_count": len(low_stock),
            "top_selling_product": top_selling_product,
            "total_orders": total_orders,
        },
        "fast_items": fast_items,
        "slow_items": slow_items,
        "low_stock": low_stock,
        "valuable_products": valuable_products,
        "category_value_rows": category_value_rows,
        "insights": insights,
        "charts": {
            "fast_labels": [item["product_name"] for item in fast_items],
            "fast_values": [item["sold"] for item in fast_items],
            "slow_labels": [item["product_name"] for item in slow_items],
            "slow_values": [item["sold"] for item in slow_items],
            "category_labels": list(category_stock.keys()),
            "category_values": list(category_stock.values()),
            "trend_labels": sorted(trend_by_date.keys()),
            "trend_values": [trend_by_date[key] for key in sorted(trend_by_date.keys())],
            "value_labels": [item["product_name"] for item in valuable_products],
            "value_values": [round(item["value"], 2) for item in valuable_products],
        },
    }


def build_url_qr(url_value):
    try:
        import qrcode

        filename = f"product_url_{uuid4().hex[:8]}.png"
        path = QR_DIR / filename
        image = qrcode.make(url_value)
        image.save(path)
        return filename
    except Exception:
        return None


def set_user_session(user_id, username, role):
    session["user_id"] = user_id
    session["username"] = username
    session["role"] = role


def redirect_for_role(role):
    if role == "admin":
        return redirect(url_for("admin_dashboard"))
    if role == "manager":
        return redirect(url_for("manager_dashboard"))
    if role == "guest":
        return redirect(url_for("guest_dashboard"))
    return redirect(url_for("user_dashboard"))


def get_recent_reports(limit=None):
    report_rows = fetch_all(
        f"""
        SELECT r.report_id,
               r.title,
               r.source_file_name,
               r.token,
               r.created_at,
               s.qr_filename,
               COUNT(d.id) AS row_count
        FROM uploaded_reports r
        LEFT JOIN shared_catalogs s ON s.source_report_id = r.report_id
        LEFT JOIN inventory_data d ON d.report_id = r.report_id
        GROUP BY r.report_id, r.title, r.source_file_name, r.token, r.created_at, s.qr_filename
        ORDER BY r.created_at DESC
        FETCH FIRST {int(limit or 25)} ROWS ONLY
        """
    )
    reports = [
        {
            "id": row["REPORT_ID"],
            "filename": row.get("SOURCE_FILE_NAME") or row.get("TITLE"),
            "message": "Report available.",
            "status": "success",
            "valid_rows": int(row.get("ROW_COUNT", 0) or 0),
            "created_at": row.get("CREATED_AT"),
            "qr_filename": row.get("QR_FILENAME"),
            "view_token": row.get("TOKEN"),
        }
        for row in report_rows
    ]
    return reports[:limit] if limit else reports


def load_report_view(report_id):
    report_row = fetch_one(
        """
        SELECT r.report_id, r.title, r.source_file_name, r.token, r.created_at, s.qr_filename, COUNT(d.id) AS row_count
        FROM uploaded_reports r
        LEFT JOIN shared_catalogs s ON s.source_report_id = r.report_id
        LEFT JOIN inventory_data d ON d.report_id = r.report_id
        WHERE r.report_id = :report_id
        GROUP BY r.report_id, r.title, r.source_file_name, r.token, r.created_at, s.qr_filename
        """,
        {"report_id": report_id},
    )
    if not report_row:
        return None
    preview_rows = [
        {
            "product_name": row.get("PRODUCT_NAME"),
            "category": row.get("CATEGORY"),
            "price": float(row.get("PRICE", 0) or 0),
            "quantity": int(row.get("QUANTITY", 0) or 0),
            "date": row.get("ENTRY_DATE") or "-",
        }
        for row in get_catalog_products(report_id)[:8]
    ]
    return {
        "id": report_row["REPORT_ID"],
        "filename": report_row.get("SOURCE_FILE_NAME") or report_row.get("TITLE"),
        "message": "Report available.",
        "status": "success",
        "rows_processed": int(report_row.get("ROW_COUNT", 0) or 0),
        "valid_rows": int(report_row.get("ROW_COUNT", 0) or 0),
        "failed_rows": 0,
        "raw_headers": [],
        "normalized_headers": [],
        "mapped_columns": {},
        "preview_rows": preview_rows,
        "warnings": [],
        "created_at": report_row.get("CREATED_AT"),
        "uploaded_report_id": report_row["REPORT_ID"],
        "catalog_title": report_row.get("TITLE"),
        "share_token": report_row.get("TOKEN"),
        "view_token": report_row.get("TOKEN"),
        "qr_filename": report_row.get("QR_FILENAME"),
        "charts": build_chart_payload(
            [
                {
                    "ID": index + 1,
                    "PRODUCT_NAME": row["product_name"],
                    "CATEGORY": row["category"],
                    "PRICE": row["price"],
                    "QUANTITY": row["quantity"],
                    "ENTRY_DATE": row["date"],
                }
                for index, row in enumerate(preview_rows)
            ]
        ),
    }


def log_activity(action, detail):
    safe_execute(
        """
        INSERT INTO activity_logs (actor_username, actor_role, action, detail)
        VALUES (:actor_username, :actor_role, :action, :detail)
        """,
        {
            "actor_username": session.get("username"),
            "actor_role": session.get("role"),
            "action": action,
            "detail": detail,
        },
    )


def get_recent_activity(limit=12):
    return fetch_all(
        f"""
        SELECT activity_id, actor_username, actor_role, action, detail, created_at
        FROM activity_logs
        ORDER BY created_at DESC, activity_id DESC
        FETCH FIRST {int(limit)} ROWS ONLY
        """
    )


def create_uploaded_report_batch(source_file_name, row_count):
    report_id = uuid4().hex
    token = uuid4().hex[:16]
    title = f"{Path(source_file_name).stem} catalog"
    safe_execute(
        """
        INSERT INTO uploaded_reports (report_id, title, created_by_admin, token, source_file_name, status)
        VALUES (:report_id, :title, :created_by_admin, :token, :source_file_name, :status)
        """,
        {
            "report_id": report_id,
            "title": title,
            "created_by_admin": session.get("user_id"),
            "token": token,
            "source_file_name": source_file_name,
            "status": "active",
        },
    )
    print("Created report_id:", report_id)
    print("Generated token:", token)
    print("Rows assigned to report:", row_count)
    return {
        "report_id": report_id,
        "title": title,
        "token": token,
        "source_file_name": source_file_name,
    }


def get_latest_uploaded_report():
    return fetch_one(
        """
        SELECT report_id, title, created_by_admin, token, source_file_name, status, created_at
        FROM uploaded_reports
        ORDER BY created_at DESC
        FETCH FIRST 1 ROWS ONLY
        """
    )


def get_uploaded_report_by_id(report_id):
    if not report_id:
        return None
    return fetch_one(
        """
        SELECT report_id, title, created_by_admin, token, source_file_name, status, created_at
        FROM uploaded_reports
        WHERE report_id = :report_id
        """,
        {"report_id": report_id},
    )


def get_uploaded_report_by_token(token):
    if not token:
        return None
    return fetch_one(
        """
        SELECT report_id, title, created_by_admin, token, source_file_name, status, created_at
        FROM uploaded_reports
        WHERE token = :token
        """,
        {"token": token},
    )


def get_uploaded_report_by_source_file_name(source_file_name):
    if not source_file_name:
        return None
    return fetch_one(
        """
        SELECT report_id, title, created_by_admin, token, source_file_name, status, created_at
        FROM uploaded_reports
        WHERE LOWER(source_file_name) = LOWER(:source_file_name)
        ORDER BY created_at DESC
        FETCH FIRST 1 ROWS ONLY
        """,
        {"source_file_name": source_file_name},
    )


def get_uploaded_report_options(limit=12):
    return fetch_all(
        f"""
        SELECT report_id, title, token, source_file_name, status, created_at
        FROM uploaded_reports
        ORDER BY created_at DESC
        FETCH FIRST {int(limit)} ROWS ONLY
        """
    )


def count_uploaded_reports():
    row = fetch_one("SELECT COUNT(*) AS total_reports FROM uploaded_reports")
    return int(row.get("TOTAL_REPORTS", 0) or 0) if row else 0


def extract_filename_from_catalog_title(title):
    title_text = str(title or "").strip()
    match = re.search(r"catalog from\s+(.+)$", title_text, re.IGNORECASE)
    return match.group(1).strip() if match else None


def count_rows_for_report(report_id):
    if not report_id:
        return 0
    row = fetch_one(
        """
        SELECT COUNT(*) AS row_count
        FROM inventory_data
        WHERE report_id = :report_id
        """,
        {"report_id": report_id},
    )
    return int(row.get("ROW_COUNT", 0) or 0) if row else 0


def resolve_catalog_report_record(catalog_record):
    if not catalog_record:
        return None
    source_report_id = catalog_record.get("SOURCE_REPORT_ID")
    if source_report_id and count_rows_for_report(source_report_id) > 0:
        return get_uploaded_report_by_id(source_report_id)

    repaired_report = get_uploaded_report_by_id(catalog_record.get("TOKEN"))
    if not repaired_report:
        repaired_report = get_uploaded_report_by_source_file_name(extract_filename_from_catalog_title(catalog_record.get("TITLE")))
    if not repaired_report:
        latest_report = get_latest_uploaded_report()
        if latest_report and count_rows_for_report(latest_report["REPORT_ID"]) > 0:
            repaired_report = latest_report

    if repaired_report:
        safe_execute(
            """
            UPDATE shared_catalogs
            SET source_report_id = :source_report_id
            WHERE token = :token
            """,
            {"source_report_id": repaired_report["REPORT_ID"], "token": catalog_record["TOKEN"]},
        )
        catalog_record["SOURCE_REPORT_ID"] = repaired_report["REPORT_ID"]
        print("Repaired shared catalog source_report_id:", repaired_report["REPORT_ID"])
    return repaired_report


def create_or_refresh_shared_catalog(report_id=None, title=None):
    uploaded_report = None
    if report_id:
        uploaded_report = fetch_one(
            """
            SELECT report_id, title, token, source_file_name, created_at, status
            FROM uploaded_reports
            WHERE report_id = :report_id
            """,
            {"report_id": report_id},
        )
    token = uploaded_report["TOKEN"] if uploaded_report else uuid4().hex[:16]
    catalog_title = uploaded_report["TITLE"] if uploaded_report else (title or "Shared Inventory Catalog")
    existing = fetch_one(
        """
        SELECT share_id, token
        FROM shared_catalogs
        WHERE token = :token
        """,
        {"token": token},
    )
    if existing:
        safe_execute(
            """
            UPDATE shared_catalogs
            SET title = :title,
                source_report_id = :source_report_id,
                status = :status
            WHERE token = :token
            """,
            {"title": catalog_title, "source_report_id": report_id, "status": "active", "token": token},
        )
    else:
        safe_execute(
            """
            INSERT INTO shared_catalogs (admin_user_id, title, token, source_report_id, status)
            VALUES (:admin_user_id, :title, :token, :source_report_id, :status)
            """,
            {
                "admin_user_id": session.get("user_id"),
                "title": catalog_title,
                "token": token,
                "source_report_id": report_id,
                "status": "active",
            },
        )
    return get_shared_catalog_by_token(token)


def attach_qr_to_shared_catalog(catalog):
    if not catalog:
        return None
    qr_filename = build_url_qr(build_catalog_link(catalog["TOKEN"]))
    if not qr_filename:
        return catalog
    safe_execute(
        "UPDATE shared_catalogs SET qr_filename = :qr_filename WHERE token = :token",
        {"qr_filename": qr_filename, "token": catalog["TOKEN"]},
    )
    return get_shared_catalog_by_token(catalog["TOKEN"])


def get_shared_catalog_by_token(token):
    return fetch_one(
        """
        SELECT share_id, admin_user_id, title, token, source_report_id, qr_filename, status, created_at
        FROM shared_catalogs
        WHERE token = :token AND status = :status
        """,
        {"token": token, "status": "active"},
    )


def get_latest_shared_catalog():
    return fetch_one(
        """
        SELECT share_id, admin_user_id, title, token, source_report_id, qr_filename, status, created_at
        FROM shared_catalogs
        WHERE status = :status
        ORDER BY created_at DESC, share_id DESC
        FETCH FIRST 1 ROWS ONLY
        """,
        {"status": "active"},
    )


def get_shared_catalogs(limit=5):
    return fetch_all(
        f"""
        SELECT share_id, admin_user_id, title, token, source_report_id, qr_filename, status, created_at
        FROM shared_catalogs
        ORDER BY created_at DESC, share_id DESC
        FETCH FIRST {int(limit)} ROWS ONLY
        """
    )


def build_catalog_link(token):
    return url_for("catalog", token=token, _external=True)


def build_analytics_link(report_id):
    return url_for("analytics", report_id=report_id) if report_id else url_for("analytics")


def extract_token_from_input(value):
    raw_value = str(value or "").strip()
    match = re.search(r"/(?:catalog|view-data)/([A-Za-z0-9]+)", raw_value)
    return match.group(1) if match else raw_value


def resolve_active_analytics_report():
    role = session.get("role")
    requested_report_id = request.args.get("report_id", "").strip()
    if role in {"admin", "manager"}:
        report = (
            get_uploaded_report_by_id(requested_report_id)
            or get_uploaded_report_by_id(session.get("active_analytics_report_id"))
            or get_latest_uploaded_report()
        )
    else:
        token = request.args.get("token", "").strip() or session.get("last_catalog_token")
        catalog_record = get_shared_catalog_by_token(token) if token else get_latest_shared_catalog()
        report = get_uploaded_report_by_id(catalog_record.get("SOURCE_REPORT_ID")) if catalog_record else None
    if report:
        session["active_analytics_report_id"] = report["REPORT_ID"]
    return report


def get_report_inventory_rows(report_id, exclude_demo_names=False):
    if not report_id:
        return []
    rows = fetch_all(
        """
        SELECT id, report_id, user_id, product_name, category, price, quantity, reorder_level, supplier_name, branch_name, image_url, entry_date, created_at
        FROM inventory_data
        WHERE report_id = :report_id
        ORDER BY created_at DESC, id DESC
        """,
        {"report_id": report_id},
    )
    if exclude_demo_names:
        rows = filter_real_inventory_rows(rows)
    normalized_rows = [
        {
            "ID": row["ID"],
            "REPORT_ID": row.get("REPORT_ID"),
            "PRODUCT_NAME": row["PRODUCT_NAME"],
            "CATEGORY": cleaned_analytics_category(row.get("CATEGORY")),
            "PRICE": float(row.get("PRICE", 0) or 0),
            "QUANTITY": int(float(row.get("QUANTITY", 0) or 0)),
            "REORDER_LEVEL": int(float(row.get("REORDER_LEVEL", 10) or 10)),
            "SUPPLIER_NAME": row.get("SUPPLIER_NAME"),
            "BRANCH_NAME": row.get("BRANCH_NAME"),
            "IMAGE_URL": row.get("IMAGE_URL"),
            "ENTRY_DATE": row.get("ENTRY_DATE"),
        }
        for row in rows
    ]
    return normalized_rows


def get_catalog_products(report_id):
    return get_report_inventory_rows(report_id, exclude_demo_names=False)


def get_orders_for_role(limit=12):
    if session.get("role") in {"admin", "manager"}:
        return fetch_all(
            f"""
            SELECT order_id, user_id, username, status, share_token, report_id, total_amount, created_at
            FROM orders
            ORDER BY created_at DESC, order_id DESC
            FETCH FIRST {int(limit)} ROWS ONLY
            """
        )
    return fetch_all(
        f"""
        SELECT order_id, user_id, username, status, share_token, report_id, total_amount, created_at
        FROM orders
        WHERE user_id = :user_id OR username = :username
        ORDER BY created_at DESC, order_id DESC
        FETCH FIRST {int(limit)} ROWS ONLY
        """,
        {"user_id": session.get("user_id"), "username": session.get("username")},
    )


def get_order_items(order_id):
    return fetch_all(
        """
        SELECT order_item_id, order_id, inventory_id, product_name, category, unit_price, quantity, line_total
        FROM order_items
        WHERE order_id = :order_id
        ORDER BY order_item_id ASC
        """,
        {"order_id": order_id},
    )


def get_catalog_metrics(products):
    total_value = sum(float(product.get("PRICE", 0) or 0) * int(product.get("QUANTITY", 0) or 0) for product in products)
    available_count = sum(1 for product in products if int(product.get("QUANTITY", 0) or 0) > 0)
    low_stock_count = sum(
        1
        for product in products
        if 0 < int(product.get("QUANTITY", 0) or 0) <= int(product.get("REORDER_LEVEL", 10) or 10)
    )
    return {
        "total_products": len(products),
        "available_count": available_count,
        "low_stock_count": low_stock_count,
        "inventory_value": total_value,
    }


def get_cart_items():
    if session.get("role") != "user":
        return []
    return fetch_all(
        """
        SELECT c.cart_item_id,
               c.user_id,
               c.share_token,
               c.inventory_id,
               c.report_id,
               c.product_name,
               c.category,
               c.unit_price,
               c.quantity,
               c.created_at,
               i.quantity AS current_stock,
               i.image_url AS image_url,
               i.branch_name AS branch_name,
               i.supplier_name AS supplier_name,
               i.reorder_level AS reorder_level
        FROM cart_items c
        LEFT JOIN inventory_data i ON i.id = c.inventory_id AND i.report_id = c.report_id
        WHERE c.user_id = :user_id
        ORDER BY c.created_at DESC, c.cart_item_id DESC
        """,
        {"user_id": session.get("user_id")},
    )


def cart_summary():
    items = get_cart_items()
    subtotal = sum(float(item.get("UNIT_PRICE", 0) or 0) * int(item.get("QUANTITY", 0) or 0) for item in items)
    return {
        "items": items,
        "count": sum(int(item.get("QUANTITY", 0) or 0) for item in items),
        "subtotal": subtotal,
        "catalog_count": len({item.get("SHARE_TOKEN") for item in items if item.get("SHARE_TOKEN")}),
    }


def build_order_views(limit=50):
    orders = get_orders_for_role(limit)
    return [
        {
            **order,
            "ITEMS": get_order_items(order["ORDER_ID"]),
        }
        for order in orders
    ]


def build_catalog_filters(products):
    categories = sorted({product["CATEGORY"] for product in products if product.get("CATEGORY")})
    branches = sorted({product.get("BRANCH_NAME") for product in products if product.get("BRANCH_NAME")})
    return {"categories": categories, "branches": [branch for branch in branches if branch]}


def filter_catalog_products(products, search, category, stock_level, branch_name, sort_by):
    filtered = products
    if search:
        filtered = [product for product in filtered if search.lower() in product["PRODUCT_NAME"].lower()]
    if category:
        filtered = [product for product in filtered if product["CATEGORY"] == category]
    if branch_name:
        filtered = [product for product in filtered if product.get("BRANCH_NAME") == branch_name]
    if stock_level == "low":
        filtered = [product for product in filtered if product["QUANTITY"] <= product.get("REORDER_LEVEL", 10)]
    elif stock_level == "out":
        filtered = [product for product in filtered if product["QUANTITY"] <= 0]

    if sort_by == "price":
        filtered = sorted(filtered, key=lambda item: item["PRICE"])
    elif sort_by == "stock":
        filtered = sorted(filtered, key=lambda item: item["QUANTITY"], reverse=True)
    elif sort_by == "popularity":
        popularity_rows = fetch_all(
            """
            SELECT product_name, SUM(quantity) AS quantity
            FROM order_items
            GROUP BY product_name
            """
        )
        popularity = {row["PRODUCT_NAME"]: int(row["QUANTITY"] or 0) for row in popularity_rows}
        filtered = sorted(filtered, key=lambda item: popularity.get(item["PRODUCT_NAME"], 0), reverse=True)
    else:
        filtered = sorted(filtered, key=lambda item: item["ID"], reverse=True)
    return filtered


def add_product_to_cart(user_id, product, quantity, share_token):
    existing_cart_items = fetch_all(
        """
        SELECT cart_item_id, share_token, report_id
        FROM cart_items
        WHERE user_id = :user_id
        """,
        {"user_id": user_id},
    )
    if any(
        item.get("SHARE_TOKEN") != share_token or item.get("REPORT_ID") != product.get("REPORT_ID")
        for item in existing_cart_items
    ):
        return False, "Your cart already contains items from another shared catalog. Checkout or clear the cart first."
    existing = fetch_one(
        """
        SELECT cart_item_id, quantity
        FROM cart_items
        WHERE user_id = :user_id AND inventory_id = :inventory_id AND share_token = :share_token
        """,
        {"user_id": user_id, "inventory_id": product["ID"], "share_token": share_token},
    )
    existing_quantity = int(existing["QUANTITY"] or 0) if existing else 0
    available = int(product["QUANTITY"] or 0)
    if existing_quantity + quantity > available:
        return False, f"Only {available} units are available for {product['PRODUCT_NAME']}."
    if existing:
        return safe_execute(
            """
            UPDATE cart_items
            SET quantity = :quantity
            WHERE cart_item_id = :cart_item_id
            """,
            {"quantity": existing_quantity + quantity, "cart_item_id": existing["CART_ITEM_ID"]},
        )[:2]
    return safe_execute(
        """
        INSERT INTO cart_items (user_id, share_token, inventory_id, report_id, product_name, category, unit_price, quantity)
        VALUES (:user_id, :share_token, :inventory_id, :report_id, :product_name, :category, :unit_price, :quantity)
        """,
        {
            "user_id": user_id,
            "share_token": share_token,
            "inventory_id": product["ID"],
            "report_id": product.get("REPORT_ID"),
            "product_name": product["PRODUCT_NAME"],
            "category": product["CATEGORY"],
            "unit_price": product["PRICE"],
            "quantity": quantity,
        },
    )[:2]


def clear_user_cart(user_id):
    return safe_execute("DELETE FROM cart_items WHERE user_id = :user_id", {"user_id": user_id})


def checkout_cart_for_user(user_id, username):
    items = get_cart_items()
    if not items:
        return False, "Your cart is empty.", None
    share_token = items[0].get("SHARE_TOKEN")
    report_id = items[0].get("REPORT_ID")
    total_amount = 0
    for item in items:
        inventory = fetch_one(
            """
            SELECT id, report_id, quantity, price, product_name, category
            FROM inventory_data
            WHERE id = :inventory_id AND report_id = :report_id
            """,
            {"inventory_id": item["INVENTORY_ID"], "report_id": item.get("REPORT_ID")},
        )
        if not inventory:
            return False, f"{item['PRODUCT_NAME']} is no longer available in this catalog.", None
        requested = int(item["QUANTITY"] or 0)
        available = int(inventory["QUANTITY"] or 0)
        if requested > available:
            return False, f"Stock changed for {inventory['PRODUCT_NAME']}. Only {available} units remain.", None
        total_amount += float(inventory["PRICE"] or 0) * requested

    success, message, _ = safe_execute(
        """
        INSERT INTO orders (user_id, username, status, share_token, report_id, total_amount)
        VALUES (:user_id, :username, :status, :share_token, :report_id, :total_amount)
        """,
        {
            "user_id": user_id,
            "username": username,
            "status": "Confirmed",
            "share_token": share_token,
            "report_id": report_id,
            "total_amount": total_amount,
        },
    )
    if not success:
        return False, message, None
    order = fetch_one(
        """
        SELECT order_id
        FROM orders
        WHERE user_id = :user_id AND username = :username
        ORDER BY created_at DESC, order_id DESC
        FETCH FIRST 1 ROWS ONLY
        """,
        {"user_id": user_id, "username": username},
    )
    order_id = order["ORDER_ID"]
    for item in items:
        inventory = fetch_one(
            """
            SELECT id, quantity, price, product_name, category
            FROM inventory_data
            WHERE id = :inventory_id AND report_id = :report_id
            """,
            {"inventory_id": item["INVENTORY_ID"], "report_id": item.get("REPORT_ID")},
        )
        requested = int(item["QUANTITY"] or 0)
        safe_execute(
            "UPDATE inventory_data SET quantity = :quantity WHERE id = :inventory_id",
            {"quantity": int(inventory["QUANTITY"] or 0) - requested, "inventory_id": item["INVENTORY_ID"]},
        )
        safe_execute(
            """
            INSERT INTO order_items (order_id, inventory_id, product_name, category, unit_price, quantity, line_total)
            VALUES (:order_id, :inventory_id, :product_name, :category, :unit_price, :quantity, :line_total)
            """,
            {
                "order_id": order_id,
                "inventory_id": item["INVENTORY_ID"],
                "product_name": item["PRODUCT_NAME"],
                "category": item["CATEGORY"],
                "unit_price": float(item["UNIT_PRICE"] or 0),
                "quantity": requested,
                "line_total": float(item["UNIT_PRICE"] or 0) * requested,
            },
        )
    clear_user_cart(user_id)
    log_activity("Checkout completed", f"Order #{order_id} placed for Rs. {total_amount:.2f}")
    return True, f"Order #{order_id} confirmed successfully.", order_id


def repair_uncategorized_inventory_categories():
    uncategorized_rows = fetch_all(
        """
        SELECT id, product_name
        FROM inventory_data
        WHERE LOWER(category) = LOWER(:category)
        """,
        {"category": "Uncategorized"},
    )
    repaired = 0
    for row in uncategorized_rows:
        product = fetch_one(
            """
            SELECT category
            FROM products
            WHERE LOWER(name) = LOWER(:product_name)
              AND category IS NOT NULL
              AND LOWER(category) != LOWER(:category)
            ORDER BY product_id DESC
            FETCH FIRST 1 ROWS ONLY
            """,
            {"product_name": row["PRODUCT_NAME"], "category": "Uncategorized"},
        )
        if not product:
            continue
        success, _message, _ = safe_execute(
            """
            UPDATE inventory_data
            SET category = :category
            WHERE id = :row_id
            """,
            {"category": clean_category_value(product["CATEGORY"]), "row_id": row["ID"]},
        )
        if success:
            repaired += 1
    return repaired


def clean_invalid_category_rows():
    invalid_rows = fetch_all(
        """
        SELECT id, product_name, category
        FROM inventory_data
        WHERE category IS NULL
           OR TRIM(category) = ''
           OR LOWER(TRIM(category)) = LOWER(:category)
        """,
        {"category": "Uncategorized"},
    )
    deleted = 0
    for row in invalid_rows:
        product_name = row.get("PRODUCT_NAME", "")
        if is_real_business_name(product_name):
            continue
        success, _message, _ = safe_execute("DELETE FROM inventory_data WHERE id = :row_id", {"row_id": row["ID"]})
        if success:
            deleted += 1
    return deleted


def reset_uploaded_inventory_data():
    safe_execute("DELETE FROM order_items")
    safe_execute("DELETE FROM orders")
    safe_execute("DELETE FROM cart_items")
    safe_execute("DELETE FROM user_orders")
    safe_execute("DELETE FROM shared_catalogs")
    safe_execute("DELETE FROM uploaded_reports")
    safe_execute("DELETE FROM sales")
    safe_execute("DELETE FROM products")
    success, message, count = safe_execute("DELETE FROM inventory_data")
    return success, message, count


def authenticate_from_database(username, password):
    try:
        print(f"[AUTH] Checking database user: {username}")
        user, db_ok = try_get_user_by_username(username)
        if not db_ok:
            print(f"[AUTH] Database lookup failed for user: {username}")
            return None
        if not user:
            print(f"[AUTH] No database user found: {username}")
            return None
        password_ok = check_password_hash(user["PASSWORD"], password)
        print(f"[AUTH] Password hash validation for {username}: {'passed' if password_ok else 'failed'}")
        if not password_ok:
            return None
        return {
            "user_id": user["USER_ID"],
            "username": user["USERNAME"],
            "role": user["ROLE"],
        }
    except Exception as error:
        print(f"[AUTH] Database authentication error for {username}: {error}")
        return None


def authenticate_from_demo_users(username, password):
    demo_user = DEMO_USERS.get(username.lower())
    if not demo_user:
        return None
    if demo_user["password"] != password:
        return None
    return {
        "user_id": None,
        "username": username,
        "role": demo_user["role"],
    }


def authenticate_user(username, password):
    if username.lower() in DEMO_USERS:
        demo_auth = authenticate_from_demo_users(username, password)
        if demo_auth:
            print(f"[AUTH] Logged in using built-in account: {username}")
            return demo_auth

    database_auth = authenticate_from_database(username, password)
    if database_auth:
        print(f"[AUTH] Logged in using database account: {username}")
        return database_auth

    demo_auth = authenticate_from_demo_users(username, password)
    if demo_auth:
        print(f"[AUTH] Fallback demo login succeeded for: {username}")
    return demo_auth


def normalize_auth_view(raw_value):
    value = str(raw_value or "main").strip().lower()
    return value if value in {"main", "admin", "manager", "user"} else "main"


def normalize_auth_mode(raw_value, auth_view):
    value = str(raw_value or "login").strip().lower()
    if auth_view == "manager":
        return "login"
    if auth_view == "main":
        return "select"
    return value if value in {"login", "register"} else "login"


@app.route("/")
def index():
    rows = get_active_rows() if session.get("role") else []
    metrics = build_dashboard_metrics(rows)
    return render_template("index.html", metrics=metrics, categories=CATEGORIES)


@app.route("/contact")
def contact():
    return render_template("contact.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    auth_view = normalize_auth_view(request.args.get("access", "main"))
    auth_mode = normalize_auth_mode(request.args.get("mode", "login"), auth_view)
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        if not username or not password:
            flash("Invalid username or password", "error")
            return render_template("login.html", auth_view=auth_view, auth_mode=auth_mode)

        authenticated = authenticate_user(username, password)
        if not authenticated:
            flash("Invalid username or password", "error")
            return render_template("login.html", auth_view=auth_view, auth_mode=auth_mode)

        set_user_session(authenticated["user_id"], authenticated["username"], authenticated["role"])
        session.pop("guest_inventory_rows", None)
        flash(f"Logged in as {authenticated['role'].title()}.", "success")
        return redirect_for_role(authenticated["role"])

    return render_template("login.html", auth_view=auth_view, auth_mode=auth_mode)


@app.route("/register", methods=["GET", "POST"])
def register():
    auth_view = normalize_auth_view(request.args.get("access", "user"))
    if request.method == "POST":
        email = request.form.get("email", "").strip()
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        requested_role = request.form.get("role_intent", "user").strip().lower()
        target_role = "user"
        print(f"[AUTH] Registration attempt for username: {username}")
        if not username or not password:
            flash("Username and password are required.", "error")
            return render_template("login.html", auth_view=auth_view, auth_mode="register")

        if get_user_by_username(username):
            print(f"[AUTH] Registration blocked, duplicate username: {username}")
            flash("That username already exists. Please choose another one.", "error")
            return render_template("login.html", auth_view=auth_view, auth_mode="register")

        if requested_role == "admin":
            expected_code = os.getenv("ADMIN_REGISTRATION_CODE", "").strip()
            provided_code = request.form.get("admin_access_code", "").strip()
            generated_code = validate_generated_admin_code(provided_code, email)
            env_code_valid = bool(expected_code and provided_code == expected_code)
            if not env_code_valid and not generated_code:
                flash("A valid admin access code is required to create a new admin account.", "error")
                return render_template("login.html", auth_view="admin", auth_mode="register")
            target_role = "admin"

        success, message, _ = create_user(username, generate_password_hash(password), target_role)
        print(f"[AUTH] Registration insert for {username}: {'succeeded' if success else 'failed'}")
        if not success:
            flash(message or "We could not create your account right now.", "error")
            return render_template("login.html", auth_view=auth_view, auth_mode="register")

        user = get_user_by_username(username)
        print(f"[AUTH] Registration fetch-after-insert for {username}: {'found' if user else 'missing'}")
        if user:
            set_user_session(user["USER_ID"], user["USERNAME"], user["ROLE"])
        else:
            flash("Your account was created, but login could not be completed automatically. Please sign in.", "warning")
            return redirect(url_for("login", access=auth_view, mode="login"))
        if requested_role == "admin" and not env_code_valid and generated_code:
            mark_generated_admin_code_used(generated_code["ID"])
            log_activity("Admin account created", f"Request-based admin signup completed for {username}")
        flash("Your account is ready. Welcome in.", "success")
        return redirect_for_role(user["ROLE"] if user else target_role)

    return redirect(url_for("login", access=auth_view, mode="register"))


@app.route("/request-admin-access", methods=["GET", "POST"])
def request_admin_access():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        email = request.form.get("email", "").strip()
        username = request.form.get("username", "").strip()
        reason = request.form.get("reason", "").strip()
        business_name = request.form.get("business_name", "").strip()
        proof = request.form.get("proof", "").strip()
        proof_file = request.files.get("proof_file")

        if not name or not email or not reason:
            flash("Name, email, and a reason are required so we can review your request.", "error")
            return render_template("request_admin_access.html")

        try:
            proof_file_path = save_admin_request_proof(proof_file)
        except ValueError as error:
            flash(str(error), "error")
            return render_template("request_admin_access.html")

        success, message, _ = safe_execute(
            """
            INSERT INTO admin_requests (name, email, username, reason, business_name, proof, proof_file_path, status)
            VALUES (:name, :email, :username, :reason, :business_name, :proof, :proof_file_path, :status)
            """,
            {
                "name": name,
                "email": email,
                "username": username or None,
                "reason": reason,
                "business_name": business_name or None,
                "proof": proof or None,
                "proof_file_path": proof_file_path or None,
                "status": "pending",
            },
        )
        if not success:
            flash(message or "We could not save your request right now.", "error")
            return render_template("request_admin_access.html")

        flash("Your admin access request was submitted. We’ll review it and share the code after approval.", "success")
        return redirect(url_for("login", access="admin", mode="register"))

    return render_template("request_admin_access.html")


@app.route("/guest-login")
def guest_login():
    set_user_session(None, "Guest", "guest")
    session["guest_inventory_rows"] = []
    flash("Guest access enabled. Your uploads will stay temporary for this session.", "success")
    return redirect_for_role("guest")


@app.route("/logout")
def logout():
    session.clear()
    flash("You have been logged out successfully.", "success")
    return redirect(url_for("login"))


@app.route("/dashboard")
@login_required
def dashboard():
    return redirect_for_role(session.get("role"))


@app.route("/admin-dashboard")
@login_required
@admin_required
def admin_dashboard():
    rows = get_active_rows()
    metrics = build_dashboard_metrics(rows)
    insights = build_insights(rows)
    decision_analytics = build_dataframe_analytics(rows)
    recent_rows = rows[:6]
    users = get_all_users()[:6]
    reports = get_recent_reports(6)
    latest_share = get_latest_shared_catalog()
    orders = build_order_views(8)
    latest_uploaded_report = get_latest_uploaded_report()
    admin_requests = get_admin_requests()
    return render_template(
        "admin_dashboard.html",
        metrics=metrics,
        insights=insights,
        decision_analytics=decision_analytics,
        recent_rows=recent_rows,
        users=users,
        reports=reports,
        total_reports=count_uploaded_reports(),
        latest_share=latest_share,
        shared_catalogs=get_shared_catalogs(5),
        orders=orders,
        latest_share_link=build_catalog_link(latest_share["TOKEN"]) if latest_share else "",
        latest_uploaded_report=latest_uploaded_report,
        recent_activity=get_recent_activity(8),
        admin_requests=admin_requests,
        smtp_configured=smtp_is_configured(),
        admin_code_expiry_hours=app.config["ADMIN_CODE_EXPIRY_HOURS"],
    )


@app.route("/admin-requests/<int:request_id>/approve", methods=["POST"])
@login_required
@admin_required
def approve_admin_request(request_id):
    request_row = get_admin_request_by_id(request_id)
    if not request_row:
        flash("That admin access request could not be found.", "error")
        return redirect(url_for("admin_dashboard") + "#admin-access-requests")

    if (request_row.get("STATUS") or "").lower() == "rejected":
        flash("Rejected requests cannot be approved without a new request.", "error")
        return redirect(url_for("admin_dashboard") + "#admin-access-requests")

    try:
        access_code, expires_at = create_admin_access_code(request_row)
    except RuntimeError as error:
        flash(str(error), "error")
        return redirect(url_for("admin_dashboard") + "#admin-access-requests")

    email_sent, delivery_message = send_admin_access_code_email(request_row["EMAIL"], access_code, expires_at)
    log_activity("Admin request approved", f"Admin request #{request_id} approved for {request_row['EMAIL']}")
    if email_sent:
        flash(f"Request approved. {delivery_message}", "success")
    else:
        flash(f"Request approved. {delivery_message} Share code {access_code} manually from the dashboard.", "warning")
    return redirect(url_for("admin_dashboard") + "#admin-access-requests")


@app.route("/admin-requests/<int:request_id>/reject", methods=["POST"])
@login_required
@admin_required
def reject_admin_request_route(request_id):
    request_row = get_admin_request_by_id(request_id)
    if not request_row:
        flash("That admin access request could not be found.", "error")
        return redirect(url_for("admin_dashboard") + "#admin-access-requests")

    reject_admin_request(request_id)
    log_activity("Admin request rejected", f"Admin request #{request_id} rejected for {request_row['EMAIL']}")
    flash("The admin access request was rejected.", "success")
    return redirect(url_for("admin_dashboard") + "#admin-access-requests")


@app.route("/manager-dashboard")
@login_required
@admin_or_manager_required
def manager_dashboard():
    if session.get("role") == "admin":
        return redirect(url_for("admin_dashboard"))
    rows = get_active_rows()
    metrics = build_dashboard_metrics(rows)
    insights = build_insights(rows)
    analytics_snapshot = build_business_analytics(filter_real_inventory_rows(rows), get_sales_rows_for_analytics())
    return render_template(
        "manager_dashboard.html",
        metrics=metrics,
        insights=insights,
        analytics=analytics_snapshot,
        recent_rows=rows[:8],
        orders=build_order_views(10),
        shared_catalogs=get_shared_catalogs(5),
        recent_activity=get_recent_activity(8),
        latest_uploaded_report=get_latest_uploaded_report(),
    )


@app.route("/user-dashboard")
@login_required
def user_dashboard():
    if session.get("role") == "admin":
        return redirect(url_for("admin_dashboard"))
    if session.get("role") == "manager":
        return redirect(url_for("manager_dashboard"))
    if session.get("role") == "guest":
        return redirect(url_for("guest_dashboard"))
    viewed_reports = session.get("recent_viewed_reports", [])
    latest_share = get_latest_shared_catalog()
    latest_share_products = []
    latest_share_report = None
    if latest_share:
        latest_share_report = resolve_catalog_report_record(dict(latest_share))
        report_id = latest_share_report["REPORT_ID"] if latest_share_report else latest_share.get("SOURCE_REPORT_ID")
        latest_share_products = get_catalog_products(report_id)[:8] if report_id else []
    return render_template(
        "user_dashboard.html",
        viewed_reports=viewed_reports,
        latest_share=latest_share,
        latest_share_report=latest_share_report,
        latest_share_products=latest_share_products,
        latest_share_link=build_catalog_link(latest_share["TOKEN"]) if latest_share else "",
        orders=build_order_views(8),
        cart=cart_summary(),
    )


@app.route("/guest-dashboard")
@login_required
def guest_dashboard():
    if session.get("role") == "admin":
        return redirect(url_for("admin_dashboard"))
    if session.get("role") == "manager":
        return redirect(url_for("manager_dashboard"))
    if session.get("role") == "user":
        return redirect(url_for("user_dashboard"))
    latest_share = get_latest_shared_catalog()
    latest_share_products = []
    latest_share_report = None
    if latest_share:
        latest_share_report = resolve_catalog_report_record(dict(latest_share))
        report_id = latest_share_report["REPORT_ID"] if latest_share_report else latest_share.get("SOURCE_REPORT_ID")
        latest_share_products = get_catalog_products(report_id)[:6] if report_id else []
    return render_template(
        "guest_dashboard.html",
        viewed_reports=session.get("recent_viewed_reports", []),
        latest_share=latest_share,
        latest_share_report=latest_share_report,
        latest_share_products=latest_share_products,
        latest_share_link=build_catalog_link(latest_share["TOKEN"]) if latest_share else "",
    )


@app.route("/products")
@login_required
@admin_required
def products():
    search = request.args.get("search", "").strip()
    category = request.args.get("category", "").strip()
    filters = []
    params = {}
    if search:
        filters.append("(LOWER(name) LIKE :search OR LOWER(category) LIKE :search)")
        params["search"] = f"%{search.lower()}%"
    if category:
        filters.append("category = :category")
        params["category"] = category
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    product_rows = fetch_all(
        f"""
        SELECT product_id, name, category, price, quantity, reorder_level, supplier_name, branch_name, image_url, created_at
        FROM products
        {where_clause}
        ORDER BY created_at DESC, product_id DESC
        """,
        params,
    )
    return render_template(
        "products.html",
        products=product_rows,
        search=search,
        selected_category=category,
        categories=CATEGORIES,
    )


@app.route("/manage-data")
@login_required
@admin_required
def manage_data():
    rows = get_active_rows()
    return render_template("manage_data.html", rows=rows[:100])


@app.route("/edit-data/<int:row_id>", methods=["GET", "POST"])
@login_required
@admin_required
def edit_data(row_id):
    row = fetch_one(
        """
        SELECT id, user_id, product_name, category, price, quantity, reorder_level, supplier_name, branch_name, image_url, entry_date, created_at
        FROM inventory_data
        WHERE id = :row_id
        """,
        {"row_id": row_id},
    )
    if not row:
        flash("Inventory record not found.", "error")
        return redirect(url_for("manage_data"))

    if request.method == "POST":
        try:
            params = {
                "row_id": row_id,
                "product_name": request.form.get("product_name", "").strip(),
                "category": category_for_storage(request.form.get("category")),
                "price": parse_decimal(request.form.get("price"), "Price"),
                "quantity": parse_int(request.form.get("quantity"), "Quantity"),
                "reorder_level": parse_int(request.form.get("reorder_level") or 10, "Reorder level"),
                "supplier_name": request.form.get("supplier_name", "").strip() or None,
                "branch_name": request.form.get("branch_name", "").strip() or None,
                "image_url": request.form.get("image_url", "").strip() or None,
                "entry_date": request.form.get("date", "").strip() or datetime.now().strftime("%Y-%m-%d"),
            }
        except ValueError as validation_error:
            flash(str(validation_error), "error")
            return render_template("edit_data.html", row=row, categories=CATEGORIES)

        success, message, _ = safe_execute(
            """
            UPDATE inventory_data
            SET product_name = :product_name,
                category = :category,
                price = :price,
                quantity = :quantity,
                reorder_level = :reorder_level,
                supplier_name = :supplier_name,
                branch_name = :branch_name,
                image_url = :image_url,
                entry_date = TO_DATE(:entry_date, 'YYYY-MM-DD')
            WHERE id = :row_id
            """,
            params,
        )
        flash("Inventory record updated." if success else message, "success" if success else "error")
        return redirect(url_for("manage_data"))

    return render_template("edit_data.html", row=row, categories=CATEGORIES)


@app.route("/delete-data/<int:row_id>", methods=["POST"])
@login_required
@admin_required
def delete_data(row_id):
    success, message, _ = safe_execute("DELETE FROM inventory_data WHERE id = :row_id", {"row_id": row_id})
    flash("Inventory record deleted." if success else message, "success" if success else "error")
    return redirect(url_for("manage_data"))


@app.route("/repair-categories", methods=["POST"])
@login_required
@admin_required
def repair_categories():
    repaired = repair_uncategorized_inventory_categories()
    if repaired:
        flash(f"Repaired {repaired} Uncategorized inventory records using matching product categories.", "success")
    else:
        flash("No repairable Uncategorized records were found. Future uploads will keep mapped categories correctly.", "warning")
    return redirect(url_for("manage_data"))


@app.route("/clean-invalid-categories", methods=["POST"])
@login_required
@admin_required
def clean_invalid_categories():
    deleted = clean_invalid_category_rows()
    if deleted:
        flash(f"Removed {deleted} old demo/test rows with invalid categories.", "success")
    else:
        flash("No old demo/test invalid-category rows were found.", "warning")
    return redirect(url_for("manage_data"))


@app.route("/reset-uploaded-inventory", methods=["POST"])
@login_required
@admin_required
def reset_uploaded_inventory():
    success, message, count = reset_uploaded_inventory_data()
    if success:
        flash(f"Reset uploaded inventory data successfully. Removed {count} inventory rows.", "success")
    else:
        flash(message, "error")
    return redirect(url_for("manage_data"))


@app.route("/sales")
@login_required
def sales():
    return redirect(url_for("analytics"))


@app.route("/upload", methods=["GET", "POST"])
@login_required
@admin_required
def upload():
    if request.method == "POST":
        mode = request.form.get("upload_mode", "").strip()
        rows = []

        if mode == "manual":
            try:
                rows = normalize_upload_rows(
                    [
                        {
                            "product_name": request.form.get("product_name", ""),
                            "category": request.form.get("category", ""),
                            "price": parse_decimal(request.form.get("price"), "Price"),
                            "quantity": parse_int(request.form.get("quantity"), "Quantity"),
                            "reorder_level": parse_int(request.form.get("reorder_level") or 10, "Reorder level"),
                            "supplier_name": request.form.get("supplier_name", ""),
                            "branch_name": request.form.get("branch_name", ""),
                            "image_url": request.form.get("image_url", ""),
                            "date": request.form.get("date", "").strip() or datetime.now().strftime("%Y-%m-%d"),
                        }
                    ]
                )
            except ValueError as validation_error:
                flash(str(validation_error), "error")
                return render_template("upload.html", categories=CATEGORIES)
        else:
            upload_file = request.files.get("excel_file")
            if not upload_file or not upload_file.filename:
                flash("Please choose an Excel file to upload.", "error")
                return render_template("upload.html", categories=CATEGORIES)
            if not allowed_file(upload_file.filename, ALLOWED_EXCEL_EXTENSIONS):
                flash("Wrong file type. Please upload a .xlsx file.", "error")
                return render_template("upload.html", categories=CATEGORIES)

            filename = f"{uuid4().hex}_{secure_filename(upload_file.filename)}"
            file_path = UPLOAD_DIR / filename
            upload_file.save(file_path)
            result = parse_excel_upload(str(file_path))
            if result["status"] == "mapping_required":
                session["pending_excel_file"] = str(file_path)
                session["pending_excel_filename"] = secure_filename(upload_file.filename)
                session["pending_excel_headers"] = result["uploaded_headers"]
                session["pending_excel_missing"] = result["missing_fields"]
                session["pending_excel_auto_mapping"] = result["auto_mapping"]
                session["pending_excel_metadata"] = result.get("metadata", {})
                flash(
                    "We found your Excel headers, but a few fields need manual mapping before import.",
                    "warning",
                )
                return redirect(url_for("map_upload_columns"))
            if result["status"] != "success":
                report = make_upload_report(upload_file.filename, "failed", result["message"], [], result.get("metadata", {}))
                flash(result["message"], "error")
                return render_template("upload_result.html", report=report)
            rows = result["rows"]
            for warning in result.get("metadata", {}).get("warnings", []):
                flash(warning, "warning")
            upload_filename = upload_file.filename

        if not rows:
            report = make_upload_report(request.form.get("product_name", "Manual entry"), "failed", "No valid inventory rows were found.", [])
            flash("No valid inventory rows were found.", "error")
            return render_template("upload_result.html", report=report)

        report_batch = create_uploaded_report_batch(
            locals().get("upload_filename", request.form.get("product_name", "Manual entry")),
            len(rows),
        )
        success, message = save_uploaded_rows(rows, report_batch)
        if not success:
            report = make_upload_report(locals().get("upload_filename", "Manual entry"), "failed", message, rows, report_batch=report_batch)
            flash(message, "error")
            return render_template("upload_result.html", report=report)
        make_upload_report(locals().get("upload_filename", "Manual entry"), "success", message, rows, locals().get("result", {}).get("metadata", {}), report_batch)
        log_activity("Inventory uploaded", f"{len(rows)} rows saved in report {report_batch['report_id']}")
        flash(message, "success")

        return redirect(url_for("upload_result"))

    return render_template("upload.html", categories=CATEGORIES)


@app.route("/upload/result")
@login_required
@admin_required
def upload_result():
    report = load_report_view(session.get("last_upload_report_id")) if session.get("last_upload_report_id") else None
    if not report:
        flash("No upload result is available yet. Please upload a file or add a manual entry.", "warning")
        return redirect(url_for("upload"))
    return render_template("upload_result.html", report=report)


@app.route("/generate-qr/<report_id>")
@login_required
@admin_required
def generate_report_qr(report_id):
    report = load_report_view(report_id)
    if not report:
        flash("Report not found for QR generation.", "error")
        return redirect(url_for("admin_dashboard"))

    catalog = create_or_refresh_shared_catalog(report_id, f"Catalog from {report.get('filename', 'upload')}")
    if not catalog:
        flash("We could not create shared catalog access right now.", "error")
        return redirect(url_for("report_view", report_id=report_id))

    token = catalog["TOKEN"]
    report["view_token"] = token
    view_url = build_catalog_link(token)
    qr_filename = build_url_qr(view_url)
    if not qr_filename:
        flash("We could not generate a QR code for this report right now.", "error")
        return redirect(url_for("report_view", report_id=report_id))

    safe_execute(
        "UPDATE shared_catalogs SET qr_filename = :qr_filename WHERE token = :token",
        {"qr_filename": qr_filename, "token": token},
    )
    report["qr_filename"] = qr_filename
    log_activity("QR generated", f"QR generated for report {report_id} with token {token}")
    flash("Report QR code generated successfully.", "success")
    return redirect(url_for("report_view", report_id=report_id))


@app.route("/generate-share/<report_id>")
@login_required
@admin_required
def generate_share(report_id):
    report = load_report_view(report_id)
    title = f"Catalog from {report.get('filename', 'latest upload')}" if report else "Shared Inventory Catalog"
    catalog = attach_qr_to_shared_catalog(create_or_refresh_shared_catalog(report_id, title))
    if not catalog:
        flash("We could not create a share link right now.", "error")
        return redirect(url_for("admin_dashboard"))
    log_activity("Share link generated", f"Share token {catalog['TOKEN']} generated for report {report_id}")
    flash("Share link generated successfully.", "success")
    return redirect(url_for("admin_dashboard") + "#share-access")


@app.route("/generate-catalog-share")
@login_required
@admin_required
def generate_catalog_share():
    latest_report = get_latest_uploaded_report()
    if not latest_report:
        flash("Upload a dataset first so the shared catalog is tied to a specific report.", "warning")
        return redirect(url_for("admin_dashboard"))
    catalog = attach_qr_to_shared_catalog(create_or_refresh_shared_catalog(latest_report["REPORT_ID"], latest_report["TITLE"]))
    if not catalog:
        flash("We could not create a share link right now.", "error")
        return redirect(url_for("admin_dashboard"))
    log_activity("Share link generated", f"Latest report {latest_report['REPORT_ID']} shared as token {catalog['TOKEN']}")
    flash("Share link generated for the latest uploaded dataset.", "success")
    return redirect(url_for("admin_dashboard") + "#share-access")


@app.route("/upload/map", methods=["GET", "POST"])
@login_required
@admin_required
def map_upload_columns():
    file_path = session.get("pending_excel_file")
    filename = session.get("pending_excel_filename", "Mapped Excel file")
    uploaded_headers = session.get("pending_excel_headers", [])
    missing_fields = session.get("pending_excel_missing", [])
    auto_mapping = session.get("pending_excel_auto_mapping", {})
    pending_metadata = session.get("pending_excel_metadata", {})

    if not file_path or not uploaded_headers:
        flash("No pending Excel upload was found. Please upload the file again.", "error")
        return redirect(url_for("upload"))

    if request.method == "POST":
        mapping = {}
        for field in EXCEL_MAPPING_FIELDS:
            selected_column = request.form.get(field, "").strip()
            if selected_column:
                mapping[field] = selected_column

        result = parse_excel_upload_with_manual_mapping(file_path, mapping)
        success, message, rows = result[:3]
        metadata = result[3] if len(result) > 3 else pending_metadata
        if not success:
            flash(message, "error")
            return render_template(
                "map_upload.html",
                required_fields=EXCEL_MAPPING_FIELDS,
                required_core_fields=REQUIRED_EXCEL_FIELDS,
                uploaded_headers=uploaded_headers,
                missing_fields=missing_fields,
                auto_mapping=auto_mapping,
            )

        report_batch = create_uploaded_report_batch(filename, len(rows))
        save_success, save_message = save_uploaded_rows(rows, report_batch)
        if not save_success:
            report = make_upload_report(filename, "failed", save_message, rows, metadata, report_batch)
            flash(save_message, "error")
            return render_template("upload_result.html", report=report)
        make_upload_report(filename, "success", save_message, rows, metadata, report_batch)
        for warning in metadata.get("warnings", []):
            flash(warning, "warning")
        flash(save_message, "success")

        session.pop("pending_excel_file", None)
        session.pop("pending_excel_headers", None)
        session.pop("pending_excel_missing", None)
        session.pop("pending_excel_auto_mapping", None)
        session.pop("pending_excel_filename", None)
        session.pop("pending_excel_metadata", None)
        return redirect(url_for("upload_result"))

    return render_template(
        "map_upload.html",
        required_fields=EXCEL_MAPPING_FIELDS,
        required_core_fields=REQUIRED_EXCEL_FIELDS,
        uploaded_headers=uploaded_headers,
        missing_fields=missing_fields,
        auto_mapping=auto_mapping,
    )


@app.route("/products/<int:product_id>/qr")
@login_required
@admin_required
def generate_product_qr(product_id):
    product = fetch_one(
        """
        SELECT product_id, name, category, price, quantity, created_at
        FROM products
        WHERE product_id = :product_id
        """,
        {"product_id": product_id},
    )
    if not product:
        flash("Product not found for QR generation.", "error")
        return redirect(url_for("products"))

    product_url = url_for("product_detail", product_id=product["PRODUCT_ID"], _external=True)
    qr_filename = build_url_qr(product_url)
    if not qr_filename:
        flash("We could not generate the QR code right now.", "error")
        return redirect(url_for("products"))

    flash("QR code generated successfully.", "success")
    return redirect(url_for("product_detail", product_id=product_id, qr=qr_filename))


@app.route("/product/<int:product_id>")
@login_required
@admin_required
def product_detail(product_id):
    product = fetch_one(
        """
        SELECT product_id, name, category, price, quantity, reorder_level, supplier_name, branch_name, image_url, created_at
        FROM products
        WHERE product_id = :product_id
        """,
        {"product_id": product_id},
    )
    if not product:
        flash("Product not found.", "error")
        return redirect(url_for("products"))
    qr_filename = request.args.get("qr", "").strip()
    stock_status = "Low Stock" if int(product["QUANTITY"] or 0) < 10 else "Available"
    return render_template("product_detail.html", product=product, stock_status=stock_status, qr_filename=qr_filename)


@app.route("/scan", methods=["GET", "POST"])
@login_required
@admin_required
def scan():
    if request.method == "POST":
        scanned_value = request.form.get("scan_value", "").strip()
        match = re.search(r"/product/(\d+)", scanned_value)
        if match:
            return redirect(url_for("product_detail", product_id=int(match.group(1))))
        if scanned_value.isdigit():
            return redirect(url_for("product_detail", product_id=int(scanned_value)))
        parsed = decode_qr_payload(scanned_value)
        if parsed and parsed.get("url"):
            match = re.search(r"/product/(\d+)", parsed["url"])
            if match:
                return redirect(url_for("product_detail", product_id=int(match.group(1))))
        flash("Could not read a valid product QR value.", "error")
    return render_template("scan.html")


@app.route("/qr/lookup", methods=["POST"])
@login_required
@admin_required
def qr_lookup():
    qr_data = request.form.get("qr_data", "").strip()
    match = re.search(r"/product/(\d+)", qr_data)
    if match:
        return redirect(url_for("product_detail", product_id=int(match.group(1))))
    if qr_data.isdigit():
        return redirect(url_for("product_detail", product_id=int(qr_data)))
    parsed = decode_qr_payload(qr_data)
    if parsed and parsed.get("url"):
        match = re.search(r"/product/(\d+)", parsed["url"])
        if match:
            return redirect(url_for("product_detail", product_id=int(match.group(1))))
    if not parsed:
        flash("QR parse failure. Please use a valid QR payload.", "error")
        return redirect(url_for("analytics"))
    flash(f"QR loaded for {parsed.get('name', 'product')} in {parsed.get('category', 'category')}.", "success")
    return redirect(url_for("analytics"))


@app.route("/analytics")
@login_required
def analytics():
    active_report = resolve_active_analytics_report()
    active_report_id = active_report["REPORT_ID"] if active_report else None
    rows = get_report_inventory_rows(active_report_id, exclude_demo_names=True)
    sales_rows = (
        get_sales_rows_for_report(active_report_id)
        if active_report_id
        else []
    )
    print("Active report_id used for analytics:", active_report_id or "NONE")
    print("Rows fetched for analytics:", len(rows))
    business_analytics = build_business_analytics(rows, sales_rows)
    print("Low stock rows being used:", [item["product_name"] for item in business_analytics["low_stock"]])
    print("Top moving rows being used:", [item["product_name"] for item in business_analytics["fast_items"]])
    print("Slow moving rows being used:", [item["product_name"] for item in business_analytics["slow_items"]])
    return render_template(
        "analytics.html",
        analytics=business_analytics,
        chart_payload=json.dumps(business_analytics["charts"]),
        rows=rows[:8],
        active_report=active_report,
        report_options=get_uploaded_report_options(10) if session.get("role") in {"admin", "manager"} else [],
    )


@app.route("/report/<report_id>")
@login_required
@admin_required
def report_view(report_id):
    report = load_report_view(report_id)
    if not report or report.get("id") != report_id:
        flash("That report is not available in this session.", "warning")
        return redirect(url_for("admin_dashboard"))
    return render_template("upload_result.html", report=report, read_only=True)


@app.route("/view-data", methods=["POST"])
@login_required
def view_data_lookup():
    token = extract_token_from_input(request.form.get("qr_token", ""))
    if not token:
        flash("Please enter a report QR token or shared report link.", "error")
        return redirect(url_for("user_dashboard" if session.get("role") == "user" else "guest_dashboard"))
    return redirect(url_for("catalog", token=token))


@app.route("/view-data/<qr_token>")
@login_required
def view_data(qr_token):
    if get_shared_catalog_by_token(qr_token):
        return redirect(url_for("catalog", token=qr_token))
    uploaded_report = get_uploaded_report_by_token(qr_token)
    report = load_report_view(uploaded_report["REPORT_ID"]) if uploaded_report else None
    if not report:
        flash("This shared report link is invalid or has expired. Please ask the admin for a fresh QR code.", "error")
        return redirect_for_role(session.get("role"))

    viewed_reports = session.get("recent_viewed_reports", [])
    summary = {
        "id": report["id"],
        "filename": report["filename"],
        "created_at": report.get("created_at", "-"),
        "rows": report.get("valid_rows", 0),
    }
    viewed_reports = [item for item in viewed_reports if item.get("id") != report["id"]]
    viewed_reports.insert(0, summary)
    session["recent_viewed_reports"] = viewed_reports[:5]
    return render_template("upload_result.html", report=report, read_only=True, shared_view=True)


@app.route("/catalog/<token>")
@login_required
def catalog(token):
    print("Catalog token received:", token)
    catalog_record = get_shared_catalog_by_token(token)
    if not catalog_record:
        flash("This shared catalog link is invalid or expired. Please ask the admin for a fresh link.", "error")
        return redirect_for_role(session.get("role"))
    resolved_report = resolve_catalog_report_record(catalog_record)
    report_id = resolved_report["REPORT_ID"] if resolved_report else catalog_record.get("SOURCE_REPORT_ID")
    print("Resolved shared catalog title:", catalog_record.get("TITLE"))
    print("Report_id resolved from token:", report_id)
    products = get_catalog_products(report_id)
    print("Number of products found for report:", len(products))
    print("First 5 product names fetched:", [product.get("PRODUCT_NAME") for product in products[:5]])
    search = request.args.get("search", "").strip()
    category = request.args.get("category", "").strip()
    stock_level = request.args.get("stock_level", "").strip()
    branch_name = request.args.get("branch", "").strip()
    sort_by = request.args.get("sort", "").strip() or "newest"
    filters = build_catalog_filters(products)
    filtered_products = filter_catalog_products(products, search, category, stock_level, branch_name, sort_by)
    print("Row count fetched before filters:", len(products))
    print("Row count after filters:", len(filtered_products))
    viewed_reports = session.get("recent_viewed_reports", [])
    summary = {
        "id": catalog_record["TOKEN"],
        "filename": catalog_record["TITLE"],
        "created_at": catalog_record.get("CREATED_AT", "-"),
        "rows": len(products),
    }
    viewed_reports = [item for item in viewed_reports if item.get("id") != catalog_record["TOKEN"]]
    viewed_reports.insert(0, summary)
    session["recent_viewed_reports"] = viewed_reports[:5]
    session["last_catalog_token"] = token
    return render_template(
        "catalog.html",
        catalog=catalog_record,
        products=filtered_products,
        total_products=len(products),
        has_active_filters=bool(search or category or stock_level or branch_name),
        catalog_metrics=get_catalog_metrics(products),
        filters=filters,
        search=search,
        selected_category=category,
        selected_stock_level=stock_level,
        selected_branch=branch_name,
        selected_sort=sort_by,
        cart=cart_summary(),
        orders=build_order_views(6) if session.get("role") == "user" else [],
    )


@app.route("/cart/add/<int:product_id>", methods=["POST"])
@login_required
def add_to_cart(product_id):
    if session.get("role") != "user":
        flash("Only logged-in users can build a cart and checkout.", "error")
        return redirect_for_role(session.get("role"))

    token = request.form.get("share_token", "").strip()
    catalog_record = get_shared_catalog_by_token(token)
    if not catalog_record:
        flash("Shared catalog access is invalid or expired.", "error")
        return redirect(url_for("user_dashboard"))

    try:
        order_quantity = parse_int(request.form.get("order_quantity"), "Quantity")
    except ValueError as validation_error:
        flash(str(validation_error), "error")
        return redirect(url_for("catalog", token=token))
    if order_quantity <= 0:
        flash("Quantity must be at least 1.", "error")
        return redirect(url_for("catalog", token=token))

    product = fetch_one(
        """
        SELECT id, report_id, product_name, category, price, quantity, reorder_level, supplier_name, branch_name, image_url
        FROM inventory_data
        WHERE id = :product_id
          AND report_id = :report_id
        """,
        {"product_id": product_id, "report_id": catalog_record.get("SOURCE_REPORT_ID")},
    )
    if not product:
        flash("Product not found in the shared catalog.", "error")
        return redirect(url_for("catalog", token=token))
    if not is_real_business_name(product["PRODUCT_NAME"]):
        flash("This item is not available in the shared catalog.", "error")
        return redirect(url_for("catalog", token=token))

    available_quantity = int(product["QUANTITY"] or 0)
    if order_quantity > available_quantity:
        flash(f"Only {available_quantity} units are available for {product['PRODUCT_NAME']}.", "error")
        return redirect(url_for("catalog", token=token))

    success, message = add_product_to_cart(session.get("user_id"), product, order_quantity, token)
    flash(message if success else message, "success" if success else "error")
    if success:
        log_activity("Cart updated", f"Added {order_quantity} x {product['PRODUCT_NAME']} to cart")
    return redirect(url_for("catalog", token=token))


@app.route("/cart")
@login_required
def cart():
    if session.get("role") != "user":
        flash("Cart access is available only for user accounts.", "error")
        return redirect_for_role(session.get("role"))
    summary = cart_summary()
    latest_share = get_latest_shared_catalog()
    return render_template("cart.html", cart=summary, latest_share=latest_share)


@app.route("/cart/update/<int:cart_item_id>", methods=["POST"])
@login_required
def update_cart_item(cart_item_id):
    if session.get("role") != "user":
        flash("Only users can update cart quantities.", "error")
        return redirect_for_role(session.get("role"))
    try:
        quantity = parse_int(request.form.get("quantity"), "Quantity")
    except ValueError as validation_error:
        flash(str(validation_error), "error")
        return redirect(url_for("cart"))

    item = fetch_one(
        """
        SELECT cart_item_id, user_id, inventory_id, report_id, product_name
        FROM cart_items
        WHERE cart_item_id = :cart_item_id AND user_id = :user_id
        """,
        {"cart_item_id": cart_item_id, "user_id": session.get("user_id")},
    )
    if not item:
        flash("Cart item not found.", "error")
        return redirect(url_for("cart"))
    if quantity <= 0:
        safe_execute("DELETE FROM cart_items WHERE cart_item_id = :cart_item_id", {"cart_item_id": cart_item_id})
        flash("Item removed from cart.", "success")
        return redirect(url_for("cart"))

    inventory = fetch_one(
        """
        SELECT quantity
        FROM inventory_data
        WHERE id = :inventory_id AND report_id = :report_id
        """,
        {"inventory_id": item["INVENTORY_ID"], "report_id": item["REPORT_ID"]},
    )
    available = int(inventory["QUANTITY"] or 0) if inventory else 0
    if quantity > available:
        flash(f"Only {available} units are available for {item['PRODUCT_NAME']}.", "error")
        return redirect(url_for("cart"))

    safe_execute(
        "UPDATE cart_items SET quantity = :quantity WHERE cart_item_id = :cart_item_id",
        {"quantity": quantity, "cart_item_id": cart_item_id},
    )
    flash("Cart quantity updated.", "success")
    return redirect(url_for("cart"))


@app.route("/cart/remove/<int:cart_item_id>", methods=["POST"])
@login_required
def remove_cart_item(cart_item_id):
    if session.get("role") != "user":
        flash("Only users can change cart contents.", "error")
        return redirect_for_role(session.get("role"))
    safe_execute(
        "DELETE FROM cart_items WHERE cart_item_id = :cart_item_id AND user_id = :user_id",
        {"cart_item_id": cart_item_id, "user_id": session.get("user_id")},
    )
    flash("Item removed from cart.", "success")
    return redirect(url_for("cart"))


@app.route("/checkout", methods=["GET", "POST"])
@login_required
def checkout():
    if session.get("role") != "user":
        flash("Checkout is available only for user accounts.", "error")
        return redirect_for_role(session.get("role"))
    summary = cart_summary()
    if not summary["items"]:
        flash("Your cart is empty. Add products before checkout.", "warning")
        latest_share = get_latest_shared_catalog()
        if latest_share:
            return redirect(url_for("catalog", token=latest_share["TOKEN"]))
        return redirect(url_for("user_dashboard"))

    if request.method == "POST":
        success, message, order_id = checkout_cart_for_user(session.get("user_id"), session.get("username"))
        flash(message, "success" if success else "error")
        if success:
            return redirect(url_for("order_detail", order_id=order_id))
    return render_template("checkout.html", cart=summary)


@app.route("/orders")
@login_required
def orders():
    return render_template("orders.html", orders=build_order_views(50))


@app.route("/orders/<int:order_id>")
@login_required
def order_detail(order_id):
    order = fetch_one(
        """
        SELECT order_id, user_id, username, status, share_token, report_id, total_amount, created_at
        FROM orders
        WHERE order_id = :order_id
        """,
        {"order_id": order_id},
    )
    if not order:
        flash("Order not found.", "error")
        return redirect(url_for("orders"))
    if session.get("role") not in {"admin", "manager"} and order.get("USER_ID") != session.get("user_id"):
        flash("You do not have access to that order.", "error")
        return redirect(url_for("orders"))
    order_view = {**order, "ITEMS": get_order_items(order_id)}
    return render_template("orders.html", orders=[order_view], focus_order=order_view)


@app.route("/orders/<int:order_id>/status", methods=["POST"])
@login_required
@admin_or_manager_required
def update_order_status(order_id):
    allowed_statuses = {"Pending", "Confirmed", "Packed", "Shipped", "Delivered", "Cancelled"}
    status = request.form.get("status", "").strip().title()
    if status not in allowed_statuses:
        flash("Invalid order status selected.", "error")
        return redirect(url_for("orders"))
    success, message, _ = safe_execute(
        "UPDATE orders SET status = :status WHERE order_id = :order_id",
        {"status": status, "order_id": order_id},
    )
    if success:
        log_activity("Order status updated", f"Order #{order_id} moved to {status}")
    flash("Order status updated." if success else message, "success" if success else "error")
    return redirect(url_for("orders"))


@app.route("/api/product/<int:product_id>")
@login_required
def api_product(product_id):
    rows = get_active_rows()
    for row in rows:
        if int(row.get("ID", 0)) == product_id:
            return jsonify({"success": True, "product": row})
    return jsonify({"success": False, "message": "Product not found."}), 404


@app.route("/health")
def health():
    # NEW: Simple health endpoint for Render health checks.
    database_ok = bool(fetch_one("SELECT 1 AS ok"))
    return jsonify(
        {
            "status": "ok" if database_ok else "degraded",
            "database": "ok" if database_ok else "unavailable",
            "uploads_dir": str(UPLOAD_DIR),
            "qr_dir": str(QR_DIR),
        }
    ), (200 if database_ok else 503)


@app.errorhandler(413)
def file_too_large(_error):
    flash("That file is too large. Please upload a file under 10 MB.", "error")
    return redirect(request.referrer or url_for("dashboard"))


@app.errorhandler(404)
def page_not_found(_error):
    return render_template("404.html"), 404


@app.errorhandler(500)
def internal_error(_error):
    return render_template("500.html"), 500


initialize_app()


if __name__ == "__main__":
    # UPDATED: Local development runner. Production uses gunicorn main:app.
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "5000")),
        debug=os.getenv("FLASK_DEBUG", "false").lower() in {"1", "true", "yes"},
    )
