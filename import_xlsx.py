"""
import_xlsx.py
─────────────
Reads data.xlsx and creates leads (+ linked contacts) in AmoCRM.

The script auto-discovers custom field IDs by querying AmoCRM and matches
them to the Excel column names defined in COLUMNS.  After creation every
lead is placed in the pipeline / status configured via .env so that
sync_service.py immediately picks them up and writes them to Google Sheets.

Usage:
    python import_xlsx.py                   # uses data.xlsx in current dir
    python import_xlsx.py path/to/file.xlsx
    python import_xlsx.py --dry-run         # prints rows, does NOT call API

Environment variables (same .env as sync_service.py):
    AMO_SUBDOMAIN, AMO_CLIENT_ID, AMO_CLIENT_SECRET, AMO_REDIRECT_URI
    AMO_AUTH_CODE / AMO_ACCESS_TOKEN / AMO_REFRESH_TOKEN
    AMO_TOKEN_STORE   (default: .amo_tokens.json)
    PIPELINE_ID       – pipeline to file leads into (0 = default pipeline)
    TRIGGER_STATUS_ID – status used for newly created leads
    IMPORT_BATCH_SIZE – leads per API request  (default: 50)
    IMPORT_DELAY_SEC  – delay between batches in seconds (default: 0.5)
"""

import json
import os
import sys
import time
import threading
import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# Column definitions (must match sync_service.py COLUMNS)
# ─────────────────────────────────────────────────────────────────────────────
COLUMNS = [
    "ID",
    "Ф.И.О.",
    "Контактный номер",
    "Бюджет сделки",
    "Заказ №",
    "Продукт 1",
    "Количество 1",
    "Продукт 2",
    "Количество 2",
    "Группа",
    "Дата заказа",
    "Дата доставка",
    "Регион",
    "Адрес",
    "Тип продажи",
    "Продажа в рассрочку",
    "Код сотрудника",
    "Компания",
    "Воронка",
    "статус",
]

# These columns are handled specially – NOT sent as custom fields
SKIP_AS_CUSTOM = {"ID", "Ф.И.О.", "Контактный номер", "Бюджет сделки", "Воронка", "статус"}


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
AMO_SUBDOMAIN      = os.getenv("AMO_SUBDOMAIN", "").strip()
AMO_CLIENT_ID      = os.getenv("AMO_CLIENT_ID", "").strip()
AMO_CLIENT_SECRET  = os.getenv("AMO_CLIENT_SECRET", "").strip()
AMO_REDIRECT_URI   = (os.getenv("AMO_REDIRECT_URI") or os.getenv("AMO_REDIRECT_URL") or "").strip()
AMO_AUTH_CODE      = os.getenv("AMO_AUTH_CODE", "").strip()
PIPELINE_ID        = int(os.getenv("PIPELINE_ID", "0"))
TRIGGER_STATUS_ID  = int(os.getenv("TRIGGER_STATUS_ID", "0"))
BATCH_SIZE         = int(os.getenv("IMPORT_BATCH_SIZE", "50"))
DELAY_SEC          = float(os.getenv("IMPORT_DELAY_SEC", "0.5"))
TOKEN_STORE_PATH   = Path(os.getenv("AMO_TOKEN_STORE", ".amo_tokens.json"))
BASE_URL           = f"https://{AMO_SUBDOMAIN}.amocrm.ru"


# ─────────────────────────────────────────────────────────────────────────────
# Token helpers (mirrors sync_service.py)
# ─────────────────────────────────────────────────────────────────────────────
_token_lock = threading.Lock()


def _load_tokens() -> Dict[str, str]:
    with _token_lock:
        if TOKEN_STORE_PATH.exists():
            return json.loads(TOKEN_STORE_PATH.read_text(encoding="utf-8"))
        return {
            "access_token": os.getenv("AMO_ACCESS_TOKEN", ""),
            "refresh_token": os.getenv("AMO_REFRESH_TOKEN", ""),
        }


def _save_tokens(access_token: str, refresh_token: str) -> None:
    with _token_lock:
        TOKEN_STORE_PATH.write_text(
            json.dumps(
                {"access_token": access_token, "refresh_token": refresh_token},
                ensure_ascii=False, indent=2,
            ),
            encoding="utf-8",
        )


def _is_valid(access_token: str) -> bool:
    if not access_token:
        return False
    r = requests.get(
        f"{BASE_URL}/api/v4/account",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=20,
    )
    return r.status_code == 200


def _refresh(refresh_token: str) -> str:
    if not refresh_token:
        raise RuntimeError("No refresh_token. Run sync_service.py first to complete OAuth.")
    r = requests.post(
        f"{BASE_URL}/oauth2/access_token",
        json={
            "client_id": AMO_CLIENT_ID,
            "client_secret": AMO_CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "redirect_uri": AMO_REDIRECT_URI,
        },
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Token refresh failed: {r.status_code} {r.text}")
    data = r.json()
    _save_tokens(data["access_token"], data["refresh_token"])
    return data["access_token"]


def _exchange_code(code_or_url: str) -> str:
    value = code_or_url.strip()
    if "code=" in value:
        code = value.split("code=")[1].split("&")[0]
    else:
        parsed = urlparse(value)
        code = parse_qs(parsed.query).get("code", [""])[0] if parsed.query else value

    r = requests.post(
        f"{BASE_URL}/oauth2/access_token",
        json={
            "client_id": AMO_CLIENT_ID,
            "client_secret": AMO_CLIENT_SECRET,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": AMO_REDIRECT_URI,
        },
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"OAuth exchange failed: {r.status_code} {r.text}")
    data = r.json()
    _save_tokens(data["access_token"], data["refresh_token"])
    return data["access_token"]


def get_access_token() -> str:
    tokens = _load_tokens()
    at, rt = tokens.get("access_token", ""), tokens.get("refresh_token", "")
    if _is_valid(at):
        return at
    if not rt and AMO_AUTH_CODE:
        print("[INFO] Bootstrapping token from AMO_AUTH_CODE …")
        return _exchange_code(AMO_AUTH_CODE)
    return _refresh(rt)


# ─────────────────────────────────────────────────────────────────────────────
# Generic API helpers
# ─────────────────────────────────────────────────────────────────────────────
def _headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {get_access_token()}", "Content-Type": "application/json"}


def api_get(endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
    r = requests.get(f"{BASE_URL}{endpoint}", headers=_headers(), params=params, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"GET {endpoint} → {r.status_code}: {r.text[:400]}")
    return r.json()


def api_post(endpoint: str, body: Any) -> Dict[str, Any]:
    r = requests.post(f"{BASE_URL}{endpoint}", headers=_headers(), json=body, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"POST {endpoint} → {r.status_code}: {r.text[:600]}")
    return r.json() if r.text else {}


# ─────────────────────────────────────────────────────────────────────────────
# Discover custom field IDs from AmoCRM
# ─────────────────────────────────────────────────────────────────────────────
def discover_custom_fields(entity: str = "leads") -> Dict[str, int]:
    """Returns {field_name_lower: field_id} for all custom fields of entity."""
    mapping: Dict[str, int] = {}
    page = 1
    while True:
        try:
            data = api_get(f"/api/v4/{entity}/custom_fields", params={"limit": 250, "page": page})
        except Exception as exc:
            print(f"[WARN] Could not fetch custom fields (page {page}): {exc}")
            break
        items = data.get("_embedded", {}).get("custom_fields", [])
        if not items:
            break
        for cf in items:
            name = str(cf.get("name", "")).strip()
            # Normalize spaces to match COLUMNS
            name = " ".join(name.split())
            fid  = int(cf.get("id", 0) or 0)
            if name and fid:
                mapping[name.lower()] = fid
        if len(items) < 250:
            break
        page += 1
    return mapping


def discover_contact_custom_fields() -> Dict[str, int]:
    return discover_custom_fields("contacts")


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline / status resolution
# ─────────────────────────────────────────────────────────────────────────────
def resolve_pipeline_status() -> Tuple[int, int]:
    """
    Returns (pipeline_id, status_id) to use for newly created leads.
    Uses PIPELINE_ID and TRIGGER_STATUS_ID from env when set,
    otherwise falls back to the first available pipeline/status.
    """
    if PIPELINE_ID and TRIGGER_STATUS_ID:
        return PIPELINE_ID, TRIGGER_STATUS_ID

    try:
        data = api_get("/api/v4/leads/pipelines?with=statuses&limit=50")
        pipelines = data.get("_embedded", {}).get("pipelines", [])
    except Exception as exc:
        print(f"[WARN] Could not load pipelines: {exc}")
        return PIPELINE_ID, TRIGGER_STATUS_ID

    for pipeline in pipelines:
        pid = int(pipeline.get("id", 0) or 0)
        if PIPELINE_ID and pid != PIPELINE_ID:
            continue
        statuses = pipeline.get("_embedded", {}).get("statuses", [])
        for status in statuses:
            sid = int(status.get("id", 0) or 0)
            if TRIGGER_STATUS_ID and sid != TRIGGER_STATUS_ID:
                continue
            print(f"[INFO] Will create leads in pipeline '{pipeline.get('name')}' "
                  f"(id={pid}), status '{status.get('name')}' (id={sid})")
            return pid, sid

    # nothing matched – return whatever is in env
    return PIPELINE_ID, TRIGGER_STATUS_ID


# ─────────────────────────────────────────────────────────────────────────────
# Build AmoCRM lead payload from one Excel row
# ─────────────────────────────────────────────────────────────────────────────
def _str(val: Any) -> str:
    if pd.isna(val):
        return ""
    return str(val).strip()


def build_lead_payload(
    row: pd.Series,
    lead_cf_map: Dict[str, int],
    pipeline_id: int,
    status_id: int,
) -> Dict[str, Any]:
    name   = _str(row.get("Ф.И.О.", "")) or "—"
    price  = row.get("Бюджет сделки", 0)
    try:
        price = int(float(str(price).replace(" ", "").replace(",", "."))) if not pd.isna(price) else 0
    except (ValueError, TypeError):
        price = 0

    custom_fields_values: List[Dict] = []
    for col in COLUMNS:
        if col in SKIP_AS_CUSTOM:
            continue
        val = _str(row.get(col, ""))
        if not val:
            continue
        cf_id = lead_cf_map.get(col.lower())
        if cf_id:
            # Convert dates to Unix timestamps
            if col in ("Дата заказа", "Дата доставка"):
                try:
                    # Try parsing common date formats
                    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
                        try:
                            dt = datetime.datetime.strptime(val, fmt)
                            val = int(dt.timestamp())
                            break
                        except ValueError:
                            pass
                except Exception:
                    pass
            
            # Handle multiselect fields (comma-separated values)
            if col in ("Продукт 1", "Продукт 2"):
                values_list = [{"value": v.strip()} for v in str(val).split(",") if v.strip()]
                custom_fields_values.append({
                    "field_id": cf_id,
                    "values": values_list,
                })
            else:
                custom_fields_values.append({
                    "field_id": cf_id,
                    "values": [{"value": val}],
                })
        else:
            # field not found in AmoCRM – store as note later or skip
            pass

    payload: Dict[str, Any] = {
        "name": name,
        "price": price,
        "_embedded": {},
    }
    if pipeline_id:
        payload["pipeline_id"] = pipeline_id
    if status_id:
        payload["status_id"] = status_id
    if custom_fields_values:
        payload["custom_fields_values"] = custom_fields_values

    return payload


def build_contact_payload(row: pd.Series, contact_cf_map: Dict[str, int]) -> Optional[Dict[str, Any]]:
    name  = _str(row.get("Ф.И.О.", ""))
    phone = _str(row.get("Контактный номер", ""))

    if not name and not phone:
        return None

    custom_fields_values: List[Dict] = []
    if phone:
        # Standard AmoCRM phone field
        phone_cf_id = contact_cf_map.get("телефон") or contact_cf_map.get("phone")
        if phone_cf_id:
            custom_fields_values.append({
                "field_id": phone_cf_id,
                "values": [{"value": phone, "enum_code": "WORK"}],
            })
        else:
            # Fallback: use field_code
            custom_fields_values.append({
                "field_code": "PHONE",
                "values": [{"value": phone, "enum_code": "WORK"}],
            })

    contact: Dict[str, Any] = {"name": name or phone}
    if custom_fields_values:
        contact["custom_fields_values"] = custom_fields_values

    return contact


# ─────────────────────────────────────────────────────────────────────────────
# Create contacts in batch, return {name+phone: contact_id}
# ─────────────────────────────────────────────────────────────────────────────
def create_contacts_batch(payloads: List[Dict]) -> List[int]:
    if not payloads:
        return []
    result = api_post("/api/v4/contacts", payloads)
    items = result.get("_embedded", {}).get("contacts", [])
    return [int(item["id"]) for item in items]


# ─────────────────────────────────────────────────────────────────────────────
# Create leads in batch
# ─────────────────────────────────────────────────────────────────────────────
def create_leads_batch(payloads: List[Dict]) -> List[int]:
    if not payloads:
        return []
    result = api_post("/api/v4/leads/complex", payloads)
    # /leads/complex returns array directly or wrapped
    if isinstance(result, list):
        items = result
    else:
        items = result.get("_embedded", {}).get("leads", [])
    return [int(item["id"]) for item in items]


# ─────────────────────────────────────────────────────────────────────────────
# Main import logic
# ─────────────────────────────────────────────────────────────────────────────
def import_xlsx(xlsx_path: str, dry_run: bool = False) -> None:
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Importing: {xlsx_path}")

    # 1. Read Excel
    df = pd.read_excel(xlsx_path, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    print(f"[INFO] Loaded {len(df)} rows, columns: {list(df.columns)}")

    # Rename columns to match COLUMNS if needed (flexible match)
    col_remap: Dict[str, str] = {}
    lower_cols = {c.lower(): c for c in COLUMNS}
    for xlsx_col in df.columns:
        matched = lower_cols.get(xlsx_col.lower())
        if matched and matched != xlsx_col:
            col_remap[xlsx_col] = matched
    if col_remap:
        df.rename(columns=col_remap, inplace=True)
        print(f"[INFO] Column remapped: {col_remap}")

    if dry_run:
        print("\n── Sample row (first 3) ──")
        print(df.head(3).to_string(index=False))
        print("\n[DRY RUN] No API calls made.")
        return

    # 2. Authenticate
    print("[INFO] Authenticating with AmoCRM …")
    token = get_access_token()
    print(f"[INFO] Token OK (len={len(token)})")

    # 3. Discover field IDs
    print("[INFO] Fetching lead custom fields …")
    lead_cf_map = discover_custom_fields("leads")
    print(f"[INFO] Found {len(lead_cf_map)} lead custom fields")

    print("[INFO] Fetching contact custom fields …")
    contact_cf_map = discover_contact_custom_fields()
    print(f"[INFO] Found {len(contact_cf_map)} contact custom fields")

    # Print unmapped columns so user knows what's missing
    unmapped = [
        col for col in COLUMNS
        if col not in SKIP_AS_CUSTOM and col in df.columns and col.lower() not in lead_cf_map
    ]
    if unmapped:
        print(f"[WARN] These columns have NO matching AmoCRM custom field and will be skipped: {unmapped}")
        print("[HINT] Create those fields in AmoCRM Settings → Custom fields, then re-run.")

    # 4. Resolve pipeline / status
    pipeline_id, status_id = resolve_pipeline_status()
    if not pipeline_id:
        print("[WARN] PIPELINE_ID not set – leads will be placed in the default pipeline.")
    if not status_id:
        print("[WARN] TRIGGER_STATUS_ID not set – leads will use pipeline's first status.")

    # 5. Process rows in batches
    total    = len(df)
    created  = 0
    failed   = 0

    for batch_start in range(0, total, BATCH_SIZE):
        batch = df.iloc[batch_start: batch_start + BATCH_SIZE]
        complex_payloads: List[Dict] = []

        for _, row in batch.iterrows():
            lead_payload = build_lead_payload(row, lead_cf_map, pipeline_id, status_id)
            contact_payload = build_contact_payload(row, contact_cf_map)

            # /leads/complex accepts lead + embedded contacts in one call
            complex_entry: Dict[str, Any] = {**lead_payload}
            if contact_payload:
                complex_entry["_embedded"] = {
                    "contacts": [contact_payload],
                }
            if "Компания" in df.columns:
                company = _str(row.get("Компания", ""))
                if company:
                    complex_entry.setdefault("_embedded", {})
                    complex_entry["_embedded"]["companies"] = [{"name": company}]

            complex_payloads.append(complex_entry)

        batch_end = min(batch_start + BATCH_SIZE, total)
        print(f"[INFO] Sending batch rows {batch_start + 1}–{batch_end} ({len(complex_payloads)} leads) …")
        try:
            ids = create_leads_batch(complex_payloads)
            created += len(ids)
            print(f"[OK]   Batch created {len(ids)} leads. IDs: {ids[:5]}{'…' if len(ids) > 5 else ''}")
        except Exception as exc:
            failed += len(complex_payloads)
            print(f"[ERR]  Batch failed: {exc}")

        if batch_end < total:
            time.sleep(DELAY_SEC)

    print(f"\n── Import complete ──")
    print(f"   Total rows : {total}")
    print(f"   Created    : {created}")
    print(f"   Failed     : {failed}")
    if created:
        print(f"\n[NEXT] Once sync_service.py is running it will detect these leads via webhook")
        print(f"       or you can trigger a manual snapshot via GET /structure on the service.")


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if a]
    dry_run = "--dry-run" in args
    args = [a for a in args if a != "--dry-run"]

    xlsx_file = args[0] if args else "data.xlsx"

    if not Path(xlsx_file).exists():
        print(f"[ERROR] File not found: {xlsx_file}")
        print("Usage: python import_xlsx.py [path/to/file.xlsx] [--dry-run]")
        sys.exit(1)

    import_xlsx(xlsx_file, dry_run=dry_run)
