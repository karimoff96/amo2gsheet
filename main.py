"""
Two-way sync prototype:
- Poll amoCRM for leads in allowed statuses, append to Google Sheets (one row per lead).
- Watch the sheet for user-edited status changes and push them back to amoCRM.

Placeholders are grouped in the config section for easy replacement with real values.
Sheet column names mirror amoCRM fields to reduce mapping friction.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List

import gspread
from amocrm.v2 import Lead, tokens
from dotenv import load_dotenv

# -------------------------
# Configuration (placeholders)
# -------------------------


@dataclass
class StatusMapping:
    status_id: int
    status_name: str = ""


@dataclass
class AmoConfig:
    client_id: str
    client_secret: str
    subdomain: str
    redirect_url: str
    auth_code: str
    target_pipeline_id: int
    allowed_statuses: List[StatusMapping]


@dataclass
class SheetsConfig:
    service_account_json: str
    spreadsheet_id: str
    worksheet_name: str
    status_column: str = "status_id"  # column that users will edit to push status


@dataclass
class AppConfig:
    amo: AmoConfig
    sheets: SheetsConfig
    poll_seconds: int
    state_file: Path


def _load_int(name: str, default: int = 0) -> int:
    try:
        return int(os.getenv(name, default))
    except Exception:
        return default


def _load_statuses() -> List[StatusMapping]:
    raw = os.getenv("AMO_ALLOWED_STATUS_IDS", "")
    ids = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.append(int(part))
        except Exception:
            continue
    if not ids:
        ids = [0]
    return [StatusMapping(status_id=i) for i in ids]


load_dotenv()

config = AppConfig(
    amo=AmoConfig(
        client_id=os.getenv("AMO_CLIENT_ID", ""),
        client_secret=os.getenv("AMO_CLIENT_SECRET", ""),
        subdomain=os.getenv("AMO_SUBDOMAIN", ""),
        redirect_url=os.getenv("AMO_REDIRECT_URL", ""),
        auth_code=os.getenv("AMO_AUTH_CODE", ""),
        target_pipeline_id=_load_int("AMO_PIPELINE_ID", 0),
        allowed_statuses=_load_statuses(),
    ),
    sheets=SheetsConfig(
        service_account_json=os.getenv("GS_SERVICE_ACCOUNT_JSON", "service-account.json"),
        spreadsheet_id=os.getenv("GS_SPREADSHEET_ID", ""),
        worksheet_name=os.getenv("GS_WORKSHEET_NAME", "Sheet1"),
    ),
    poll_seconds=_load_int("POLL_SECONDS", 300),
    state_file=Path(os.getenv("STATE_FILE", "state.json")),
)

# Column order for the sheet (mirrors amo field names)
SHEET_COLUMNS = [
    "id",
    "name",
    "price",
    "status_id",
    "status_name",
    "pipeline_id",
    "responsible_user_id",
    "contact_id",
    "contact_name",
    "contact_phone",
    "contact_email",
    "updated_at",
]


# -------------------------
# amoCRM setup
# -------------------------


def init_amo_auth(conf: AmoConfig) -> None:
    tokens.default_token_manager(
        client_id=conf.client_id,
        client_secret=conf.client_secret,
        subdomain=conf.subdomain,
        redirect_url=conf.redirect_url,
        storage=tokens.FileTokensStorage(),
    )
    tokens.default_token_manager.init(code=conf.auth_code, skip_error=True)


# -------------------------
# Google Sheets setup
# -------------------------


def init_sheet(conf: SheetsConfig):
    client = gspread.service_account(filename=conf.service_account_json)
    spreadsheet = client.open_by_key(conf.spreadsheet_id)
    sheet = spreadsheet.worksheet(conf.worksheet_name)
    ensure_header(sheet)
    return sheet


def ensure_header(sheet) -> None:
    header = sheet.row_values(1)
    if header != SHEET_COLUMNS:
        if header:
            sheet.delete_rows(1)
        sheet.insert_row(SHEET_COLUMNS, 1)


# -------------------------
# State helpers
# -------------------------


def load_state(path: Path) -> Dict:
    if path.exists():
        return json.loads(path.read_text())
    return {"last_checked": int(time.time()), "sheet_status_by_lead": {}}


def save_state(path: Path, state: Dict) -> None:
    path.write_text(json.dumps(state))


# -------------------------
# Lead fetching and formatting
# -------------------------


def fetch_leads_since(ts: int, conf: AmoConfig) -> Iterable[Lead]:
    status_ids = [s.status_id for s in conf.allowed_statuses]
    return Lead.objects.filter(
        updated_at__gt=ts,
        pipeline_id=conf.target_pipeline_id,
        status_id=status_ids,
    )


def pick_primary_contact(lead: Lead):
    contacts = getattr(lead, "contacts", None)
    if not contacts:
        return None
    if isinstance(contacts, list):
        return contacts[0]
    try:
        return contacts.first()
    except Exception:
        return None


def get_contact_field(contact, code: str) -> str:
    try:
        for field in contact.custom_fields_values:
            if field.get("code") == code:
                values = field.get("values", [])
                if values:
                    return str(values[0].get("value", ""))
    except Exception:
        return ""
    return ""


def lead_to_row(lead: Lead) -> List[str]:
    contact = pick_primary_contact(lead)
    phone = get_contact_field(contact, "PHONE") if contact else ""
    email = get_contact_field(contact, "EMAIL") if contact else ""
    updated_at = getattr(lead, "updated_at", None)
    updated_iso = (
        updated_at.isoformat() if hasattr(updated_at, "isoformat") else str(updated_at)
    )
    return [
        str(getattr(lead, "id", "")),
        str(getattr(lead, "name", "")),
        str(getattr(lead, "price", "")),
        str(getattr(lead, "status_id", "")),
        str(getattr(lead, "status_name", "")),
        str(getattr(lead, "pipeline_id", "")),
        str(getattr(lead, "responsible_user_id", "")),
        str(getattr(contact, "id", "")) if contact else "",
        str(getattr(contact, "name", "")) if contact else "",
        phone,
        email,
        updated_iso,
    ]


# -------------------------
# Sheet parsing and amo update
# -------------------------


def parse_sheet_records(sheet) -> List[Dict[str, str]]:
    return sheet.get_all_records()


def push_status_to_amo(lead_id: int, new_status_id: int, pipeline_id: int) -> None:
    lead = Lead.objects.get(id=lead_id)
    lead.status_id = new_status_id
    lead.pipeline_id = pipeline_id
    lead.save()


# -------------------------
# Main loop
# -------------------------


def main() -> None:
    init_amo_auth(config.amo)
    sheet = init_sheet(config.sheets)

    state = load_state(config.state_file)
    last_checked = int(state.get("last_checked", time.time()))
    sheet_status_by_lead: Dict[str, str] = state.get("sheet_status_by_lead", {})

    allowed_status_ids = {str(s.status_id) for s in config.amo.allowed_statuses}

    print(f"Starting poll from timestamp: {last_checked}")

    try:
        while True:
            # Pull from amo → sheet
            leads = list(fetch_leads_since(last_checked, config.amo))
            if leads:
                rows = [lead_to_row(lead) for lead in leads]
                sheet.append_rows(rows, value_input_option="USER_ENTERED")

                def to_epoch(val) -> int:
                    if hasattr(val, "timestamp"):
                        return int(val.timestamp())
                    try:
                        return int(val)
                    except Exception:
                        return int(time.time())

                max_ts = max(
                    to_epoch(getattr(lead, "updated_at", time.time())) for lead in leads
                )
                last_checked = max(last_checked, max_ts)
                for lead in leads:
                    sheet_status_by_lead[str(getattr(lead, "id", ""))] = str(
                        getattr(lead, "status_id", "")
                    )
                print(f"Appended {len(rows)} leads; new cursor: {last_checked}")
            else:
                print("No new leads in allowed statuses")

            # Push from sheet → amo (status only)
            records = parse_sheet_records(sheet)
            for row in records:
                lead_id = row.get("id")
                if not lead_id:
                    continue
                str_lead_id = str(lead_id)
                sheet_status = str(row.get(config.sheets.status_column, ""))
                last_synced = sheet_status_by_lead.get(str_lead_id)
                if not sheet_status or sheet_status == last_synced:
                    continue
                if sheet_status not in allowed_status_ids:
                    print(
                        f"Skip lead {str_lead_id}: status {sheet_status} not in allowed list"
                    )
                    continue
                try:
                    push_status_to_amo(
                        int(lead_id), int(sheet_status), config.amo.target_pipeline_id
                    )
                    sheet_status_by_lead[str_lead_id] = sheet_status
                    print(f"Updated amo lead {lead_id} -> status {sheet_status}")
                except Exception as exc:
                    print(f"Failed to update lead {lead_id}: {exc}")

            # persist state
            save_state(
                config.state_file,
                {
                    "last_checked": last_checked,
                    "sheet_status_by_lead": sheet_status_by_lead,
                },
            )
            time.sleep(config.poll_seconds)
    except KeyboardInterrupt:
        print("Stopped by user")


if __name__ == "__main__":
    main()
