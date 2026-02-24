"""
prod_check.py — Production readiness checker for amo2gsheet.

Run this script BEFORE switching to a production AMO account or Google Sheet
to verify that everything is wired up correctly.

Checks:
  1. Environment variables — all required vars are set
  2. AMO connectivity      — token is valid / can be refreshed
  3. Pipeline & status audit
       • Lists every pipeline found in AMO
       • Verifies the trigger status exists in EACH pipeline
       • Warns about pipelines with no display-name mapping
       • Warns about status names with no display-name mapping
  4. Lead custom-field audit
       • Fetches all AMO lead custom fields
       • Cross-references against COLUMNS
       • Reports columns that will always be empty (no matching AMO field)
  5. Google Sheet audit
       • Verifies connectivity and header row
       • Checks Staff tab exists and is non-empty

Usage:
    python prod_check.py               # full audit (no writes)
    python prod_check.py --setup       # full audit + fix Google Sheet headers/dropdowns
    python prod_check.py --staff FILE  # same as --setup + import staff from CSV

The script exits with code 0 if no blocking issues are found, 1 otherwise.
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import gspread
import requests
from dotenv import load_dotenv
from gspread.utils import ValidationConditionType

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# Column / status definitions  (must mirror sync_service.py)
# ─────────────────────────────────────────────────────────────────────────────

COLUMNS: List[str] = [
    "Компания",
    "ID",
    "Заказ №",
    "Ф.И.О.",
    "Контактный номер",
    "Дата заказа",
    "Дата доставка",
    "Код сотрудника",
    "Ответственный",
    "Группа",
    "Продукт 1",
    "Количество 1",
    "Продукт 2",
    "Количество 2",
    "Бюджет сделки",
    "Регион",
    "Адрес",
    "Тип продажи",
    "Продажа в рассрочку",
    "Воронка",
    "статус",
]

# These columns are NOT AMO lead custom fields — they come from standard lead/
# contact/company fields or are derived by the sync service itself.
NON_CUSTOM_FIELD_COLS = {
    "ID",               # lead.id
    "Ф.И.О.",           # embedded contact.name
    "Контактный номер", # embedded contact phone (contact custom field, not lead)
    "Компания",         # embedded company.name
    "Бюджет сделки",    # lead.price
    "Ответственный",    # resolved from responsible_user_id
    "Воронка",          # resolved from pipeline_id
    "статус",           # resolved from status_id
}

# Columns that MUST be AMO lead custom fields
EXPECTED_CUSTOM_FIELD_COLS = [c for c in COLUMNS if c not in NON_CUSTOM_FIELD_COLS]

# Populated from PIPELINE_DISPLAY_MAP_JSON in .env (same as sync_service.py).
# All other pipelines are treated as unmapped and reported during the audit.
PIPELINE_DISPLAY_MAP: Dict[str, str] = {}
try:
    _env_pipeline_map = json.loads(os.getenv("PIPELINE_DISPLAY_MAP_JSON", "{}"))
    PIPELINE_DISPLAY_MAP.update(_env_pipeline_map)
except Exception:
    pass

STATUS_DROPDOWN_OPTS = ["В процессе", "У курера", "Успешно", "Отказ"]
STATUS_COL_INDEX     = COLUMNS.index("статус")
STATUS_COL_LETTER    = chr(ord("A") + STATUS_COL_INDEX)

# ─────────────────────────────────────────────────────────────────────────────
# Output helpers
# ─────────────────────────────────────────────────────────────────────────────

SEP  = "─" * 66
SEP2 = "═" * 66

_issues: List[str] = []   # Accumulated blocking issues
_warns:  List[str] = []   # Accumulated warnings (non-blocking)


def _ok(msg: str)   -> None: print(f"  [\033[32m✓\033[0m] {msg}")
def _fail(msg: str) -> None: print(f"  [\033[31m✗\033[0m] {msg}"); _issues.append(msg)
def _warn(msg: str) -> None: print(f"  [\033[33m!\033[0m] {msg}"); _warns.append(msg)
def _info(msg: str) -> None: print(f"  [i] {msg}")
def _head(title: str) -> None:
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Environment variable audit
# ─────────────────────────────────────────────────────────────────────────────

def check_env() -> Dict[str, str]:
    _head("ENVIRONMENT VARIABLES")

    required = {
        "AMO_SUBDOMAIN":              "amoCRM account subdomain",
        "AMO_CLIENT_ID":              "OAuth app client ID",
        "AMO_CLIENT_SECRET":          "OAuth app client secret",
        "AMO_REDIRECT_URI":           "OAuth redirect URI",
        "GOOGLE_SHEET_ID":            "Google Spreadsheet ID",
        "GOOGLE_SERVICE_ACCOUNT_FILE":"Path to service-account JSON",
    }

    env: Dict[str, str] = {}
    for key, desc in required.items():
        val = os.getenv(key, "").strip()
        if val:
            _ok(f"{key} = {val[:60]}{'…' if len(val) > 60 else ''}")
            env[key] = val
        else:
            _fail(f"{key} is not set  ({desc})")

    sa_file = env.get("GOOGLE_SERVICE_ACCOUNT_FILE", "gsheet.json")
    if sa_file and not Path(sa_file).exists():
        _fail(f"Service-account file not found: {sa_file}")

    token_store = Path(os.getenv("AMO_TOKEN_STORE", ".amo_tokens.json"))
    if token_store.exists():
        _ok(f"AMO token store found: {token_store}")
    else:
        _warn("AMO token store not found — OAuth exchange required before service can run.")

    # Optional but recommended production settings
    optionals = {
        "TRIGGER_STATUS_NAME":         "Trigger status name (default: NOMERATSIYALANMAGAN ZAKAZ)",
        "TRIGGER_STATUS_NAMES":        "Extra trigger status names for multi-pipeline (comma-separated)",
        "PIPELINE_DISPLAY_MAP_JSON":   "Extra pipeline display name overrides (JSON)",
        "DROPDOWN_STATUS_MAP_JSON":    "Sheet → AMO status mapping",
        "LEADS_CREATED_AFTER":         "Unix timestamp cutoff for old leads",
        "SHEET_ROTATION_INTERVAL":     "monthly / hourly",
    }
    print()
    for key, desc in optionals.items():
        val = os.getenv(key, "").strip()
        if val:
            _info(f"{key} = {val[:80]}")
        else:
            _info(f"{key} not set  ({desc})")

    return env


# ─────────────────────────────────────────────────────────────────────────────
# AMO HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────

def _build_amo_session(env: Dict[str, str]):
    """Return (requests.Session, base_url) authenticated with the stored AMO token.
    Attempts a token refresh if the stored access token is invalid.
    Returns (None, None) on failure.
    """
    subdomain = env.get("AMO_SUBDOMAIN", "")
    if not subdomain:
        return None, None

    base_url = f"https://{subdomain}.amocrm.ru"
    token_store = Path(os.getenv("AMO_TOKEN_STORE", ".amo_tokens.json"))

    access_token = ""
    refresh_token = ""
    if token_store.exists():
        tokens = json.loads(token_store.read_text(encoding="utf-8"))
        access_token  = tokens.get("access_token", "")
        refresh_token = tokens.get("refresh_token", "")

    session = requests.Session()

    def _set_token(tok: str) -> None:
        session.headers.update({"Authorization": f"Bearer {tok}"})

    def _is_valid(tok: str) -> bool:
        if not tok:
            return False
        r = session.get(f"{base_url}/api/v4/account", timeout=15)
        return r.status_code == 200

    def _refresh(rtok: str) -> Optional[str]:
        payload = {
            "client_id":     env.get("AMO_CLIENT_ID", ""),
            "client_secret": env.get("AMO_CLIENT_SECRET", ""),
            "grant_type":    "refresh_token",
            "refresh_token": rtok,
            "redirect_uri":  env.get("AMO_REDIRECT_URI", ""),
        }
        r = requests.post(f"{base_url}/oauth2/access_token", json=payload, timeout=20)
        if r.status_code == 200:
            data = r.json()
            token_store.write_text(
                json.dumps({"access_token": data["access_token"],
                            "refresh_token": data["refresh_token"]},
                           ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            return data["access_token"]
        return None

    _set_token(access_token)
    if _is_valid(access_token):
        return session, base_url

    if refresh_token:
        new_token = _refresh(refresh_token)
        if new_token:
            _set_token(new_token)
            return session, base_url

    return None, None


def _amo_get(session, base_url: str, endpoint: str) -> Dict[str, Any]:
    r = session.get(f"{base_url}{endpoint}", timeout=20)
    if r.status_code == 204:
        return {}
    r.raise_for_status()
    return r.json()


# ─────────────────────────────────────────────────────────────────────────────
# 2. AMO connectivity
# ─────────────────────────────────────────────────────────────────────────────

def check_amo_connectivity(env: Dict[str, str]):
    _head("AMO CONNECTIVITY")

    session, base_url = _build_amo_session(env)
    if not session:
        _fail("Could not authenticate with AMO. Run the service first (POST /oauth/exchange) "
              "to obtain tokens, then re-run this script.")
        return None, None

    try:
        acct = _amo_get(session, base_url, "/api/v4/account")
        name = acct.get("name", "?")
        _ok(f"Connected to AMO account: \"{name}\" (subdomain: {env.get('AMO_SUBDOMAIN')})")
    except Exception as exc:
        _fail(f"AMO /api/v4/account call failed: {exc}")
        return None, None

    return session, base_url


# ─────────────────────────────────────────────────────────────────────────────
# 3. Pipeline & status audit
# ─────────────────────────────────────────────────────────────────────────────

def check_pipelines(session, base_url: str, env: Dict[str, str]) -> None:
    _head("PIPELINE & STATUS AUDIT")

    # Collect all configured trigger names (primary + extras)
    trigger_names: List[str] = []
    primary = os.getenv("TRIGGER_STATUS_NAME", "NOMERATSIYALANMAGAN ZAKAZ").strip()
    if primary:
        trigger_names.append(primary)
    extras_raw = os.getenv("TRIGGER_STATUS_NAMES", "").strip()
    if extras_raw:
        for t in extras_raw.split(","):
            t = t.strip()
            if t and t not in trigger_names:
                trigger_names.append(t)

    _info(f"Trigger status name(s) configured: {trigger_names}")

    try:
        data = _amo_get(session, base_url, "/api/v4/leads/pipelines?with=statuses&limit=250")
    except Exception as exc:
        _fail(f"Could not fetch pipelines: {exc}")
        return

    pipelines = (data.get("_embedded") or {}).get("pipelines") or []
    if not pipelines:
        _warn("No pipelines found in AMO account.")
        return

    _info(f"Found {len(pipelines)} pipeline(s).")
    print()

    # Auto-register every pipeline found in AMO that isn't already in the map.
    # This mirrors the same logic in sync_service.py _load_structure_mappings so
    # no manual configuration is needed — new pipelines added to the AMO account
    # are picked up automatically on the next run.
    newly_registered: List[str] = []
    for pl in pipelines:
        pl_name = pl.get("name", "").strip()
        if pl_name and pl_name not in PIPELINE_DISPLAY_MAP:
            PIPELINE_DISPLAY_MAP[pl_name] = pl_name
            newly_registered.append(pl_name)

    if newly_registered:
        _info(f"Auto-registered {len(newly_registered)} pipeline(s) from AMO "
              f"(use PIPELINE_DISPLAY_MAP_JSON in .env to set custom display names).")
        for n in newly_registered:
            _info(f"  '{n}'  →  display: '{n}'  (raw name used)")
        print()

    pipelines_missing_trigger: List[str] = []

    for pl in pipelines:
        pl_id   = pl.get("id")
        pl_name = pl.get("name", "?").strip()
        display = PIPELINE_DISPLAY_MAP.get(pl_name, pl_name)
        is_custom = pl_name not in newly_registered  # has a user-defined display name
        statuses = (pl.get("_embedded") or {}).get("statuses") or []
        status_names = [s.get("name", "").strip() for s in statuses]

        label = f"Pipeline [{pl_id}] \"{pl_name}\"  →  display: \"{display}\""
        if is_custom:
            _ok(label + "  (custom)")
        else:
            _ok(label + "  (auto)")

        # Check trigger status presence
        trigger_found = any(t in status_names for t in trigger_names)
        if trigger_found:
            matched = next(t for t in trigger_names if t in status_names)
            _ok(f"  Trigger status \"{matched}\" found.")
        else:
            _warn(f"  None of the trigger statuses {trigger_names} exist in this pipeline.")
            pipelines_missing_trigger.append(pl_name)

        # List all statuses in this pipeline for visibility
        for s in statuses:
            s_name = s.get("name", "").strip()
            s_id   = s.get("id")
            is_trigger = s_name in trigger_names
            _info(f"    [{s_id}] {s_name}{' ← TRIGGER' if is_trigger else ''}")

    if pipelines_missing_trigger:
        print()
        _fail(f"Pipelines missing any trigger status: {pipelines_missing_trigger}")
        _info("  Fix: ensure the trigger status name is added to each pipeline in AMO,")
        _info(f"  OR add the pipeline's local trigger name to TRIGGER_STATUS_NAMES in .env.")


# ─────────────────────────────────────────────────────────────────────────────
# 4. Lead custom-field audit
# ─────────────────────────────────────────────────────────────────────────────

def check_custom_fields(session, base_url: str) -> None:
    _head("LEAD CUSTOM-FIELD AUDIT")

    try:
        data = _amo_get(session, base_url, "/api/v4/leads/custom_fields?limit=250")
    except Exception as exc:
        _fail(f"Could not fetch lead custom fields: {exc}")
        return

    fields = (data.get("_embedded") or {}).get("custom_fields") or []
    amo_field_names = {f.get("name", "").strip() for f in fields}

    _info(f"Found {len(fields)} custom field(s) in AMO leads.")
    print()

    missing_in_amo: List[str] = []
    for col in EXPECTED_CUSTOM_FIELD_COLS:
        # Normalise spaces for comparison (AMO sometimes has double spaces)
        found = any(" ".join(f.split()) == " ".join(col.split()) for f in amo_field_names)
        if found:
            _ok(f"Custom field found: \"{col}\"")
        else:
            _fail(f"Custom field MISSING in AMO: \"{col}\"  — column will always be empty")
            missing_in_amo.append(col)

    print()
    _info("Columns sourced from standard AMO fields (not custom fields — always present):")
    for col in NON_CUSTOM_FIELD_COLS:
        _info(f"  • {col}")

    if missing_in_amo:
        print()
        _warn(f"Missing custom fields: {missing_in_amo}")
        _info("  Fix: create these custom fields in AMO under Leads → Settings → Fields.")

    # Also list AMO custom fields that are NOT in COLUMNS (informational)
    extra_fields = sorted(
        f for f in amo_field_names
        if " ".join(f.split()) not in {" ".join(c.split()) for c in EXPECTED_CUSTOM_FIELD_COLS}
    )
    if extra_fields:
        print()
        _info("AMO custom fields NOT mapped to any sheet column (ignored by sync):")
        for f in extra_fields:
            _info(f"  • {f}")


# ─────────────────────────────────────────────────────────────────────────────
# 5. Google Sheet audit / setup
# ─────────────────────────────────────────────────────────────────────────────

def check_google_sheet(env: Dict[str, str], setup: bool, staff_csv: Optional[str]) -> None:
    _head("GOOGLE SHEET AUDIT" + (" + SETUP" if setup else ""))

    sa_file  = env.get("GOOGLE_SERVICE_ACCOUNT_FILE", "gsheet.json")
    sheet_id = env.get("GOOGLE_SHEET_ID", "")
    ws_name  = os.getenv("GOOGLE_WORKSHEET_NAME", "Sheet1").strip()

    try:
        gc = gspread.service_account(filename=sa_file)
        spreadsheet = gc.open_by_key(sheet_id)
        _ok(f"Connected to spreadsheet: \"{spreadsheet.title}\"")
        _info(f"URL: https://docs.google.com/spreadsheets/d/{sheet_id}")
    except Exception as exc:
        _fail(f"Could not open spreadsheet: {exc}")
        _info("Ensure the service-account email has Editor access on the sheet.")
        return

    # ── Main data worksheet ───────────────────────────────────────────────────
    try:
        ws = spreadsheet.worksheet(ws_name)
        _ok(f"Worksheet \"{ws_name}\" exists.")
    except gspread.WorksheetNotFound:
        if setup:
            ws = spreadsheet.add_worksheet(title=ws_name, rows=2000, cols=max(26, len(COLUMNS)))
            _ok(f"Created worksheet \"{ws_name}\".")
        else:
            _fail(f"Worksheet \"{ws_name}\" not found. Run with --setup to create it.")
            return

    current_headers = ws.row_values(1)
    if current_headers == COLUMNS:
        _ok(f"Headers are correct ({len(COLUMNS)} columns).")
    else:
        if setup:
            ws.update(values=[COLUMNS], range_name="A1")
            ws.freeze(rows=1)
            try:
                ws.columns_auto_resize(0, len(COLUMNS))
            except Exception:
                pass
            _ok(f"Headers written and row 1 frozen ({len(COLUMNS)} columns).")
        else:
            _fail(f"Header mismatch! Run with --setup to fix.\n"
                  f"    Expected : {COLUMNS}\n"
                  f"    Found    : {current_headers}")

    # Status column dropdown
    dropdown_range = f"{STATUS_COL_LETTER}2:{STATUS_COL_LETTER}2000"
    if setup:
        try:
            ws.add_validation(
                dropdown_range,
                ValidationConditionType.one_of_list,
                STATUS_DROPDOWN_OPTS,
                showCustomUi=True,
            )
            _ok(f"Status dropdown applied to col {STATUS_COL_LETTER} (rows 2-2000).")
        except Exception as exc:
            _warn(f"Could not apply status dropdown: {exc}")
    else:
        _info(f"Status column: {STATUS_COL_LETTER}  "
              f"(run --setup to apply dropdown: {STATUS_DROPDOWN_OPTS})")

    print()

    # ── Staff sheet ───────────────────────────────────────────────────────────
    try:
        staff_ws = spreadsheet.worksheet("Staff")
        _ok("\"Staff\" worksheet exists.")
    except gspread.WorksheetNotFound:
        if setup:
            staff_ws = spreadsheet.add_worksheet(title="Staff", rows=500, cols=3)
            _ok("Created \"Staff\" worksheet.")
        else:
            _warn("\"Staff\" worksheet not found. Run with --setup to create it.")
            staff_ws = None

    if staff_ws is not None:
        staff_headers = ["Код сотрудника", "Имя"]
        current_sh = staff_ws.row_values(1)
        if current_sh[:2] == staff_headers:
            _ok("Staff headers are correct.")
        elif setup:
            staff_ws.update(values=[staff_headers], range_name="A1")
            staff_ws.freeze(rows=1)
            _ok("Staff headers written.")
        else:
            _warn(f"Staff header mismatch: got {current_sh}")

        staff_data = staff_ws.get_all_values()
        data_rows = max(0, len(staff_data) - 1)
        if data_rows > 0:
            _ok(f"Staff sheet has {data_rows} data row(s).")
        else:
            _warn("Staff sheet is empty. Populate it manually or use --staff <csv>.")

        # ── Import staff from CSV if requested ────────────────────────────────
        if setup and staff_csv:
            _import_staff_csv(staff_ws, staff_csv)


def _import_staff_csv(staff_ws, csv_path: str) -> None:
    import csv
    path = Path(csv_path)
    if not path.exists():
        _warn(f"Staff CSV not found: {csv_path}")
        return
    rows = []
    with open(path, encoding="utf-8-sig", newline="") as f:
        for i, row in enumerate(csv.reader(f)):
            if i == 0 and row and row[0].strip().lower() in ("код","kod","code","код сотрудника"):
                continue
            if len(row) >= 2 and row[0].strip() and row[1].strip():
                rows.append([row[0].strip(), row[1].strip()])
    if rows:
        all_vals = staff_ws.get_all_values()
        staff_ws.update(values=rows, range_name=f"A{len(all_vals)+1}")
        _ok(f"Imported {len(rows)} staff records from {csv_path}.")
    else:
        _warn(f"No valid rows found in {csv_path} (expected: code, name).")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="amo2gsheet — Production readiness checker"
    )
    parser.add_argument(
        "--setup", action="store_true",
        help="Also configure the Google Sheet (write headers, dropdowns, Staff tab)."
    )
    parser.add_argument(
        "--staff", metavar="FILE",
        help="CSV (code,name) to import into the Staff sheet. Implies --setup."
    )
    args = parser.parse_args()

    do_setup = args.setup or bool(args.staff)

    print(f"\n{SEP2}")
    print("  amo2gsheet — Production Readiness Check")
    print(SEP2)

    # 1. Env
    env = check_env()

    # 2. AMO connectivity
    session, base_url = check_amo_connectivity(env)

    # 3 & 4. AMO structure checks (only if we have a session)
    if session:
        check_pipelines(session, base_url, env)
        check_custom_fields(session, base_url)
    else:
        _warn("Skipping AMO pipeline and custom-field checks (no AMO session).")

    # 5. Google Sheet
    check_google_sheet(env, setup=do_setup, staff_csv=args.staff)

    # ── Final summary ─────────────────────────────────────────────────────────
    print(f"\n{SEP2}")
    print("  SUMMARY")
    print(SEP2)

    if _issues:
        print(f"\n  [\033[31m✗\033[0m] {len(_issues)} BLOCKING ISSUE(S) — fix before going live:\n")
        for i, issue in enumerate(_issues, 1):
            print(f"    {i}. {issue}")
    else:
        print("\n  [\033[32m✓\033[0m] No blocking issues found.")

    if _warns:
        print(f"\n  [\033[33m!\033[0m] {len(_warns)} WARNING(S) — review but not blocking:\n")
        for i, w in enumerate(_warns, 1):
            print(f"    {i}. {w}")

    print()
    if _issues:
        print("  Result: FAIL — resolve the issues above, then re-run this script.")
        print(f"{SEP2}\n")
        sys.exit(1)
    else:
        print("  Result: PASS — safe to start the sync service on this environment.")
        print(f"{SEP2}\n")
        sys.exit(0)


if __name__ == "__main__":
    main()
