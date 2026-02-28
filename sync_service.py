import json
import os
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse
import gspread
import requests
from env_loader import load_env
from fastapi import FastAPI, Request
from gspread.utils import ValidationConditionType
from dashboard_router import create_dashboard_router

load_env()


COLUMNS = [
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
    "Статус",
]

# Maps raw AmoCRM pipeline name → display name written to Google Sheets.
# Populated dynamically from AMO at startup; every pipeline found in the AMO
# account is registered automatically using its raw name as the display value.
# Use PIPELINE_DISPLAY_MAP_JSON in .env to set shorter / prettier names.
PIPELINE_DISPLAY_MAP: Dict[str, str] = {}

# Allow .env to add/override pipeline display names without editing this file.
# Example: PIPELINE_DISPLAY_MAP_JSON={"Nilufar - Sotuv Bioflex": "Нилуфар", ...}
try:
    _env_pipeline_map: Dict[str, str] = json.loads(os.getenv("PIPELINE_DISPLAY_MAP_JSON", "{}"))
    PIPELINE_DISPLAY_MAP.update(_env_pipeline_map)
except Exception:
    pass

# Maps raw AmoCRM status name → proper Russian display name written to Google Sheets
STATUS_DISPLAY_MAP: Dict[str, str] = {
    "Неразобранное":              "Неразобранное",
    "КОНСУЛТАЦИЯ":                "Консультация",
    "Консультация":               "Консультация",
    "ДУМКА":                      "Раздумье",
    "Раздумье":                   "Раздумье",
    "Заказ":                      "Заказ",
    "ЗАКАЗ":                      "Заказ",
    "NOMERATSIYALANMAGAN ZAKAZ":  "В процессе",
    "Заказ без нумерации":        "В процессе",
    "ЗАКАЗ БЕЗ НУМЕРАЦИИ":       "В процессе",
    "ЗАЗАЗ БЕЗ НУМЕРАЦИИ":       "В процессе",
    "ЗАКАЗ ОТПРАВЛЕН":            "У курера",
    "Заказ отправлен":            "У курера",
    "OTKAZ":                      "Отказ",
    "ОТКАЗ":                      "Отказ",
    "Отказ":                      "Отказ",
    "Успешно":                    "Успешно",
    "Успешно ":                   "Успешно",
    "Успешно реализовано":        "Успешно",
    "Закрыто и не реализовано":   "Закрыто и не реализовано",
}

ID_COL_INDEX = COLUMNS.index("ID")
STATUS_COL_INDEX = COLUMNS.index("Статус")
ORDER_NUM_COL_INDEX = COLUMNS.index("Заказ №")

# AMO display name to target when admin fills in Заказ № on the sheet.
# "Заказ отправлен" maps to display name "У курера" in STATUS_DISPLAY_MAP.
ORDER_NUM_FILLED_AMO_STATUS_DISPLAY = "У курера"

# Maps what the user picks in Google Sheets → the AMO display name used for status ID lookup.
# Both "У курера" and "Успешно" move the lead to the "Успешно реализовано" (won) step in AMO.
# "Отказ" moves the lead to the pipeline's reject step.
SHEET_STATUS_TO_AMO_DISPLAY: Dict[str, str] = {
    "В процессе": "В процессе",
    "У курера":   "Успешно",
    "Успешно":    "Успешно",
    "Отказ":      "Отказ",
}

# Maps an AMO display status name → the status that should be written to the Google Sheet
# when a tracked lead receives that AMO status via webhook.
# e.g. when a manager manually sets "Раздумье" in AMO, the sheet row is updated to "Отказ".
AMO_STATUS_TO_SHEET_OVERRIDE: Dict[str, str] = {
    "Раздумье": "Отказ",
}


def _parse_leads_created_after(raw: str) -> int:
    """Accept a Unix timestamp integer OR a human-readable date/time string (UTC).

    Supported formats:
      - '27.02.2026 00:00:00'  (DD.MM.YYYY HH:MM:SS)
      - '27.02.2026 00:00'     (DD.MM.YYYY HH:MM)
      - '2026-02-27 00:00:00'  (YYYY-MM-DD HH:MM:SS)
      - '2026-02-27'           (YYYY-MM-DD)
      - '1772121600'           (plain Unix timestamp)
      - '0' or empty          → process all leads
    """
    raw = (raw or "").strip()
    if not raw or raw == "0":
        return 0
    if raw.isdigit():
        return int(raw)
    for fmt in ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return int(datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc).timestamp())
        except ValueError:
            continue
    print(f"[WARN] LEADS_CREATED_AFTER='{raw}' is not a recognized format. Using 0.")
    return 0


class Config:
    AMO_SUBDOMAIN = os.getenv("AMO_SUBDOMAIN", "").strip()
    AMO_CLIENT_ID = os.getenv("AMO_CLIENT_ID", "").strip()
    AMO_CLIENT_SECRET = os.getenv("AMO_CLIENT_SECRET", "").strip()
    AMO_REDIRECT_URI = (os.getenv("AMO_REDIRECT_URI") or os.getenv("AMO_REDIRECT_URL") or "").strip()
    AMO_AUTH_CODE = os.getenv("AMO_AUTH_CODE", "").strip()

    TOKEN_STORE_PATH = Path(os.getenv("AMO_TOKEN_STORE", ".amo_tokens.json"))

    GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "gsheet.json").strip()
    GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
    GOOGLE_WORKSHEET_NAME = os.getenv("GOOGLE_WORKSHEET_NAME", "Sheet1").strip()

    TRIGGER_STATUS_ID = int(os.getenv("TRIGGER_STATUS_ID", "0"))
    PIPELINE_ID = int(os.getenv("PIPELINE_ID", "0"))
    TRIGGER_STATUS_NAME = os.getenv("TRIGGER_STATUS_NAME", "NOMERATSIYALANMAGAN ZAKAZ").strip()
    # Additional trigger status names for multi-pipeline setups (comma-separated).
    # When different pipelines (Воронка) use a differently-named status to signal a new
    # order, list those names here.  All names are checked during status resolution.
    TRIGGER_STATUS_NAMES_EXTRA = os.getenv("TRIGGER_STATUS_NAMES", "").strip()

    STATUS_MAP = json.loads(os.getenv("DROPDOWN_STATUS_MAP_JSON", "{}"))
    STATUS_ID_TO_NAME = {str(v): k for k, v in STATUS_MAP.items() if v}

    SYNC_POLL_SECONDS = int(os.getenv("SYNC_POLL_SECONDS", "60"))
    # Sheet rotation interval: "monthly" (default) or "hourly" (useful for testing).
    SHEET_ROTATION_INTERVAL = os.getenv("SHEET_ROTATION_INTERVAL", "monthly").strip().lower()
    # Initial date-range sync: YYYY-MM-DD strings. Both must be set to activate.
    INITIAL_SYNC_DATE_FROM = os.getenv("INITIAL_SYNC_DATE_FROM", "").strip()
    INITIAL_SYNC_DATE_TO   = os.getenv("INITIAL_SYNC_DATE_TO",   "").strip()
    # Minimum seconds between consecutive amoCRM API calls. Increase on prod if you see 429s.
    AMO_REQUEST_DELAY_SEC = float(os.getenv("AMO_REQUEST_DELAY_SEC", "0.08"))
    # How long (seconds) the Staff sheet mapping is cached before re-fetching.
    STAFF_CACHE_TTL_SEC = int(os.getenv("STAFF_CACHE_TTL_SEC", "300"))
    # If the same (lead_id, status_id) webhook arrives again within this window, skip it.
    # Prevents repeated AMO API calls caused by amoCRM’s own webhook retry logic.
    WEBHOOK_DEDUP_TTL_SEC = int(os.getenv("WEBHOOK_DEDUP_TTL_SEC", "60"))
    # Leads created before this timestamp are silently ignored (0 = process all).
    # Supports human-readable 'DD.MM.YYYY HH:MM:SS' (UTC) or plain Unix timestamp.
    LEADS_CREATED_AFTER = _parse_leads_created_after(os.getenv("LEADS_CREATED_AFTER", "0"))
    # Only process leads from pipelines whose name contains this keyword (case-insensitive).
    # New pipelines matching the keyword are picked up automatically. Empty = all pipelines.
    PIPELINE_KEYWORD = os.getenv("PIPELINE_KEYWORD", "").strip().lower()


def require_env() -> None:
    required = [
        "AMO_SUBDOMAIN",
        "AMO_CLIENT_ID",
        "AMO_CLIENT_SECRET",
        "AMO_REDIRECT_URI",
        "GOOGLE_SHEET_ID",
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")


class TokenStore:
    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.Lock()

    def load(self) -> Dict[str, str]:
        with self.lock:
            if not self.path.exists():
                return {
                    "access_token": os.getenv("AMO_ACCESS_TOKEN", ""),
                    "refresh_token": os.getenv("AMO_REFRESH_TOKEN", ""),
                }
            return json.loads(self.path.read_text(encoding="utf-8"))

    def save(self, access_token: str, refresh_token: str) -> None:
        with self.lock:
            self.path.write_text(
                json.dumps(
                    {"access_token": access_token, "refresh_token": refresh_token},
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )


class AmoClient:
    def __init__(self, cfg: Config, token_store: TokenStore):
        self.cfg = cfg
        self.token_store = token_store
        self.base_url = f"https://{cfg.AMO_SUBDOMAIN}.amocrm.ru"
        # Per-call throttle state
        self._last_request_ts: float = 0.0
        self._req_lock = threading.Lock()
        # Token cache – avoids a /account ping before every API call
        self._cached_access_token: str = ""
        self._token_validated_ts: float = 0.0

    def _throttle(self) -> None:
        """Enforce a minimum gap between consecutive AMO API calls."""
        delay = self.cfg.AMO_REQUEST_DELAY_SEC
        if delay <= 0:
            return
        with self._req_lock:
            elapsed = time.time() - self._last_request_ts
            if elapsed < delay:
                time.sleep(delay - elapsed)
            self._last_request_ts = time.time()

    def _api_request(self, method: str, url: str, headers: Dict, **kwargs) -> requests.Response:
        """Execute an AMO API call with throttle and automatic 429 back-off retry."""
        for attempt in range(1, 6):
            self._throttle()
            r = requests.request(method, url, headers=headers, timeout=30, **kwargs)
            if r.status_code == 429:
                wait = min(attempt * 10, 60)
                print(f"[WARN] AMO 429 on {method} {url}, retrying in {wait}s (attempt {attempt}/5)")
                time.sleep(wait)
                continue
            return r
        return r  # return last response after exhausting retries

    def auth_url(self) -> str:
        return (
            "https://www.amocrm.ru/oauth"
            f"?client_id={self.cfg.AMO_CLIENT_ID}"
            "&response_type=code"
            f"&redirect_uri={requests.utils.quote(self.cfg.AMO_REDIRECT_URI, safe='')}"
            "&state=setup"
        )

    def _token_data(self) -> Dict[str, str]:
        return self.token_store.load()

    def _headers(self, access_token: str) -> Dict[str, str]:
        return {"Authorization": f"Bearer {access_token}"}

    def _is_token_valid(self, access_token: str) -> bool:
        if not access_token:
            return False
        r = requests.get(
            f"{self.base_url}/api/v4/account",
            headers=self._headers(access_token),
            timeout=20,
        )
        return r.status_code == 200

    def _refresh(self, refresh_token: str) -> str:
        if not refresh_token:
            raise RuntimeError("No refresh token found. Complete OAuth first.")

        payload = {
            "client_id": self.cfg.AMO_CLIENT_ID,
            "client_secret": self.cfg.AMO_CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "redirect_uri": self.cfg.AMO_REDIRECT_URI,
        }
        r = requests.post(
            f"{self.base_url}/oauth2/access_token",
            json=payload,
            timeout=30,
        )
        if r.status_code != 200:
            raise RuntimeError(f"Token refresh failed: {r.status_code} {r.text}")

        data = r.json()
        self.token_store.save(data["access_token"], data["refresh_token"])
        return data["access_token"]

    def get_access_token(self) -> str:
        # Re-use cached token for up to 5 minutes to avoid a /account ping before every call
        now = time.time()
        if self._cached_access_token and now - self._token_validated_ts < 300:
            return self._cached_access_token

        tokens = self._token_data()
        access_token = tokens.get("access_token", "")
        refresh_token = tokens.get("refresh_token", "")

        if self._is_token_valid(access_token):
            self._cached_access_token = access_token
            self._token_validated_ts = now
            return access_token
        if not refresh_token and self.cfg.AMO_AUTH_CODE:
            print("[INFO] No refresh token found, trying AMO_AUTH_CODE bootstrap...")
            try:
                data = self.exchange_code(self.cfg.AMO_AUTH_CODE)
                token = data["access_token"]
                self._cached_access_token = token
                self._token_validated_ts = time.time()
                return token
            except Exception as exc:
                raise RuntimeError(f"AMO_AUTH_CODE bootstrap failed: {exc}")
        token = self._refresh(refresh_token)
        self._cached_access_token = token
        self._token_validated_ts = time.time()
        return token

    def exchange_code(self, code_or_redirect_url: str) -> Dict[str, Any]:
        value = (code_or_redirect_url or "").strip()
        if not value:
            raise RuntimeError("Authorization code is empty")

        if "code=" in value:
            code = value.split("code=")[1].split("&")[0]
        else:
            parsed = urlparse(value)
            if parsed.query:
                code = parse_qs(parsed.query).get("code", [""])[0]
            else:
                code = value

        if code.count(".") == 2 and code.startswith("eyJ"):
            raise RuntimeError(
                "AMO_AUTH_CODE looks like a JWT token, not OAuth authorization code. "
                "Use the short-lived value from redirect URL parameter '?code=...'."
            )

        payload = {
            "client_id": self.cfg.AMO_CLIENT_ID,
            "client_secret": self.cfg.AMO_CLIENT_SECRET,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.cfg.AMO_REDIRECT_URI,
        }
        r = requests.post(f"{self.base_url}/oauth2/access_token", json=payload, timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"OAuth exchange failed: {r.status_code} {r.text}")

        data = r.json()
        self.token_store.save(data["access_token"], data["refresh_token"])
        return data

    def get(self, endpoint: str) -> Dict[str, Any]:
        token = self.get_access_token()
        r = self._api_request("GET", f"{self.base_url}{endpoint}", self._headers(token))
        if r.status_code == 204 or not r.text:
            return {}
        if r.status_code >= 400:
            raise RuntimeError(f"GET {endpoint} failed: {r.status_code} {r.text}")
        return r.json()

    def fetch_leads_by_date_range(
        self, date_from: str, date_to: str
    ) -> List[Dict[str, Any]]:
        """Fetch all AMO leads whose created_at falls in [date_from, date_to] (YYYY-MM-DD).

        Pages through the full result set automatically.
        """
        try:
            ts_from = int(datetime.strptime(date_from, "%Y-%m-%d").timestamp())
            # Include the entire last day (up to 23:59:59).
            ts_to   = int(datetime.strptime(date_to,   "%Y-%m-%d").timestamp()) + 86399
        except ValueError as exc:
            raise RuntimeError(f"Invalid date format (expected YYYY-MM-DD): {exc}")

        all_leads: List[Dict[str, Any]] = []
        page = 1
        while True:
            endpoint = (
                f"/api/v4/leads"
                f"?filter[created_at][from]={ts_from}"
                f"&filter[created_at][to]={ts_to}"
                f"&limit=250&page={page}"
            )
            try:
                data = self.get(endpoint)
            except RuntimeError as exc:
                if "204" in str(exc) or "No Content" in str(exc):
                    break  # AMO returns 204 when there are no more pages
                raise
            leads = (data.get("_embedded") or {}).get("leads") or []
            if not leads:
                break
            all_leads.extend(leads)
            # AMO paginates with _links.next; stop when it is absent.
            if not (data.get("_links") or {}).get("next"):
                break
            page += 1
        return all_leads

    def fetch_order_event_lead_ids(
        self,
        ts_from: int,
        ts_to: int,
        order_status_ids: set,
        created_lead_ids: set | None = None,
    ) -> set:
        """Return the set of lead IDs that *first* entered an order stage in [ts_from, ts_to].

        Only counts transitions FROM a non-order stage TO an order stage, so
        internal hops like Заказ→В процессе→У курера are not double-counted.

        If ``created_lead_ids`` is given, the result is further intersected with
        that set — i.e. only leads that were also created in the same window count.
        This matches the reference-sheet definition:
            consul  = leads created on date
            zakas   = leads created on date that became orders on that same date
        """
        events: List[Dict[str, Any]] = []
        page = 1
        while True:
            endpoint = (
                f"/api/v4/events"
                f"?filter[type][]=lead_status_changed"
                f"&filter[created_at][from]={ts_from}"
                f"&filter[created_at][to]={ts_to}"
                f"&limit=250&page={page}"
            )
            try:
                data = self.get(endpoint)
            except RuntimeError as exc:
                if "204" in str(exc) or "No Content" in str(exc):
                    break
                raise
            batch = (data.get("_embedded") or {}).get("events") or []
            if not batch:
                break
            events.extend(batch)
            if not (data.get("_links") or {}).get("next"):
                break
            page += 1

        first_entry: Dict[int, int] = {}  # lead_id -> earliest event ts
        for ev in events:
            lead_id  = int(ev.get("entity_id", 0) or 0)
            before   = (ev.get("value_before") or [{}])[0]
            after    = (ev.get("value_after")  or [{}])[0]
            old_sid  = int((before.get("lead_status") or {}).get("id", 0) or 0)
            new_sid  = int((after.get("lead_status")  or {}).get("id", 0) or 0)
            if new_sid in order_status_ids and old_sid not in order_status_ids:
                ev_ts = int(ev.get("created_at", 0) or 0)
                if lead_id not in first_entry or ev_ts < first_entry[lead_id]:
                    first_entry[lead_id] = ev_ts

        result = set(first_entry.keys())
        if created_lead_ids is not None:
            result &= created_lead_ids
        return result

    def patch(self, endpoint: str, body: Dict[str, Any]) -> Dict[str, Any]:
        token = self.get_access_token()
        r = self._api_request(
            "PATCH",
            f"{self.base_url}{endpoint}",
            {**self._headers(token), "Content-Type": "application/json"},
            json=body,
        )
        if r.status_code >= 400:
            raise RuntimeError(f"PATCH {endpoint} failed: {r.status_code} {r.text}")
        return r.json() if r.text else {}


class SheetSync:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.gc = gspread.service_account(filename=cfg.GOOGLE_SERVICE_ACCOUNT_FILE)
        self.spreadsheet = self.gc.open_by_key(cfg.GOOGLE_SHEET_ID)
        self.lock = threading.Lock()
        # Cache of worksheet objects keyed by tab name
        self._sheets: Dict[str, Any] = {}
        # Staff mapping cache – refreshed every STAFF_CACHE_TTL_SEC seconds
        self._staff_cache: Dict[str, str] = {}
        self._staff_cache_ts: float = 0.0
        # In-memory row index: ws_name → {lead_id → 1-based row number}
        # A single get_all_values() builds the index; all subsequent find_row / upsert
        # calls are O(1) dict lookups with no additional Sheets API calls.
        self._row_index: Dict[str, Dict[str, int]] = {}
        self._row_count: Dict[str, int] = {}  # ws_name → last occupied row number

    def _get_or_create_sheet(self, name: str):
        if name in self._sheets:
            return self._sheets[name]
        # Always re-fetch spreadsheet metadata first to avoid stale cache issues
        self.spreadsheet = self.gc.open_by_key(self.cfg.GOOGLE_SHEET_ID)
        try:
            ws = self.spreadsheet.worksheet(name)
        except gspread.WorksheetNotFound:
            ws = self.spreadsheet.add_worksheet(title=name, rows=2000, cols=max(26, len(COLUMNS)))
        
        # Only enforce main columns on the main worksheet
        if name == self.cfg.GOOGLE_WORKSHEET_NAME:
            first_row = ws.row_values(1)
            if first_row != COLUMNS:
                ws.update(values=[COLUMNS], range_name="A1")
                ws.freeze(rows=1)
                # Header changed — row index is stale
                self._invalidate_row_index(name)
            # Apply dropdown validation to the entire status column (rows 2-2000)
            status_col_letter = chr(ord("A") + STATUS_COL_INDEX)
            self._apply_status_dropdown(ws, f"{status_col_letter}2:{status_col_letter}2000")
                
        self._sheets[name] = ws
        return ws

    def _sheet_for_pipeline(self, pipeline_display: str):
        """Return (and lazily create) the worksheet for a given pipeline display name."""
        # All leads are recorded on one file, ignoring pipeline_display
        tab = self.cfg.GOOGLE_WORKSHEET_NAME
        return self._get_or_create_sheet(tab)

    def rotate_to_archive(self, archive_tab_name: str) -> None:
        """Rename the current active worksheet to archive_tab_name, then create a
        fresh worksheet with the default name so new-month leads start on a clean tab.
        """
        with self.lock:
            main_name = self.cfg.GOOGLE_WORKSHEET_NAME
            # Rename the existing active sheet to the archive name
            try:
                ws = self.spreadsheet.worksheet(main_name)
                ws.update_title(archive_tab_name)
                print(f"[INFO] Worksheet '{main_name}' renamed to '{archive_tab_name}'")
            except gspread.WorksheetNotFound:
                print(f"[WARN] Worksheet '{main_name}' not found during rotation — skipping rename")
            # Clear the sheet cache so the renamed tab is no longer served as the active sheet
            self._sheets.pop(main_name, None)
            self._sheets.pop(archive_tab_name, None)
            # Invalidate row indices for both old and new tab names
            self._invalidate_row_index(main_name)
            self._invalidate_row_index(archive_tab_name)
            # Create (or re-open) a new active sheet with headers + dropdown
            self._get_or_create_sheet(main_name)
            print(f"[INFO] New active worksheet '{main_name}' created for the new month")

    def get_staff_mapping(self) -> Dict[str, str]:
        """Fetch the staff mapping from the 'Staff' sheet (result is cached for STAFF_CACHE_TTL_SEC)."""
        now = time.time()
        if self._staff_cache and now - self._staff_cache_ts < self.cfg.STAFF_CACHE_TTL_SEC:
            return self._staff_cache
        try:
            ws = self._get_or_create_sheet("Staff")
            values = ws.get_all_values()
            mapping = {}
            # Staff sheet columns: №(0) | Код сотрудника(1) | Сотрудник(2) | Отдел(3)
            for row in values[1:]:  # Skip header
                if len(row) >= 3:
                    code = str(row[1]).strip()
                    name = str(row[2]).strip()
                    if code and name:
                        # Store with and without leading zeros for flexible matching
                        try:
                            code_int = str(int(code))
                        except ValueError:
                            code_int = code
                        mapping[code] = name        # e.g. "0134" → name
                        mapping[code_int] = name    # e.g. "134"  → name
            self._staff_cache = mapping
            self._staff_cache_ts = now
            return mapping
        except Exception as e:
            print(f"[WARN] Could not load Staff sheet: {e}")
            return self._staff_cache  # Return stale cache on error rather than empty

    # Statuses that can be chosen from the dropdown in the "Статус" column
    STATUS_DROPDOWN_OPTIONS = ["В процессе", "У курера", "Успешно", "Отказ"]

    def _apply_status_dropdown(self, ws, row_range: str) -> None:
        """Apply a dropdown validation to the status column for the given row range.

        ``row_range`` should be an A1-notation range for the status column only,
        e.g. ``"T2:T2000"`` or ``"T5:T5"``.
        """
        try:
            ws.add_validation(
                row_range,
                ValidationConditionType.one_of_list,
                self.STATUS_DROPDOWN_OPTIONS,
                showCustomUi=True,
            )
        except Exception as e:
            print(f"[WARN] Could not set dropdown validation on {row_range}: {e}")

    def _all_rows(self, ws) -> List[List[str]]:
        values = ws.get_all_values()
        if not values:
            return []
        return values[1:]

    # ── Row index cache ───────────────────────────────────────────────────────
    # Eliminates repeated get_all_values() calls.  Built once per worksheet on
    # first access; updated in O(1) whenever a row is appended or header reset.

    def _build_row_index(self, ws, ws_name: str) -> None:
        """Read the sheet once and build lead_id → row_number mapping."""
        all_vals = ws.get_all_values()
        idx: Dict[str, int] = {}
        for i, row in enumerate(all_vals):
            if i == 0:
                continue  # skip header
            if len(row) > ID_COL_INDEX:
                lid = str(row[ID_COL_INDEX]).strip()
                if lid:
                    idx[lid] = i + 1  # 1-based row number
        self._row_index[ws_name] = idx
        self._row_count[ws_name] = len(all_vals)

    def _get_row_index(self, ws, ws_name: str) -> Dict[str, int]:
        if ws_name not in self._row_index:
            self._build_row_index(ws, ws_name)
        return self._row_index[ws_name]

    def _invalidate_row_index(self, ws_name: str) -> None:
        """Discard cached index so it is rebuilt on next access."""
        self._row_index.pop(ws_name, None)
        self._row_count.pop(ws_name, None)

    def find_row(self, ws, lead_id: str) -> Optional[int]:
        """O(1) row lookup via in-memory index (cold start: one get_all_values())."""
        return self._get_row_index(ws, ws.title).get(str(lead_id))

    def upsert_row(self, row_data: List[Any], pipeline_display: str = "") -> int:
        ws = self._sheet_for_pipeline(pipeline_display)
        lead_id = str(row_data[ID_COL_INDEX])
        ws_name = ws.title
        with self.lock:
            row_idx = self._get_row_index(ws, ws_name)
            row_num = row_idx.get(lead_id)
            if row_num:
                ws.update(values=[row_data], range_name=f"A{row_num}")
                return row_num

            # Append after the last known row — no second get_all_values() needed
            next_row = self._row_count.get(ws_name, 1) + 1
            ws.update(values=[row_data], range_name=f"A{next_row}")
            # Keep index consistent
            row_idx[lead_id] = next_row
            self._row_count[ws_name] = next_row
            # Apply a dropdown to the status cell of the new row
            status_col_letter = chr(ord("A") + STATUS_COL_INDEX)
            self._apply_status_dropdown(ws, f"{status_col_letter}{next_row}:{status_col_letter}{next_row}")
            return next_row

    def update_status(self, lead_id: str, status_name: str, pipeline_display: str = "") -> None:
        ws = self._sheet_for_pipeline(pipeline_display)
        with self.lock:
            row_num = self.find_row(ws, lead_id)
            if not row_num:
                return
            col = STATUS_COL_INDEX + 1
            ws.update_cell(row_num, col, status_name)

    def iter_lead_statuses(self) -> List[Dict[str, str]]:
        """Iterate statuses across the main worksheet."""
        out: List[Dict[str, str]] = []
        try:
            ws = self._get_or_create_sheet(self.cfg.GOOGLE_WORKSHEET_NAME)
        except Exception:
            return out
        
        for row in self._all_rows(ws):
            if len(row) <= max(ID_COL_INDEX, STATUS_COL_INDEX):
                continue
            lead_id = str(row[ID_COL_INDEX]).strip()
            status = str(row[STATUS_COL_INDEX]).strip()
            order_number = str(row[ORDER_NUM_COL_INDEX]).strip() if len(row) > ORDER_NUM_COL_INDEX else ""
            if lead_id:
                out.append({"lead_id": lead_id, "status": status, "order_number": order_number})
        return out


def parse_payload(raw: bytes, content_type: str) -> Dict[str, Any]:
    text = raw.decode("utf-8") if raw else ""
    if "application/json" in (content_type or ""):
        if not text:
            return {}
        return json.loads(text)

    parsed = parse_qs(text, keep_blank_values=True)
    return {k: (v[0] if isinstance(v, list) and v else "") for k, v in parsed.items()}


def extract_leads(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if isinstance(data.get("_embedded"), dict) and isinstance(data["_embedded"].get("leads"), list):
        return data["_embedded"]["leads"]

    grouped: Dict[str, Dict[str, Any]] = {}
    pattern = re.compile(r"^leads\[(add|update|status)\]\[(\d+)\]\[(.+)\]$")

    for key, value in data.items():
        m = pattern.match(key)
        if not m:
            continue
        action, idx, field = m.groups()
        group_key = f"{action}_{idx}"
        grouped.setdefault(group_key, {})[field] = value

    return list(grouped.values())


def build_row(lead: Dict[str, Any], status_name: str, pipeline_name: str = "", responsible_name: str = "", staff_mapping: Dict[str, str] = None) -> List[Any]:
    display_status = STATUS_DISPLAY_MAP.get(status_name, status_name)
    display_pipeline = PIPELINE_DISPLAY_MAP.get(pipeline_name, pipeline_name)

    # Extract contact name and phone from embedded contacts
    contact_name = lead.get("name", "")
    contact_phone = ""
    contacts = (lead.get("_embedded") or {}).get("contacts") or []
    for contact in contacts:
        if contact.get("name"):
            contact_name = contact["name"]
        # custom_fields_values may already be embedded (if fetched with full contact)
        for cf in contact.get("custom_fields_values") or []:
            if cf.get("field_code") == "PHONE" or cf.get("field_name", "").upper() in ("PHONE", "ТЕЛЕФОН"):
                vals = cf.get("values") or []
                if vals:
                    contact_phone = vals[0].get("value", "")
                    break
        if contact_phone:
            break

    # Extract company name from embedded companies
    company_name = ""
    companies = (lead.get("_embedded") or {}).get("companies") or []
    if companies:
        company_name = companies[0].get("name", "")

    mapped: Dict[str, Any] = {
        "ID": lead.get("id", ""),
        "Бюджет сделки": lead.get("price", ""),
        "Статус": display_status,
        "Воронка": display_pipeline,
        "Ф.И.О.": contact_name,
        "Контактный номер": contact_phone,
        "Компания": company_name,
        "Ответственный": responsible_name,
    }

    if isinstance(lead.get("custom_fields_values"), list):
        for cf in lead["custom_fields_values"]:
            field_name = cf.get("field_name", "")
            # Normalize spaces (e.g. "Количество  1" -> "Количество 1")
            norm_name = " ".join(field_name.split())
            values = cf.get("values") or []
            if norm_name in COLUMNS and values:
                # Join multiple values if present (e.g. multiple products)
                val = ", ".join(str(v.get("value", "")) for v in values if v.get("value") is not None)
                
                # Convert Unix timestamps to human-readable dates
                if norm_name in ("Дата заказа", "Дата доставка"):
                    try:
                        # Use the first value for dates
                        first_val = values[0].get("value", "")
                        if first_val == 0 or first_val == "0":
                            val = ""
                        elif isinstance(first_val, (int, float)):
                            val = datetime.fromtimestamp(first_val).strftime("%d.%m.%Y")
                        elif isinstance(first_val, str) and first_val.isdigit():
                            val = datetime.fromtimestamp(int(first_val)).strftime("%d.%m.%Y")
                    except Exception:
                        pass  # Keep original value if conversion fails
                        
                mapped[norm_name] = val
                
                if norm_name == "Код сотрудника" and staff_mapping:
                    clean_val = val.strip()
                    # Remove leading zeros to match the Staff sheet (e.g., "0100" -> "100", "0005" -> "5")
                    try:
                        clean_val = str(int(clean_val))
                    except ValueError:
                        pass
                        
                    if clean_val in staff_mapping:
                        mapped["Ответственный"] = staff_mapping[clean_val]

    # Re-apply pipeline-derived fields AFTER custom fields so AMO custom fields
    # named "Статус" or "Воронка" can never silently overwrite the correct values.
    mapped["Статус"] = display_status
    mapped["Воронка"] = display_pipeline

    return [mapped.get(col, "") for col in COLUMNS]


class SyncService:
    def __init__(self):
        require_env()
        self.cfg = Config()
        self.token_store = TokenStore(self.cfg.TOKEN_STORE_PATH)
        self.amo = AmoClient(self.cfg, self.token_store)
        self.sheet = SheetSync(self.cfg)
        self.state_lock = threading.Lock()
        self.state_path = Path(".sync_state.json")
        self.state = self._load_state()
        self._state_dirty: bool = False  # True when in-memory state differs from disk
        self.trigger_status_ids: set[int] = set()
        self.terminal_status_id_to_name: Dict[str, str] = {}
        self.pipeline_status_name_to_id: Dict[int, Dict[str, int]] = {}
        self.pipeline_status_display_to_id: Dict[int, Dict[str, int]] = {}
        self.pipeline_id_to_name: Dict[int, str] = {}
        self.status_id_to_display_name: Dict[int, str] = {}
        self.users_map: Dict[int, str] = {}
        # Deduplication cache: maps "lead_id:status_id" -> timestamp of last processing
        self._webhook_dedup: Dict[str, float] = {}
        self._dedup_lock = threading.Lock()
        self._load_structure_mappings()
        self._load_users()
        self._print_config_warnings()

    def _is_duplicate_webhook(self, lead_id: str, status_id: int) -> bool:
        """Return True if this (lead_id, status_id) was already processed within WEBHOOK_DEDUP_TTL_SEC.
        Also evicts stale entries to prevent unbounded memory growth.
        """
        key = f"{lead_id}:{status_id}"
        now = time.time()
        ttl = self.cfg.WEBHOOK_DEDUP_TTL_SEC
        with self._dedup_lock:
            # Evict entries older than 2x TTL
            stale = [k for k, ts in self._webhook_dedup.items() if now - ts > ttl * 2]
            for k in stale:
                del self._webhook_dedup[k]
            if key in self._webhook_dedup and now - self._webhook_dedup[key] < ttl:
                return True
            self._webhook_dedup[key] = now
            return False

    def _load_users(self) -> None:
        try:
            data = self.amo.get("/api/v4/users?limit=250")
            users = data.get("_embedded", {}).get("users", [])
            for u in users:
                self.users_map[u["id"]] = u["name"]
        except Exception as exc:
            print(f"[WARN] Could not load users: {exc}")

    def _load_structure_mappings(self) -> None:
        try:
            data = self.amo.get("/api/v4/leads/pipelines?with=statuses&limit=250")
            pipelines = data.get("_embedded", {}).get("pipelines", [])
        except Exception as exc:
            print(f"[WARN] Could not load amo structure, falling back to .env IDs: {exc}")
            if "refresh token" in str(exc).lower() or "token" in str(exc).lower():
                print("[INFO] Complete OAuth first via POST /oauth/exchange, then restart service.")
                if self.cfg.AMO_CLIENT_ID and self.cfg.AMO_REDIRECT_URI:
                    print(f"[INFO] Open auth URL: {self.amo.auth_url()}")
            pipelines = []

        for pipeline in pipelines:
            pipeline_id = int(pipeline.get("id", 0) or 0)
            pipeline_raw_name = str(pipeline.get("name", "")).strip()
            self.pipeline_id_to_name[pipeline_id] = pipeline_raw_name

            # Auto-register any pipeline not yet in the display map using its raw
            # AMO name as the display value.  .env PIPELINE_DISPLAY_MAP_JSON entries
            # (loaded at module level) take precedence because they were applied first.
            if pipeline_raw_name and pipeline_raw_name not in PIPELINE_DISPLAY_MAP:
                PIPELINE_DISPLAY_MAP[pipeline_raw_name] = pipeline_raw_name
                print(f"[INFO] Pipeline auto-registered: '{pipeline_raw_name}' "
                      f"(set PIPELINE_DISPLAY_MAP_JSON to customise display name)")

            statuses = pipeline.get("_embedded", {}).get("statuses", [])
            if pipeline_id not in self.pipeline_status_name_to_id:
                self.pipeline_status_name_to_id[pipeline_id] = {}
            if pipeline_id not in self.pipeline_status_display_to_id:
                self.pipeline_status_display_to_id[pipeline_id] = {}

            for status in statuses:
                status_name = str(status.get("name", "")).strip()
                status_id = int(status.get("id", 0) or 0)
                if not status_id or not status_name:
                    continue

                display_name = STATUS_DISPLAY_MAP.get(status_name, status_name)
                self.pipeline_status_name_to_id[pipeline_id][status_name] = status_id
                self.pipeline_status_display_to_id[pipeline_id][display_name] = status_id
                self.status_id_to_display_name[status_id] = display_name

                # Build the full list of trigger names to check (primary + extras).
                all_trigger_names = [self.cfg.TRIGGER_STATUS_NAME]
                for _tn in self.cfg.TRIGGER_STATUS_NAMES_EXTRA.split(","):
                    _tn = _tn.strip()
                    if _tn and _tn not in all_trigger_names:
                        all_trigger_names.append(_tn)

                # Match trigger by raw name OR display name across ALL configured trigger names.
                for t_name in all_trigger_names:
                    t_display = STATUS_DISPLAY_MAP.get(t_name, t_name)
                    if status_name == t_name or display_name == t_display:
                        self.trigger_status_ids.add(status_id)
                        break

                if display_name in self.cfg.STATUS_MAP or status_name in self.cfg.STATUS_MAP:
                    self.terminal_status_id_to_name[str(status_id)] = display_name

        if self.cfg.TRIGGER_STATUS_ID:
            self.trigger_status_ids.add(self.cfg.TRIGGER_STATUS_ID)

        if not self.terminal_status_id_to_name:
            self.terminal_status_id_to_name = dict(self.cfg.STATUS_ID_TO_NAME)

    def _print_config_warnings(self) -> None:
        if not self.trigger_status_ids:
            print("[WARN] No trigger status IDs resolved. Leads will NOT be added from webhook.")
        if self.cfg.PIPELINE_ID == 0:
            print("[WARN] PIPELINE_ID is 0. Service will try to use each lead's current pipeline dynamically.")
        zero_terminal = [name for name, sid in self.cfg.STATUS_MAP.items() if not sid]
        if zero_terminal:
            print(f"[WARN] Terminal status IDs are not configured: {zero_terminal}")
        print(f"[INFO] Resolved trigger status IDs: {sorted(self.trigger_status_ids)}")

    def _load_state(self) -> Dict[str, Dict[str, str]]:
        if self.state_path.exists():
            return json.loads(self.state_path.read_text(encoding="utf-8"))
        return {"sheet_status_by_lead": {}}

    def _save_state(self) -> None:
        """Unconditionally write state to disk. Prefer flush_state() for batching."""
        with self.state_lock:
            self.state_path.write_text(
                json.dumps(self.state, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            self._state_dirty = False

    def flush_state(self) -> None:
        """Write state to disk only if it changed since the last save (batching)."""
        if self._state_dirty:
            self._save_state()

    def remember_sheet_status(self, lead_id: str, status_name: str) -> None:
        with self.state_lock:
            self.state.setdefault("sheet_status_by_lead", {})[str(lead_id)] = status_name
            self._state_dirty = True

    def get_known_sheet_status(self, lead_id: str) -> str:
        return self.state.get("sheet_status_by_lead", {}).get(str(lead_id), "")

    def remember_sheet_order_number(self, lead_id: str, order_number: str) -> None:
        with self.state_lock:
            self.state.setdefault("sheet_order_number_by_lead", {})[str(lead_id)] = order_number
            self._state_dirty = True

    def get_known_order_number(self, lead_id: str) -> str:
        return self.state.get("sheet_order_number_by_lead", {}).get(str(lead_id), "")

    # ── Lead lifetime / expiry ────────────────────────────────────────────────
    # When a lead reaches a terminal status we start a countdown. Once the
    # countdown expires every subsequent webhook for that lead is ignored and
    # the lead is removed from the state entirely.
    _EXPIRY_SECONDS: Dict[str, int] = {
        "Успешно": 12 * 3600,   # 12 hours
        "Отказ":   24 * 3600,   # 24 hours
    }

    def remember_lead_expiry(self, lead_id: str, expiry_ts: float) -> None:
        """Record the Unix timestamp at which we should stop tracking this lead."""
        with self.state_lock:
            self.state.setdefault("lead_expiry", {})[str(lead_id)] = expiry_ts
            self._state_dirty = True

    def is_lead_expired(self, lead_id: str) -> bool:
        """Return True if the lead's monitoring window has already passed."""
        expiry = self.state.get("lead_expiry", {}).get(str(lead_id))
        return expiry is not None and time.time() >= expiry

    def forget_lead(self, lead_id: str) -> None:
        """Remove all tracking data for a lead (called when its lifetime ends)."""
        lid = str(lead_id)
        with self.state_lock:
            self.state.get("sheet_status_by_lead",       {}).pop(lid, None)
            self.state.get("sheet_order_number_by_lead", {}).pop(lid, None)
            self.state.get("lead_expiry",                {}).pop(lid, None)
            self._state_dirty = True

    def expire_finished_leads(self) -> None:
        """Purge leads whose monitoring window has elapsed. Called from the worker loop."""
        now = time.time()
        expired = [
            lid for lid, ts in list(self.state.get("lead_expiry", {}).items())
            if now >= ts
        ]
        for lid in expired:
            print(f"[INFO] Lead {lid} monitoring lifetime ended — removing from tracking.")
            self.forget_lead(lid)
        if expired:
            self.flush_state()

    def _set_expiry_for_status(self, lead_id: str, status_display: str) -> None:
        """If status_display has a configured lifetime, start (or overwrite) the countdown."""
        seconds = self._EXPIRY_SECONDS.get(status_display)
        if seconds is not None:
            self.remember_lead_expiry(lead_id, time.time() + seconds)

    def bootstrap_sheet_state(self) -> None:
        rows = self.sheet.iter_lead_statuses()
        for item in rows:
            self.remember_sheet_status(item["lead_id"], item["status"])
            # Snapshot the current order number so we can detect when it gets filled
            self.remember_sheet_order_number(item["lead_id"], item.get("order_number", ""))
        self.flush_state()  # Persist bootstrapped state in one write

    def initial_sync_leads(self, date_from: str, date_to: str) -> None:
        """Fetch all AMO leads created in [date_from, date_to] and upsert them into the sheet.

        Called once on startup when INITIAL_SYNC_DATE_FROM / INITIAL_SYNC_DATE_TO are set.
        Leads already present in the sheet are updated in-place; new ones are appended.
        """
        print(f"[INFO] Initial sync: fetching AMO leads created {date_from} – {date_to} …")
        try:
            leads = self.amo.fetch_leads_by_date_range(date_from, date_to)
        except Exception as exc:
            print(f"[ERROR] Initial sync failed to fetch leads: {exc}")
            return

        print(f"[INFO] Initial sync: {len(leads)} lead(s) returned from AMO.")
        staff_mapping = self.sheet.get_staff_mapping()
        written = 0
        skipped = 0

        for lead in leads:
            lead_id = str(lead.get("id", "")).strip()
            if not lead_id:
                skipped += 1
                continue

            # Enrich with full contact details (phone numbers).
            try:
                lead = self._enrich_lead_contacts(lead)
            except Exception:
                pass

            status_id     = int(lead.get("status_id", 0) or 0)
            pipeline_id   = int(lead.get("pipeline_id", 0) or 0)
            pipeline_name = self.pipeline_id_to_name.get(pipeline_id, "")
            # Skip pipelines not matching the keyword filter (e.g. only "sotuv" pipelines).
            if self.cfg.PIPELINE_KEYWORD and self.cfg.PIPELINE_KEYWORD not in pipeline_name.lower():
                skipped += 1
                continue
            pipeline_display = PIPELINE_DISPLAY_MAP.get(pipeline_name, pipeline_name)

            # Resolve status display name via the same map used by webhooks.
            status_display = self.status_id_to_display_name.get(
                status_id,
                STATUS_DISPLAY_MAP.get(str(status_id), str(status_id)),
            )

            responsible_id   = int(lead.get("responsible_user_id", 0) or 0)
            responsible_name = self.users_map.get(responsible_id, str(responsible_id))

            row = build_row(lead, status_display, pipeline_name, responsible_name, staff_mapping)
            self.sheet.upsert_row(row, pipeline_display)
            self.remember_sheet_status(lead_id, status_display)
            self.remember_sheet_order_number(lead_id, "")
            written += 1

        self.flush_state()  # Persist all initial sync state in one write
        print(f"[INFO] Initial sync complete: {written} written, {skipped} skipped.")

    def check_and_rotate_sheet(self) -> None:
        """Archive the active worksheet when the configured interval rolls over.

        SHEET_ROTATION_INTERVAL controls the granularity:
          - "monthly" (default): rotates on the 1st of each new month.
                State key: YYYY-MM  →  archive tab name: MM.YYYY  (e.g. 02.2026)
          - "hourly": rotates every new clock-hour (for testing purposes).
                State key: YYYY-MM-DD-HH  →  archive tab name: DD.MM.YYYY HH:00
        """
        now = datetime.now()
        interval = self.cfg.SHEET_ROTATION_INTERVAL

        if interval == "hourly":
            current_key = now.strftime("%Y-%m-%d-%H")          # e.g. "2026-02-24-14"
            def _archive_name(key: str) -> str:
                return datetime.strptime(key, "%Y-%m-%d-%H").strftime("%d.%m.%Y %H:00")
            label = "Hour"
        else:
            # Default: monthly
            current_key = now.strftime("%Y-%m")                 # e.g. "2026-02"
            def _archive_name(key: str) -> str:
                return datetime.strptime(key, "%Y-%m").strftime("%m.%Y")
            label = "Month"

        with self.state_lock:
            known_key = self.state.get("active_sheet_month", "")

        if not known_key:
            # First run — record current period, no rotation needed yet
            with self.state_lock:
                self.state["active_sheet_month"] = current_key
            self._save_state()
            print(f"[INFO] Sheet rotation initialised ({interval}): current period = '{current_key}'")
            return

        if known_key == current_key:
            return  # Still within the same period

        # Period has rolled over — archive the old sheet
        try:
            archive_name = _archive_name(known_key)
        except ValueError:
            archive_name = known_key  # Fallback: use raw key as tab name

        print(f"[INFO] {label} changed '{known_key}' → '{current_key}': archiving sheet as '{archive_name}'")
        try:
            self.sheet.rotate_to_archive(archive_name)
            with self.state_lock:
                self.state["active_sheet_month"] = current_key
            self._save_state()
        except Exception as exc:
            print(f"[ERROR] Sheet rotation failed: {exc}")

    def _enrich_lead_contacts(self, lead: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch full contact details (incl. phone) for each contact embedded in lead."""
        contacts = (lead.get("_embedded") or {}).get("contacts") or []
        enriched = []
        for c in contacts:
            cid = c.get("id")
            if not cid:
                enriched.append(c)
                continue
            try:
                full_contact = self.amo.get(f"/api/v4/contacts/{cid}")
                enriched.append(full_contact)
            except Exception:
                enriched.append(c)
        if enriched:
            lead.setdefault("_embedded", {})["contacts"] = enriched
        return lead

    def process_webhook_leads(self, leads: List[Dict[str, Any]]) -> Dict[str, Any]:
        written = 0
        trigger_matches = 0
        terminal_matches = 0
        skipped_no_id = 0
        skipped_duplicate = 0
        skipped_status_mismatch = 0
        skipped_too_old = 0
        seen_status_ids: List[int] = []

        staff_mapping = self.sheet.get_staff_mapping()

        for lead in leads:
            lead_id = str(lead.get("id", "")).strip()
            if not lead_id:
                skipped_no_id += 1
                continue

            webhook_status_id = int(lead.get("status_id", 0) or 0)
            seen_status_ids.append(webhook_status_id)

            # Deduplicate: amoCRM retries webhooks — skip if we already handled this exact event
            if self._is_duplicate_webhook(lead_id, webhook_status_id):
                skipped_duplicate += 1
                continue

            # Skip leads whose monitoring lifetime has ended
            if self.is_lead_expired(lead_id):
                skipped_status_mismatch += 1
                continue

            is_trigger = webhook_status_id in self.trigger_status_ids
            is_terminal = str(webhook_status_id) in self.terminal_status_id_to_name
            known_status = self.get_known_sheet_status(lead_id)

            if not (is_trigger or is_terminal or known_status):
                skipped_status_mismatch += 1
                continue

            # Fetch the absolute latest state from AmoCRM to avoid race conditions
            try:
                full_lead = self.amo.get(f"/api/v4/leads/{lead_id}?with=contacts,companies")
                full_lead = self._enrich_lead_contacts(full_lead)
                status_id = int(full_lead.get("status_id", 0) or 0)
            except Exception:
                full_lead = lead
                status_id = webhook_status_id

            # Skip leads last updated before the configured cutoff (ignores stale history)
            if self.cfg.LEADS_CREATED_AFTER:
                updated_at = int(full_lead.get("updated_at", 0) or 0)
                if updated_at and updated_at < self.cfg.LEADS_CREATED_AFTER:
                    skipped_too_old += 1
                    continue

            # Skip leads from pipelines not matching the keyword filter.
            if self.cfg.PIPELINE_KEYWORD:
                wh_pipeline_id = int(full_lead.get("pipeline_id", 0) or 0)
                wh_pipeline_name = self.pipeline_id_to_name.get(wh_pipeline_id, "")
                if self.cfg.PIPELINE_KEYWORD not in wh_pipeline_name.lower():
                    continue

            if status_id in self.trigger_status_ids:
                trigger_matches += 1
                trigger_display = STATUS_DISPLAY_MAP.get(self.cfg.TRIGGER_STATUS_NAME, self.cfg.TRIGGER_STATUS_NAME)
                pipeline_id = int(full_lead.get("pipeline_id", 0) or 0)
                pipeline_name = self.pipeline_id_to_name.get(pipeline_id, "")
                pipeline_display = PIPELINE_DISPLAY_MAP.get(pipeline_name, pipeline_name)
                
                responsible_id = int(full_lead.get("responsible_user_id", 0) or 0)
                responsible_name = self.users_map.get(responsible_id, str(responsible_id))
                
                current_status_name = self.status_id_to_display_name.get(status_id, trigger_display)
                
                row = build_row(full_lead, current_status_name, pipeline_name, responsible_name, staff_mapping)
                self.sheet.upsert_row(row, pipeline_display)
                self.remember_sheet_status(lead_id, current_status_name)
                # Lead arrives without Заказ №; record empty so we can detect when admin fills it
                self.remember_sheet_order_number(lead_id, "")
                written += 1
                continue

            terminal_name = self.terminal_status_id_to_name.get(str(status_id))
            if terminal_name:
                terminal_matches += 1
                lead_pipeline_id = int(full_lead.get("pipeline_id", 0) or 0)
                p_name = self.pipeline_id_to_name.get(lead_pipeline_id, "")
                p_display = PIPELINE_DISPLAY_MAP.get(p_name, p_name)
                # Apply sheet override: e.g. AMO "Раздумье" → sheet "Отказ"
                sheet_display = AMO_STATUS_TO_SHEET_OVERRIDE.get(terminal_name, terminal_name)
                # When admin filled Заказ № → AMO moved to Заказ отправлен → webhook comes back as
                # "У курера".  Sheet must stay "В процессе" until operator changes it manually.
                if sheet_display == "У курера" and known_status == "В процессе":
                    skipped_status_mismatch += 1
                    continue
                self.sheet.update_status(lead_id, sheet_display, p_display)
                self.remember_sheet_status(lead_id, sheet_display)
                self._set_expiry_for_status(lead_id, sheet_display)
                written += 1
            else:
                if known_status:
                    new_status_display = self.status_id_to_display_name.get(status_id, str(status_id))
                    lead_pipeline_id = int(full_lead.get("pipeline_id", 0) or 0)
                    p_name = self.pipeline_id_to_name.get(lead_pipeline_id, "")
                    p_display = PIPELINE_DISPLAY_MAP.get(p_name, p_name)
                    # Apply sheet override: e.g. AMO "Раздумье" → sheet "Отказ"
                    sheet_display = AMO_STATUS_TO_SHEET_OVERRIDE.get(new_status_display, new_status_display)
                    # Same suppression: Заказ отправлен webhook must not overwrite "В процессе"
                    if sheet_display == "У курера" and known_status == "В процессе":
                        skipped_status_mismatch += 1
                        continue
                    self.sheet.update_status(lead_id, sheet_display, p_display)
                    self.remember_sheet_status(lead_id, sheet_display)
                    self._set_expiry_for_status(lead_id, sheet_display)
                    written += 1
                else:
                    skipped_status_mismatch += 1

        # Flush all state mutations accumulated during this batch in one disk write
        self.flush_state()
        return {
            "received": len(leads),
            "written": written,
            "trigger_matches": trigger_matches,
            "terminal_matches": terminal_matches,
            "skipped_no_id": skipped_no_id,
            "skipped_duplicate": skipped_duplicate,
            "skipped_status_mismatch": skipped_status_mismatch,
            "skipped_too_old": skipped_too_old,
            "seen_status_ids": sorted(list(set(seen_status_ids))),
            "resolved_trigger_status_ids": sorted(self.trigger_status_ids),
            "configured_terminal_status_ids": self.cfg.STATUS_MAP,
        }

    def sync_sheet_to_amo(self) -> None:
        rows = self.sheet.iter_lead_statuses()
        for item in rows:
            lead_id = item["lead_id"]
            status_name = item["status"]
            order_number = item.get("order_number", "")

            # ── Order-number trigger: Заказ № filled by admin → move to Заказ отправлен ──
            known_order = self.get_known_order_number(lead_id)
            # Only act if:
            #  • we have previously tracked this lead (known_order is not None sentinel),
            #  • the order number was empty before, and
            #  • it is now non-empty.
            # Use a sentinel of None (key absent) to mean "never tracked" vs. "" (empty).
            order_was_tracked = str(lead_id) in self.state.get("sheet_order_number_by_lead", {})
            if order_was_tracked and not known_order and order_number:
                try:
                    lead = self.amo.get(f"/api/v4/leads/{lead_id}")
                    lead_pipeline_id = int(lead.get("pipeline_id", 0) or 0)
                    status_id = self.pipeline_status_display_to_id.get(lead_pipeline_id, {}).get(ORDER_NUM_FILLED_AMO_STATUS_DISPLAY)
                    if not status_id:
                        status_id = self.pipeline_status_name_to_id.get(lead_pipeline_id, {}).get("Заказ отправлен")
                    if status_id:
                        self.amo.patch(
                            f"/api/v4/leads/{lead_id}",
                            {
                                "status_id": status_id,
                                "pipeline_id": lead_pipeline_id or self.cfg.PIPELINE_ID,
                                "custom_fields_values": [
                                    {
                                        "field_id": 987889,
                                        "values": [{"value": order_number}],
                                    }
                                ],
                            },
                        )
                        print(f"[INFO] Lead {lead_id}: Заказ № filled ('{order_number}') → set in AMO + moved to Заказ отправлен")
                    else:
                        print(f"[WARN] Lead {lead_id}: Заказ № filled but could not find 'Заказ отправлен' status ID for pipeline {lead_pipeline_id}")
                    # Remember new order number regardless (avoids re-triggering)
                    self.remember_sheet_order_number(lead_id, order_number)
                except Exception as exc:
                    print(f"Failed to move lead {lead_id} to Заказ отправлен: {exc}")

            # ── Status trigger: sheet status changed → push to AMO ──
            if status_name not in self.cfg.STATUS_MAP:
                continue

            known = self.get_known_sheet_status(lead_id)
            if known == status_name:
                continue

            try:
                lead = self.amo.get(f"/api/v4/leads/{lead_id}")
                lead_pipeline_id = int(lead.get("pipeline_id", 0) or 0)

                # Translate the sheet status to the AMO display name we want to target
                amo_lookup = SHEET_STATUS_TO_AMO_DISPLAY.get(status_name, status_name)

                status_id = self.pipeline_status_display_to_id.get(lead_pipeline_id, {}).get(amo_lookup)
                if not status_id:
                    status_id = self.pipeline_status_name_to_id.get(lead_pipeline_id, {}).get(amo_lookup)
                if not status_id:
                    status_id = self.cfg.STATUS_MAP.get(amo_lookup)

                if not status_id:
                    print(f"No status ID mapping for lead {lead_id}, status '{status_name}', pipeline {lead_pipeline_id}")
                    continue

                self.amo.patch(
                    f"/api/v4/leads/{lead_id}",
                    {
                        "status_id": status_id,
                        "pipeline_id": lead_pipeline_id or self.cfg.PIPELINE_ID,
                    },
                )
                self.remember_sheet_status(lead_id, status_name)
            except Exception as exc:
                print(f"Failed to sync sheet->amo for lead {lead_id}: {exc}")
        # One disk write for all status updates in this poll cycle
        self.flush_state()


service = SyncService()
app = FastAPI(title="amoCRM <-> Google Sheets Sync")

# Mount staff KPI dashboard (completely independent from sync logic)
app.include_router(create_dashboard_router(service))


@app.on_event("startup")
def on_startup() -> None:
    # Check for month rollover before bootstrapping state
    service.check_and_rotate_sheet()

    # Run initial date-range sync before bootstrapping sheet state so that
    # leads pulled from AMO are immediately reflected in the local state.
    if service.cfg.INITIAL_SYNC_DATE_FROM and service.cfg.INITIAL_SYNC_DATE_TO:
        try:
            service.initial_sync_leads(
                service.cfg.INITIAL_SYNC_DATE_FROM,
                service.cfg.INITIAL_SYNC_DATE_TO,
            )
        except Exception as exc:
            print(f"[ERROR] Initial date-range sync failed: {exc}")

    # Retry bootstrap on Sheets quota errors (429)
    for attempt in range(1, 6):
        try:
            service.bootstrap_sheet_state()
            break
        except Exception as exc:
            msg = str(exc)
            if "429" in msg or "Quota exceeded" in msg:
                wait = attempt * 30
                print(f"[WARN] Sheets quota hit during bootstrap (attempt {attempt}), retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise

    def worker() -> None:
        backoff = 0
        while True:
            try:
                service.check_and_rotate_sheet()
                service.expire_finished_leads()
                service.sync_sheet_to_amo()
                backoff = 0
            except Exception as exc:
                msg = str(exc)
                print(f"Sheet sync worker error: {msg}")
                if "429" in msg or "Quota exceeded" in msg:
                    backoff = min(backoff + 60, 300)
                    print(f"[WARN] Sheets quota hit — backing off {backoff}s")
                    time.sleep(backoff)
                    continue
            time.sleep(service.cfg.SYNC_POLL_SECONDS)

    t = threading.Thread(target=worker, daemon=True)
    t.start()


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"status": "ok"}


@app.get("/")
def root_health() -> Dict[str, Any]:
    return {"status": "ok", "message": "Use POST /webhook/amocrm for amoCRM webhooks"}


@app.get("/structure")
def structure() -> Dict[str, Any]:
    return service.amo.get("/api/v4/leads/pipelines?with=statuses&limit=250")


@app.get("/leads/custom_fields")
def leads_custom_fields() -> Dict[str, Any]:
    """Return all custom field definitions for leads."""
    return service.amo.get("/api/v4/leads/custom_fields?limit=250")


@app.get("/leads/{lead_id}")
def get_lead(lead_id: int) -> Dict[str, Any]:
    """Return every field AmoCRM exposes for a single lead.

    Embeds: contacts, companies, tags, catalog_elements (linked products).
    Custom fields are returned raw (field_id, field_name, values) so you can
    see every value regardless of whether it is listed in COLUMNS.
    """
    lead = service.amo.get(
        f"/api/v4/leads/{lead_id}?with=contacts,companies,tags,catalog_elements"
    )
    return lead


@app.post("/oauth/exchange")
async def oauth_exchange(payload: Dict[str, str]) -> Dict[str, Any]:
    redirect = payload.get("redirect_url") or payload.get("code") or ""
    if not redirect:
        return {"status": "error", "message": "Pass redirect_url or code"}
    data = service.amo.exchange_code(redirect)
    return {"status": "ok", "token_saved": bool(data.get("access_token"))}


@app.post("/webhook/amocrm")
async def webhook_amocrm(request: Request) -> Dict[str, Any]:
    raw = await request.body()
    content_type = request.headers.get("content-type", "")
    payload = parse_payload(raw, content_type)
    leads = extract_leads(payload)
    result = service.process_webhook_leads(leads)
    return {"status": "ok", **result}


@app.post("/")
async def webhook_root(request: Request) -> Dict[str, Any]:
    return await webhook_amocrm(request)


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("sync_service:app", host=host, port=port, reload=True)
