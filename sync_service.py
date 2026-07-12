import json
import logging
import os
import queue
import re
import threading
import time
from datetime import datetime, timezone, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse
import gspread
import requests
from env_loader import load_env
from fastapi import FastAPI, Request
from gspread.utils import ValidationConditionType
from dashboard_router import create_dashboard_router
from kpi_store import KPIStore

load_env()

# ── Logging setup ─────────────────────────────────────────────────────────────
# Four rotating log files, all capped at 10 MB with 10 backups each:
#   app.log      — everything (mirrors every logger)
#   leads.log    — per-lead lifecycle events (writes, status changes, expiry)
#   webhooks.log — incoming webhook batches and per-lead routing decisions
#   amo_api.log  — every AMO API call: method, URL, HTTP status, duration
#
# Set LOG_DIR in .env to override the default ./logs directory.
# The same messages also print to stdout so systemd journald keeps working.

def _setup_logging() -> None:
    log_dir = Path(os.getenv("LOG_DIR", "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-5s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    def _file_handler(filename: str) -> RotatingFileHandler:
        h = RotatingFileHandler(
            log_dir / filename,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=10,
            encoding="utf-8",
        )
        h.setFormatter(fmt)
        return h

    # Root "amo2gsheet" logger → app.log + stdout
    root = logging.getLogger("amo2gsheet")
    root.setLevel(logging.DEBUG)
    if not root.handlers:  # avoid double-adding on reload
        root.addHandler(_file_handler("app.log"))
        _console = logging.StreamHandler()
        _console.setFormatter(fmt)
        root.addHandler(_console)

    # Child loggers mirror to root (app.log) AND write to their own file
    for name, fname in (
        ("amo2gsheet.leads",    "leads.log"),
        ("amo2gsheet.webhooks", "webhooks.log"),
        ("amo2gsheet.amo_api",  "amo_api.log"),
    ):
        child = logging.getLogger(name)
        child.setLevel(logging.DEBUG)
        if not child.handlers:
            child.addHandler(_file_handler(fname))
        child.propagate = True  # also write to app.log


_setup_logging()

# Module-level logger aliases used throughout this file
_log      = logging.getLogger("amo2gsheet")
_log_lead = logging.getLogger("amo2gsheet.leads")
_log_wh   = logging.getLogger("amo2gsheet.webhooks")
_log_amo  = logging.getLogger("amo2gsheet.amo_api")


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


class SheetIntegrityError(RuntimeError):
    """Raised when a lead worksheet is not safe to read from or write to.

    This exception is deliberately fail-closed: callers must not continue with
    a partial scan or attempt to reconstruct the header in-place.  A misplaced
    header can make the Sheets append API insert at row 1 and can make cached row
    numbers point at a different lead.
    """


class DuplicateLeadIdError(SheetIntegrityError):
    """Raised when an update targets an ID that occurs more than once."""

# ── Normalization for AMO status names ─────────────────────────────────────────
# Some pipelines (e.g. Rushana) have status names with mixed Latin/Cyrillic
# lookalike characters (Latin 'A'→Cyrillic 'А', 'O'→'О', etc.) and non-standard
# casing ('заказ отпрAвлен', 'Отказ', 'думка').  The table below maps the common
# Latin lookalikes to Cyrillic so a case-insensitive comparison works reliably.
_LATIN_TO_CYR = str.maketrans(
    "ABCEHKMOPTXabcehopcx",
    "АВСЕНКМОРТХавсенорсх",
)


def _normalize_amo_name(name: str) -> str:
    """Replace Latin lookalikes with Cyrillic equivalents, then lower-case."""
    return name.translate(_LATIN_TO_CYR).lower()


# Pre-normalized lookup: normalized_key → display value
_STATUS_DISPLAY_NORMALIZED: Dict[str, str] = {
    _normalize_amo_name(k): v for k, v in STATUS_DISPLAY_MAP.items()
}

# Plain ASCII-lowercase fallback: catches mixed-case variants like "Otkaz", "Zakas",
# "заказ без нумерации" — where the letters are not visual lookalikes but just wrong case.
_STATUS_DISPLAY_LOWERED: Dict[str, str] = {
    k.lower(): v for k, v in STATUS_DISPLAY_MAP.items()
}

# AMO display name to target when admin fills in Заказ № on the sheet.
# "Заказ отправлен" maps to display name "У курера" in STATUS_DISPLAY_MAP.
ORDER_NUM_FILLED_AMO_STATUS_DISPLAY = "У курера"

# Maps what the user picks in Google Sheets → the AMO display name used for status ID lookup.
# See SYNC_LOGIC.md for the full rules. Key rules:
#   "У курера"  (sheet) → "Успешно" lookup → resolves to AMO "Успешно реализовано" (won status).
#   "Успешно"   (sheet) → NOT pushed to AMO at all — it is display-only for staff.
#   "Отказ"     (sheet) → AMO pipeline step "ОТКАЗ".
#   "В процессе"(sheet) → AMO ЗАКАЗ БЕЗ НУМЕРАЦИИ (no order#) or ЗАКАЗ ОТПРАВЛЕН (has order#).
SHEET_STATUS_TO_AMO_DISPLAY: Dict[str, str] = {
    "В процессе": "В процессе",
    # When admin sets "У курера" in the sheet the lead is considered delivered —
    # move it to AMO "Успешно реализовано" (the won/closed status).  The lookup key
    # "Успешно" resolves to that status via pipeline_status_display_to_id.
    "У курера":   "Успешно",
    # "Успешно" is intentionally absent — it must never be pushed to AMO.
    # It is a display-only label that staff use to mark their own record keeping.
    # "Отказ" uses the display name (not the raw AMO name "ОТКАЗ"/"OTKAZ") because
    # pipeline_status_display_to_id is keyed by display names.  This resolves
    # correctly for all pipelines regardless of how the step is spelled in AMO.
    "Отказ":      "Отказ",
}

# Maps an AMO display status name → the status that should be written to the Google Sheet
# when a tracked lead receives that AMO status via webhook.
# e.g. when a manager manually sets "Раздумье" in AMO, the sheet row is updated to "Отказ".
# "У курера" maps back to "В процессе": when AMO sends a ЗАКАЗ ОТПРАВЛЕН webhook, the
# sheet row must stay at "В процессе" (the order number was entered, not yet delivered).
AMO_STATUS_TO_SHEET_OVERRIDE: Dict[str, str] = {
    "Раздумье": "Отказ",
    "У курера":  "В процессе",
}

# ── KPI status groupings (used by KPI store event recording) ─────────────────
# Consul: the lead enters the consultation step — this is the "Лид" credit.
KPI_CONSUL_DISPLAY_NAMES: set[str] = {"Консультация"}
# Zakas: ONLY the first confirmed-order stage entry ("Заказ").
# В процессе / У курера / Успешно are downstream progress — NOT new sales.
KPI_ZAKAS_DISPLAY_NAMES: set[str] = {"Заказ"}
# Dumka: the lead is in a "thinking / hesitating" state.
KPI_DUMKA_DISPLAY_NAMES: set[str] = {"Раздумье"}
# Uspeshka: lead was successfully realised (Успешно реализовано).
KPI_USPESHKA_DISPLAY_NAMES: set[str] = {"Успешно реализовано"}
# Otkaz: lead was closed without a sale.
KPI_OTKAZ_FINAL_DISPLAY_NAMES: set[str] = {"Закрыто и не реализовано"}


def _extract_staff_code(lead: Dict[str, Any]) -> str:
    """Extract and normalise Код сотрудника from a lead's custom fields.

    Returns the integer-normalised code string (e.g. '134') or '' if absent.
    """
    for cf in (lead.get("custom_fields_values") or []):
        fname = " ".join((cf.get("field_name") or "").split())
        if fname == "Код сотрудника":
            vals = cf.get("values") or []
            if vals:
                raw = str(vals[0].get("value", "")).strip()
                try:
                    return str(int(raw))
                except ValueError:
                    return ""
    return ""


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
    _log.warning("LEADS_CREATED_AFTER='%s' is not a recognized format. Using 0.", raw)
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
    GOOGLE_SHEET_OWNER_EMAILS = frozenset(
        email.strip().casefold()
        for email in os.getenv("GOOGLE_SHEET_OWNER_EMAILS", "").split(",")
        if email.strip()
    )

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
    WEBHOOK_DEDUP_TTL_SEC = int(os.getenv("WEBHOOK_DEDUP_TTL_SEC", "300"))
    # catch_up_trigger_leads only fetches leads updated within this many days.
    # Keeps the catch-up query focused on recent activity; increase to include older records.
    CATCH_UP_DAYS = int(os.getenv("CATCH_UP_DAYS", "3"))
    # Leads created before this timestamp are silently ignored (0 = process all).
    # Supports human-readable 'DD.MM.YYYY HH:MM:SS' (UTC) or plain Unix timestamp.
    LEADS_CREATED_AFTER = _parse_leads_created_after(os.getenv("LEADS_CREATED_AFTER", "0"))
    # Set BOOTSTRAP_RECOVERY=false to skip pushing missed Заказ № values on startup.
    BOOTSTRAP_RECOVERY = os.getenv("BOOTSTRAP_RECOVERY", "true").strip().lower() not in ("false", "0", "no")
    # Only process leads from pipelines whose name contains this keyword (case-insensitive).
    # New pipelines matching the keyword are picked up automatically. Empty = all pipelines.
    PIPELINE_KEYWORD = os.getenv("PIPELINE_KEYWORD", "").strip().lower()
    # Hours offset from UTC used when formatting timestamps for display in the Sheet.
    # Uzbekistan / Tashkent = 5 (UTC+5).  Set to 0 for UTC, 3 for Moscow, etc.
    DISPLAY_TZ_OFFSET = float(os.getenv("DISPLAY_TZ_OFFSET", "5"))


def require_env() -> None:
    required = [
        "AMO_SUBDOMAIN",
        "AMO_CLIENT_ID",
        "AMO_CLIENT_SECRET",
        "AMO_REDIRECT_URI",
        "GOOGLE_SHEET_ID",
        "GOOGLE_SHEET_OWNER_EMAILS",
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")


def _ts_to_date(ts, include_time: bool = False) -> str:
    """Convert a Unix timestamp to a display string in the configured timezone.

    Uses Config.DISPLAY_TZ_OFFSET (hours from UTC, default 5 = Tashkent/UTC+5).
    Returns '' for falsy or zero values.
    """
    if not ts:
        return ""
    try:
        ts_int = int(float(ts))
        if ts_int == 0:
            return ""
        tz = timezone(timedelta(hours=Config.DISPLAY_TZ_OFFSET))
        dt = datetime.fromtimestamp(ts_int, tz=tz)
        return dt.strftime("%d.%m.%Y %H:%M") if include_time else dt.strftime("%d.%m.%Y")
    except Exception:
        return ""


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
            t0 = time.monotonic()
            r = requests.request(method, url, headers=headers, timeout=30, **kwargs)
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            # Strip base URL for brevity in logs
            short_url = url.replace(self.base_url, "")
            if r.status_code == 429:
                wait = min(attempt * 10, 60)
                _log_amo.warning(
                    "AMO 429 %s %s — retrying in %ds (attempt %d/5)",
                    method, short_url, wait, attempt,
                )
                time.sleep(wait)
                continue
            _log_amo.debug("%s %s → %d (%dms)", method, short_url, r.status_code, elapsed_ms)
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
        # Re-use cached token for up to 23 hours — AMO tokens are valid for 24 h.
        # The refresh_token path handles the rare case of an expired token.
        now = time.time()
        if self._cached_access_token and now - self._token_validated_ts < 82800:
            return self._cached_access_token

        tokens = self._token_data()
        access_token = tokens.get("access_token", "")
        refresh_token = tokens.get("refresh_token", "")

        if self._is_token_valid(access_token):
            self._cached_access_token = access_token
            self._token_validated_ts = now
            return access_token
        if not refresh_token and self.cfg.AMO_AUTH_CODE:
            _log.info("No refresh token found, trying AMO_AUTH_CODE bootstrap...")
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
            _log_amo.error("GET %s failed: %d %s", endpoint, r.status_code, r.text[:200])
            raise RuntimeError(f"GET {endpoint} failed: {r.status_code} {r.text}")
        return r.json()

    def batch_get_leads(self, lead_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        """Fetch multiple leads in one request. Returns {lead_id: lead_data}.

        Uses filter[id][] to batch up to 50 leads per request, reducing per-lead
        GET calls from N down to ceil(N/50).  Any chunk that fails is logged and
        its IDs are simply absent from the result — callers fall back to the
        original webhook payload for those leads.
        """
        if not lead_ids:
            return {}
        result: Dict[int, Dict[str, Any]] = {}
        CHUNK = 50
        for i in range(0, len(lead_ids), CHUNK):
            chunk = lead_ids[i : i + CHUNK]
            ids_param = "&".join(f"filter[id][]={lid}" for lid in chunk)
            try:
                data = self.get(
                    f"/api/v4/leads?{ids_param}&with=contacts,companies&limit={CHUNK}"
                )
                for lead in (data.get("_embedded") or {}).get("leads") or []:
                    result[int(lead["id"])] = lead
            except Exception as exc:
                _log_amo.error(
                    "batch_get_leads chunk [%s] failed: %s",
                    ",".join(map(str, chunk)), exc,
                )
        return result

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
        _log_amo.debug("PATCH %s  body=%s", endpoint, json.dumps(body, ensure_ascii=False)[:300])
        r = self._api_request(
            "PATCH",
            f"{self.base_url}{endpoint}",
            {**self._headers(token), "Content-Type": "application/json"},
            json=body,
        )
        if r.status_code >= 400:
            _log_amo.error("PATCH %s failed: %d %s", endpoint, r.status_code, r.text[:200])
            raise RuntimeError(f"PATCH {endpoint} failed: {r.status_code} {r.text}")
        return r.json() if r.text else {}


class SheetSync:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.gc = gspread.service_account(filename=cfg.GOOGLE_SERVICE_ACCOUNT_FILE)
        google_credentials = (
            getattr(self.gc, "auth", None)
            or getattr(getattr(self.gc, "http_client", None), "auth", None)
        )
        self._service_account_email = str(
            getattr(google_credentials, "service_account_email", "")
            or ""
        ).strip()
        if not self._service_account_email:
            try:
                credentials_data = json.loads(
                    Path(cfg.GOOGLE_SERVICE_ACCOUNT_FILE).read_text(encoding="utf-8")
                )
                self._service_account_email = str(
                    credentials_data.get("client_email", "")
                ).strip()
            except Exception:
                pass
        if not self._service_account_email:
            raise RuntimeError(
                "Google service-account email is required for strict sheet protection"
            )
        self._trusted_protection_editor_users = {
            self._service_account_email.casefold(),
            *{
                str(email).strip().casefold()
                for email in getattr(cfg, "GOOGLE_SHEET_OWNER_EMAILS", set())
                if str(email).strip()
            },
        }
        self.spreadsheet = self.gc.open_by_key(cfg.GOOGLE_SHEET_ID)
        # A re-entrant lock protects worksheet mutations and every read/replace of
        # the row-index caches.  Several public methods call locked helpers, so a
        # plain Lock would deadlock while an unlocked helper would allow a stale
        # scanner result to replace a newer writer's index.
        self.lock = threading.RLock()
        # Cache of worksheet objects keyed by tab name
        self._sheets: Dict[str, Any] = {}
        # Staff mapping cache – refreshed every STAFF_CACHE_TTL_SEC seconds
        self._staff_cache: Dict[str, str] = {}
        self._staff_cache_ts: float = 0.0
        # In-memory row index: ws_name → {lead_id → 1-based row number}
        # Writers rebuild it from a live validated snapshot before mutating; this
        # favors correctness over saving one Sheets read.
        self._row_index: Dict[str, Dict[str, int]] = {}
        self._row_count: Dict[str, int] = {}  # ws_name → last occupied row number
        self._duplicate_ids: Dict[str, set[str]] = {}
        self._row_snapshots: Dict[str, List[Any]] = {}
        # Recently verified writes survive a temporarily stale get_all_values()
        # response.  ws_name → lead_id → {row, timestamp}.
        self._recent_verified_rows: Dict[str, Dict[str, Dict[str, Any]]] = {}
        # Cache of worksheet titles — refreshed at most once every 120 s to avoid
        # a metadata fetch on every poll cycle.
        self._ws_titles_cache: List[str] = []
        self._ws_titles_ts: float = 0.0

    def _live_sheet_properties(self, ws) -> Dict[str, Any]:
        """Return live title/freeze metadata, not cached Worksheet properties."""
        fetch_metadata = getattr(getattr(ws, "client", None), "fetch_sheet_metadata", None)
        if not callable(fetch_metadata):
            # Compatibility fallback for test doubles and older gspread clients.
            # Production gspread exposes fetch_sheet_metadata, so production always
            # checks the live value from Google before processing lead rows.
            return {
                "title": str(getattr(ws, "title", "")),
                "frozen_rows": int(getattr(ws, "frozen_row_count", 0) or 0),
                "sheet_protected": False,
            }
        try:
            metadata = fetch_metadata(
                ws.spreadsheet_id, params={"includeGridData": "false"}
            )
            for sheet in metadata.get("sheets", []):
                props = sheet.get("properties", {})
                if int(props.get("sheetId", -1)) == int(ws.id):
                    sheet_protected = False
                    for protected in sheet.get("protectedRanges", []):
                        protected_range = protected.get("range") or {}
                        if int(protected_range.get("sheetId", -1)) != int(ws.id):
                            continue
                        protects_whole_sheet = (
                            int(protected_range.get("startRowIndex", 0) or 0) == 0
                            and int(protected_range.get("startColumnIndex", 0) or 0) == 0
                            and "endRowIndex" not in protected_range
                            and "endColumnIndex" not in protected_range
                        )
                        unprotected_ranges = protected.get(
                            "unprotectedRanges", []
                        )
                        unprotected_columns = set()
                        unprotected_shape_valid = True
                        for allowed in unprotected_ranges:
                            if (
                                int(allowed.get("sheetId", -1)) != int(ws.id)
                                or int(allowed.get("startRowIndex", -1)) != 1
                                or "endRowIndex" in allowed
                            ):
                                unprotected_shape_valid = False
                                break
                            unprotected_columns.add((
                                int(allowed.get("startColumnIndex", -1)),
                                int(allowed.get("endColumnIndex", -1)),
                            ))
                        expected_unprotected_columns = {
                            (ORDER_NUM_COL_INDEX, ORDER_NUM_COL_INDEX + 1),
                            (STATUS_COL_INDEX, STATUS_COL_INDEX + 1),
                        }
                        editors = protected.get("editors") or {}
                        editor_users = {
                            str(user).strip().casefold()
                            for user in editors.get("users", [])
                            if str(user).strip()
                        }
                        # Google includes spreadsheet owners in protected-range
                        # metadata even when addProtectedRange names only the
                        # service account.  Trust only the configured owner(s),
                        # never an arbitrary second user.
                        editors_are_strict = (
                            editor_users
                            == self._trusted_protection_editor_users
                            and not editors.get("groups")
                            and not editors.get("domainUsersCanEdit", False)
                        )
                        if (
                            protects_whole_sheet
                            and not protected.get("warningOnly", False)
                            and unprotected_shape_valid
                            and len(unprotected_ranges) == 2
                            and unprotected_columns == expected_unprotected_columns
                            and editors_are_strict
                        ):
                            sheet_protected = True
                            break
                    return {
                        "title": str(props.get("title", "")),
                        "frozen_rows": int(
                            (props.get("gridProperties") or {}).get(
                                "frozenRowCount", 0
                            ) or 0
                        ),
                        "sheet_protected": sheet_protected,
                    }
        except Exception as exc:
            raise SheetIntegrityError(
                f"Could not verify frozen header for worksheet '{ws.title}': {exc}"
            ) from exc
        raise SheetIntegrityError(
            f"Could not find worksheet '{ws.title}' in live spreadsheet metadata"
        )

    def _assert_sheet_integrity_locked(
        self, ws, all_vals: List[Any], expected_title: str = ""
    ) -> None:
        """Validate the exact lead header and one frozen row, or stop safely."""
        actual_header = list(all_vals[0]) if all_vals else []
        if actual_header != COLUMNS:
            _log_lead.critical(
                "SHEET INTEGRITY BLOCKED tab='%s': row 1 is not the exact header; "
                "expected=%s actual=%s. No rows will be read or written.",
                ws.title, COLUMNS, actual_header,
            )
            raise SheetIntegrityError(
                f"Worksheet '{ws.title}' row 1 is not the exact expected header"
            )

        embedded_header_rows = [
            row_number
            for row_number, row in enumerate(all_vals[1:], start=2)
            if list(row[:len(COLUMNS)]) == COLUMNS
        ]
        if embedded_header_rows:
            _log_lead.critical(
                "SHEET INTEGRITY BLOCKED tab='%s': duplicate header found in "
                "data row(s) %s. No rows will be read or written.",
                ws.title, embedded_header_rows,
            )
            raise SheetIntegrityError(
                f"Worksheet '{ws.title}' contains a header inside its data rows: "
                f"{embedded_header_rows}"
            )

        live_props = self._live_sheet_properties(ws)
        live_title = live_props["title"]
        expected_title = expected_title or ws.title
        if live_title != expected_title:
            _log_lead.critical(
                "SHEET INTEGRITY BLOCKED: cached tab='%s' sheetId=%s is now "
                "named '%s'. No rows will be read or written.",
                expected_title, ws.id, live_title,
            )
            raise SheetIntegrityError(
                f"Worksheet '{expected_title}' now points to live title "
                f"'{live_title}' (sheetId={ws.id})"
            )

        if not live_props.get("sheet_protected", False):
            _log_lead.critical(
                "SHEET INTEGRITY BLOCKED tab='%s': strict whole-sheet protection "
                "with only C2:C and U2:U editable is missing. No rows will be "
                "read or written.",
                ws.title,
            )
            raise SheetIntegrityError(
                f"Worksheet '{ws.title}' does not have the required structural protection"
            )

        frozen_rows = int(live_props["frozen_rows"])
        if frozen_rows != 1:
            # Freezing/unfreezing changes metadata only and cannot overwrite lead
            # data.  Restore the invariant automatically, then verify it live.
            try:
                ws.freeze(rows=1)
                frozen_rows = int(
                    self._live_sheet_properties(ws)["frozen_rows"]
                )
            except Exception as exc:
                raise SheetIntegrityError(
                    f"Could not restore frozen header for '{ws.title}': {exc}"
                ) from exc
            if frozen_rows != 1:
                _log_lead.critical(
                    "SHEET INTEGRITY BLOCKED tab='%s': could not restore exactly "
                    "1 frozen header row (found %d). No rows will be read or written.",
                    ws.title, frozen_rows,
                )
                raise SheetIntegrityError(
                    f"Worksheet '{ws.title}' must have exactly one frozen header row"
                )
            _log_lead.warning(
                "SHEET HEADER FREEZE restored automatically for tab='%s'", ws.title
            )

    def _read_validated_values_locked(self, ws) -> List[List[str]]:
        """Read a worksheet and validate its live safety invariants."""
        all_vals = ws.get_all_values()
        self._assert_sheet_integrity_locked(ws, all_vals)
        return all_vals

    def _initialize_lead_sheet_locked(self, ws) -> None:
        """Initialize a newly-created (and therefore empty) lead worksheet."""
        last_col = chr(ord("A") + len(COLUMNS) - 1)
        ws.update(values=[COLUMNS], range_name=f"A1:{last_col}1")
        ws.freeze(rows=1)
        try:
            ws.client.batch_update(
                ws.spreadsheet_id,
                {
                    "requests": [
                        {
                            "addProtectedRange": {
                                "protectedRange": {
                                    "range": {
                                        "sheetId": ws.id,
                                    },
                                    "unprotectedRanges": [
                                        {
                                            "sheetId": ws.id,
                                            "startRowIndex": 1,
                                            "startColumnIndex": ORDER_NUM_COL_INDEX,
                                            "endColumnIndex": ORDER_NUM_COL_INDEX + 1,
                                        },
                                        {
                                            "sheetId": ws.id,
                                            "startRowIndex": 1,
                                            "startColumnIndex": STATUS_COL_INDEX,
                                            "endColumnIndex": STATUS_COL_INDEX + 1,
                                        },
                                    ],
                                    "description": (
                                        "amo2gsheet structural lock; operators may "
                                        "edit only order number and status"
                                    ),
                                    "warningOnly": False,
                                    "editors": {
                                        "users": [self._service_account_email],
                                    },
                                }
                            }
                        }
                    ]
                },
            )
        except Exception as exc:
            raise SheetIntegrityError(
                f"Could not structurally protect new worksheet '{ws.title}': {exc}"
            ) from exc
        self._invalidate_row_index(ws.title, reset_high_water=True)
        # Verify both writes reached Google before the worksheet becomes usable.
        self._read_validated_values_locked(ws)

    def _get_or_create_sheet(self, name: str):
        with self.lock:
            if name in self._sheets:
                return self._sheets[name]
            # Always re-fetch spreadsheet metadata first to avoid stale cache issues.
            self.spreadsheet = self.gc.open_by_key(self.cfg.GOOGLE_SHEET_ID)
            created = False
            try:
                ws = self.spreadsheet.worksheet(name)
            except gspread.WorksheetNotFound:
                ws = self.spreadsheet.add_worksheet(
                    title=name, rows=2000, cols=max(26, len(COLUMNS))
                )
                created = True

            # Only lead worksheets have the strict schema.  Staff has a different
            # header and must not be validated against COLUMNS.
            if name == self.cfg.GOOGLE_WORKSHEET_NAME:
                if created:
                    self._initialize_lead_sheet_locked(ws)
                else:
                    self._read_validated_values_locked(ws)
                # Always clean up stale validation rules from old column positions
                # and re-apply the dropdown only to the correct status column.
                self._fix_sheet_validation(ws)

            self._sheets[name] = ws
            return ws

    def _get_or_create_month_sheet(
        self, tab_name: str, allow_create: bool = False
    ):
        """Return (and lazily create) the worksheet for the given month tab.

        Tab names are typically "MM.YYYY" (e.g. "03.2026").  The sheet is
        created with column headers, a frozen header row, and a status-column
        dropdown when it does not yet exist.
        """
        with self.lock:
            if tab_name in self._sheets:
                return self._sheets[tab_name]
            # Re-fetch spreadsheet metadata to avoid stale cache issues.
            self.spreadsheet = self.gc.open_by_key(self.cfg.GOOGLE_SHEET_ID)
            created = False
            try:
                ws = self.spreadsheet.worksheet(tab_name)
            except gspread.WorksheetNotFound as exc:
                if not allow_create:
                    raise SheetIntegrityError(
                        f"Expected worksheet '{tab_name}' is missing; refusing to "
                        "create an empty replacement during normal sync"
                    ) from exc
                ws = self.spreadsheet.add_worksheet(
                    title=tab_name, rows=2000, cols=max(26, len(COLUMNS))
                )
                created = True
            if created:
                self._initialize_lead_sheet_locked(ws)
            else:
                self._read_validated_values_locked(ws)
            # Always clean up stale validation rules from old column positions.
            self._fix_sheet_validation(ws)
            self._sheets[tab_name] = ws
            return ws

    def rotate_to_archive(self, archive_tab_name: str) -> None:
        """Rename the current active worksheet to archive_tab_name, then create a
        fresh worksheet with the default name so new-month leads start on a clean tab.
        """
        with self.lock:
            main_name = self.cfg.GOOGLE_WORKSHEET_NAME
            # Rename the existing active sheet to the archive name
            try:
                ws = self.spreadsheet.worksheet(main_name)
                self._read_validated_values_locked(ws)
                ws.update_title(archive_tab_name)
                _log.info("Worksheet '%s' renamed to '%s'", main_name, archive_tab_name)
            except gspread.WorksheetNotFound:
                _log.warning("Worksheet '%s' not found during rotation — skipping rename", main_name)
            # Clear the sheet cache so the renamed tab is no longer served as the active sheet
            self._sheets.pop(main_name, None)
            self._sheets.pop(archive_tab_name, None)
            # Invalidate row indices for both old and new tab names
            self._invalidate_row_index(main_name, reset_high_water=True)
            self._invalidate_row_index(archive_tab_name, reset_high_water=True)
            # Create (or re-open) a new active sheet with headers + dropdown
            self._get_or_create_sheet(main_name)
            _log.info("New active worksheet '%s' created for the new month.", main_name)

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
            _log.warning("Could not load Staff sheet: %s", e)
            return self._staff_cache  # Return stale cache on error rather than empty

    # Statuses that can be chosen from the dropdown in the "Статус" column
    STATUS_DROPDOWN_OPTIONS = ["В процессе", "У курера", "Успешно", "Отказ"]

    def _clear_column_validation(self, ws, row_range: str) -> None:
        """Remove any data-validation rule from *row_range*.

        Used to wipe stale dropdown rules left on columns that previously held
        the status column before new columns were inserted in front of it.
        """
        try:
            body = {
                "requests": [
                    {
                        "setDataValidation": {
                            "range": __import__('gspread').utils.a1_range_to_grid_range(
                                row_range, ws.id
                            ),
                            # Omitting 'rule' clears any existing validation on the range.
                        }
                    }
                ]
            }
            ws.client.batch_update(ws.spreadsheet_id, body)
        except Exception as e:
            _log.warning("Could not clear validation on %s: %s", row_range, e)

    def _fix_sheet_validation(self, ws) -> None:
        """Ensure the status dropdown exists ONLY on the status column.

        Clears any validation rules on all other columns in the data area
        (rows 2-2000) so that stale dropdown rules left from a previous column
        layout (e.g. before Продажа в рассрочку / Воронка were added) are
        removed.  Then (re-)applies the dropdown to the correct status column.
        """
        status_col_letter = chr(ord("A") + STATUS_COL_INDEX)
        # Clear every column in the sheet except the status column.
        for i in range(len(COLUMNS)):
            if i == STATUS_COL_INDEX:
                continue
            col_letter = chr(ord("A") + i)
            self._clear_column_validation(ws, f"{col_letter}2:{col_letter}2000")
        # (Re-)apply the correct dropdown on the status column.
        self._apply_status_dropdown(ws, f"{status_col_letter}2:{status_col_letter}2000")

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
            _log.warning("Could not set dropdown validation on %s: %s", row_range, e)

    def _all_rows(self, ws) -> List[List[str]]:
        values = ws.get_all_values()
        if not values:
            return []
        return values[1:]

    # ── Row index cache ───────────────────────────────────────────────────────
    # All cache replacements and sheet mutations happen under self.lock.

    def _refresh_row_index_locked(
        self, ws, ws_name: str, all_vals: Optional[List[Any]] = None
    ) -> Dict[str, int]:
        """Atomically replace the row index from one validated sheet snapshot.

        Duplicate IDs are intentionally excluded from the index.  The bot never
        guesses which duplicate is authoritative and never deletes either row.
        """
        if all_vals is None:
            all_vals = self._read_validated_values_locked(ws)
        else:
            self._assert_sheet_integrity_locked(ws, all_vals, ws_name)

        occurrences: Dict[str, List[int]] = {}
        last_data_row = 1  # a valid worksheet always has the header at row 1
        for i, row in enumerate(all_vals):
            if any(str(cell).strip() for cell in row):
                last_data_row = i + 1
            if i == 0 or len(row) <= ID_COL_INDEX:
                continue
            lead_id = str(row[ID_COL_INDEX]).strip()
            if lead_id:
                occurrences.setdefault(lead_id, []).append(i + 1)

        duplicates = {lead_id for lead_id, rows in occurrences.items() if len(rows) > 1}
        previous_duplicates = self._duplicate_ids.get(ws_name, set())
        if duplicates and duplicates != previous_duplicates:
            details = {
                lead_id: occurrences[lead_id] for lead_id in sorted(duplicates)
            }
            _log_lead.critical(
                "SHEET DUPLICATE IDs tab='%s': %s. These IDs are quarantined; "
                "no Sheet or AMO updates will be made for them.",
                ws_name, details,
            )

        idx = {
            lead_id: rows[0]
            for lead_id, rows in occurrences.items()
            if lead_id not in duplicates
        }
        self._row_index[ws_name] = idx
        self._row_snapshots[ws_name] = all_vals
        # Never lower the observed high-water mark inside a running process.  A
        # Sheets read can briefly omit a just-written trailing row; retaining the
        # mark helps recent-write recovery and diagnostics remain conservative.
        self._row_count[ws_name] = max(
            self._row_count.get(ws_name, 1), last_data_row
        )
        self._duplicate_ids[ws_name] = duplicates
        return idx

    def _get_row_index(self, ws, ws_name: str) -> Dict[str, int]:
        with self.lock:
            if ws_name not in self._row_index:
                self._refresh_row_index_locked(ws, ws_name)
            return self._row_index[ws_name]

    def _invalidate_row_index(
        self, ws_name: str, reset_high_water: bool = False
    ) -> None:
        """Discard cached index so it is rebuilt on next access."""
        with self.lock:
            self._row_index.pop(ws_name, None)
            self._row_snapshots.pop(ws_name, None)
            self._duplicate_ids.pop(ws_name, None)
            if reset_high_water:
                self._row_count.pop(ws_name, None)
                self._recent_verified_rows.pop(ws_name, None)

    def _remember_verified_row_locked(
        self, ws_name: str, lead_id: str, row_num: int
    ) -> None:
        self._recent_verified_rows.setdefault(ws_name, {})[lead_id] = {
            "row": row_num,
            "timestamp": time.monotonic(),
        }

    def _recover_recent_row_locked(
        self, ws, ws_name: str, lead_id: str
    ) -> Optional[int]:
        """Recover a row hidden by short-lived Sheets read propagation lag."""
        recent = self._recent_verified_rows.get(ws_name, {}).get(lead_id)
        if not recent:
            return None
        age = time.monotonic() - float(recent.get("timestamp", 0.0))
        if age > 300:
            self._recent_verified_rows.get(ws_name, {}).pop(lead_id, None)
            return None
        row_num = int(recent["row"])
        actual_id = str(ws.cell(row_num, ID_COL_INDEX + 1).value or "").strip()
        if actual_id == lead_id:
            self._row_index.setdefault(ws_name, {})[lead_id] = row_num
            self._row_count[ws_name] = max(
                self._row_count.get(ws_name, 1), row_num
            )
            return row_num
        if not actual_id:
            # Do not create a second row while Google's point read has not yet
            # confirmed whether the successful write is visible.
            raise SheetIntegrityError(
                f"Recent verified write for lead {lead_id} at '{ws_name}' row "
                f"{row_num} is temporarily not visible; refusing duplicate insert"
            )
        self._recent_verified_rows.get(ws_name, {}).pop(lead_id, None)
        return None

    def recent_lead_ids(self) -> set[str]:
        """IDs recently written successfully, for deletion-scan grace."""
        cutoff = time.monotonic() - 300
        with self.lock:
            visible: set[str] = set()
            for ws_name, leads in self._recent_verified_rows.items():
                expired = [
                    lead_id for lead_id, item in leads.items()
                    if float(item.get("timestamp", 0.0)) < cutoff
                ]
                for lead_id in expired:
                    leads.pop(lead_id, None)
                visible.update(leads)
            return visible

    def _verified_row_for_lead_locked(
        self, ws, ws_name: str, lead_id: str, row_idx: Dict[str, int]
    ) -> Optional[int]:
        """Return a row only after column B still contains the requested ID."""
        if lead_id in self._duplicate_ids.get(ws_name, set()):
            raise DuplicateLeadIdError(
                f"Lead {lead_id} occurs more than once in worksheet '{ws_name}'"
            )
        row_num = row_idx.get(lead_id)
        if not row_num:
            return None

        actual_id = str(ws.cell(row_num, ID_COL_INDEX + 1).value or "").strip()
        if actual_id == lead_id:
            return row_num

        # A user changed row positions after our snapshot.  Refresh once under the
        # same lock and verify again; never write through a stale row number.
        _log_lead.warning(
            "SHEET ROW MOVED tab='%s' lead=%s cached_row=%d now_contains=%s; "
            "refreshing index before update",
            ws_name, lead_id, row_num, actual_id or "(blank)",
        )
        row_idx = self._refresh_row_index_locked(ws, ws_name)
        if lead_id in self._duplicate_ids.get(ws_name, set()):
            raise DuplicateLeadIdError(
                f"Lead {lead_id} occurs more than once in worksheet '{ws_name}'"
            )
        row_num = row_idx.get(lead_id)
        if not row_num:
            return None
        actual_id = str(ws.cell(row_num, ID_COL_INDEX + 1).value or "").strip()
        if actual_id != lead_id:
            raise SheetIntegrityError(
                f"Refusing update for lead {lead_id}: worksheet '{ws_name}' "
                f"row {row_num} contains ID '{actual_id}'"
            )
        return row_num

    def find_row(self, ws, lead_id: str) -> Optional[int]:
        """Find and live-verify a lead row under the row-index lock."""
        lead_id = str(lead_id).strip()
        with self.lock:
            row_idx = self._refresh_row_index_locked(ws, ws.title)
            return self._verified_row_for_lead_locked(
                ws, ws.title, lead_id, row_idx
            )

    @staticmethod
    def _append_cell_data(value: Any) -> Dict[str, Any]:
        """Encode a Python value for Sheets API appendCells."""
        if value is None or value == "":
            return {}
        if isinstance(value, bool):
            extended_value = {"boolValue": value}
        elif isinstance(value, (int, float)):
            extended_value = {"numberValue": value}
        else:
            extended_value = {"stringValue": str(value)}
        return {"userEnteredValue": extended_value}

    def upsert_row(self, row_data: List[Any], tab_name: str) -> int:
        if len(row_data) != len(COLUMNS):
            raise ValueError(
                f"Expected {len(COLUMNS)} sheet columns, got {len(row_data)}"
            )
        lead_id = str(row_data[ID_COL_INDEX]).strip()
        if not lead_id:
            raise ValueError("Cannot write a sheet row without a lead ID")

        with self.lock:
            ws = self._get_or_create_month_sheet(tab_name)
            ws_name = ws.title
            # Always refresh immediately before a mutation.  This is intentionally
            # a fresh Sheets read, not a merge with stale in-memory entries.
            row_idx = self._refresh_row_index_locked(ws, ws_name)
            row_num = self._verified_row_for_lead_locked(
                ws, ws_name, lead_id, row_idx
            )
            if not row_num:
                row_num = self._recover_recent_row_locked(
                    ws, ws_name, lead_id
                )
            if row_num:
                snapshot = self._row_snapshots.get(ws_name, [])
                if row_num <= len(snapshot):
                    current_row = snapshot[row_num - 1]
                    row_data[ORDER_NUM_COL_INDEX] = (
                        current_row[ORDER_NUM_COL_INDEX]
                        if len(current_row) > ORDER_NUM_COL_INDEX else ""
                    )
                    row_data[STATUS_COL_INDEX] = (
                        current_row[STATUS_COL_INDEX]
                        if len(current_row) > STATUS_COL_INDEX else ""
                    )
                else:
                    # Only possible during a short-lived stale bulk read after a
                    # verified write; use targeted reads rather than guessing.
                    row_data[ORDER_NUM_COL_INDEX] = str(
                        ws.cell(row_num, ORDER_NUM_COL_INDEX + 1).value or ""
                    )
                    row_data[STATUS_COL_INDEX] = str(
                        ws.cell(row_num, STATUS_COL_INDEX + 1).value or ""
                    )
                # Never rewrite identity (B), operator-owned Заказ № (C), or
                # operator-owned Статус (U) on a retry.  In addition to
                # preserving manual input, excluding B means a concurrent structural
                # shift cannot be hidden by our own write; the post-check will still
                # see the other lead's ID and stop.
                ws.batch_update(
                    [
                        {
                            "range": f"A{row_num}:A{row_num}",
                            "values": [[row_data[0]]],
                        },
                        {
                            "range": f"D{row_num}:T{row_num}",
                            "values": [row_data[3:STATUS_COL_INDEX]],
                        },
                    ],
                    value_input_option="USER_ENTERED",
                )
                written_id = str(
                    ws.cell(row_num, ID_COL_INDEX + 1).value or ""
                ).strip()
                if written_id != lead_id:
                    self._invalidate_row_index(ws_name)
                    raise SheetIntegrityError(
                        f"Sheet update verification failed for lead {lead_id} at "
                        f"'{ws_name}' row {row_num}"
                    )
                _log_lead.info(
                    "SHEET UPDATE row=%d lead=%s tab='%s'", row_num, lead_id, ws_name
                )
                self._remember_verified_row_locked(
                    ws_name, lead_id, row_num
                )
                return row_num

            # appendCells is one atomic Sheets batchUpdate operation that appends
            # after the sheet's actual last data row.  Unlike values.append it has
            # no table-anchor detection and no INSERT_ROWS mode, so it cannot
            # choose row 1 or overwrite a checked-but-shifted target row.
            ws.client.batch_update(
                ws.spreadsheet_id,
                {
                    "requests": [
                        {
                            "appendCells": {
                                "sheetId": ws.id,
                                "rows": [
                                    {
                                        "values": [
                                            self._append_cell_data(value)
                                            for value in row_data
                                        ]
                                    }
                                ],
                                "fields": "userEnteredValue",
                            }
                        }
                    ]
                },
            )

            # Verify from a new authoritative snapshot.  Google batchUpdate is
            # synchronous, but retry a short propagation delay without appending
            # again.  The ID must occur exactly once before state is updated.
            actual_row: Optional[int] = None
            for verify_attempt in range(3):
                refreshed_values = ws.get_all_values()
                refreshed_idx = self._refresh_row_index_locked(
                    ws, ws_name, refreshed_values
                )
                if lead_id in self._duplicate_ids.get(ws_name, set()):
                    raise DuplicateLeadIdError(
                        f"Atomic append produced/found duplicate lead {lead_id} "
                        f"in worksheet '{ws_name}'"
                    )
                actual_row = refreshed_idx.get(lead_id)
                if actual_row:
                    break
                if verify_attempt < 2:
                    time.sleep(0.2 * (verify_attempt + 1))
            if not actual_row:
                self._invalidate_row_index(ws_name)
                raise SheetIntegrityError(
                    f"Atomic append completed but lead {lead_id} was not visible "
                    f"in authoritative refreshes for worksheet '{ws_name}'"
                )
            written_id = str(
                ws.cell(actual_row, ID_COL_INDEX + 1).value or ""
            ).strip()
            if written_id != lead_id:
                self._invalidate_row_index(ws_name)
                raise SheetIntegrityError(
                    f"Atomic append verification failed for lead {lead_id} at "
                    f"'{ws_name}' row {actual_row}"
                )

            self._remember_verified_row_locked(
                ws_name, lead_id, actual_row
            )
            _log_lead.info(
                "SHEET INSERT row=%d lead=%s tab='%s'", actual_row, lead_id, ws_name
            )
            status_col_letter = chr(ord("A") + STATUS_COL_INDEX)
            self._apply_status_dropdown(
                ws,
                f"{status_col_letter}{actual_row}:{status_col_letter}{actual_row}",
            )
            return actual_row

    def update_status(self, lead_id: str, status_name: str, tab_name: str = "") -> bool:
        lead_id = str(lead_id).strip()
        with self.lock:
            ws = self._get_or_create_month_sheet(
                tab_name or datetime.now().strftime("%m.%Y")
            )
            row_idx = self._refresh_row_index_locked(ws, ws.title)
            try:
                row_num = self._verified_row_for_lead_locked(
                    ws, ws.title, lead_id, row_idx
                )
            except DuplicateLeadIdError:
                _log_lead.error(
                    "SHEET STATUS skipped duplicate lead=%s tab='%s'",
                    lead_id, ws.title,
                )
                return False
            if not row_num:
                _log_lead.warning(
                    "SHEET STATUS — lead %s not found in tab '%s'", lead_id, tab_name
                )
                return False
            status_col_letter = chr(ord("A") + STATUS_COL_INDEX)
            ws.update(
                values=[[status_name]],
                range_name=(
                    f"{status_col_letter}{row_num}:"
                    f"{status_col_letter}{row_num}"
                ),
                value_input_option="USER_ENTERED",
            )
            written_id = str(
                ws.cell(row_num, ID_COL_INDEX + 1).value or ""
            ).strip()
            if written_id != lead_id:
                self._invalidate_row_index(ws.title)
                raise SheetIntegrityError(
                    f"Status update verification failed for lead {lead_id}: "
                    f"'{ws.title}' row {row_num} now contains ID '{written_id}'"
                )
            _log_lead.info(
                "SHEET STATUS lead=%s → '%s' (row=%d tab='%s')",
                lead_id, status_name, row_num, ws.title,
            )
            return True

    def iter_lead_statuses(self, tabs_filter: Optional[set] = None) -> List[Dict[str, Any]]:
        """Iterate statuses across relevant monthly worksheets.

        When ``tabs_filter`` is provided only the named tabs are scanned,
        which avoids reading stale archived months on every poll cycle.
        Each returned dict includes a ``tab_name`` key so callers can record
        which sheet each lead lives on.
        """
        out: List[Dict[str, Any]] = []

        if tabs_filter is not None:
            # Active-sheet scans already know exactly which tab they require.
            # Going through a cached title list could turn a transient metadata
            # failure/miss into an empty scan and falsely classify every lead as
            # deleted, so open these tabs directly.
            tabs_to_scan = sorted(tabs_filter)
        else:
            # Unfiltered maintenance scans discover monthly worksheets.  Cache
            # titles briefly to avoid a metadata round-trip for each archive.
            now = time.time()
            if not self._ws_titles_cache or now - self._ws_titles_ts > 120:
                try:
                    self.spreadsheet = self.gc.open_by_key(self.cfg.GOOGLE_SHEET_ID)
                    self._ws_titles_cache = [
                        ws.title for ws in self.spreadsheet.worksheets()
                    ]
                    self._ws_titles_ts = now
                except Exception as exc:
                    _log.warning(
                        "iter_lead_statuses: could not list worksheets: %s", exc
                    )
                    if not self._ws_titles_cache:
                        return out
            month_pattern = re.compile(r'^\d{2}\.\d{4}$')  # e.g. "03.2026"
            tabs_to_scan = [
                title for title in self._ws_titles_cache
                if month_pattern.match(title)
                or title == self.cfg.GOOGLE_WORKSHEET_NAME
            ]

        for tab_name in tabs_to_scan:
            try:
                with self.lock:
                    ws = self._get_or_create_month_sheet(tab_name)
                    # Read and replace the cache while holding the same lock used
                    # by writers.  Never merge stale entries into a fresh snapshot.
                    all_vals = ws.get_all_values()
                    self._refresh_row_index_locked(ws, tab_name, all_vals)
                    duplicate_ids = set(self._duplicate_ids.get(tab_name, set()))

                    for i, row in enumerate(all_vals):
                        if i == 0 or len(row) <= ID_COL_INDEX:
                            continue
                        lead_id = str(row[ID_COL_INDEX]).strip()
                        if not lead_id or lead_id in duplicate_ids:
                            continue
                        status = (
                            str(row[STATUS_COL_INDEX]).strip()
                            if len(row) > STATUS_COL_INDEX else ""
                        )
                        order_number = (
                            str(row[ORDER_NUM_COL_INDEX]).strip()
                            if len(row) > ORDER_NUM_COL_INDEX else ""
                        )
                        out.append({
                            "lead_id": lead_id,
                            "status": status,
                            "order_number": order_number,
                            "tab_name": tab_name,
                            "duplicate": False,
                        })

                    # Include one quarantine marker per duplicate so deletion
                    # detection still sees the ID, while every AMO action skips it.
                    for lead_id in sorted(duplicate_ids):
                        out.append({
                            "lead_id": lead_id,
                            "status": "",
                            "order_number": "",
                            "tab_name": tab_name,
                            "duplicate": True,
                        })
            except Exception as exc:
                _log.warning("iter_lead_statuses: could not read tab '%s': %s", tab_name, exc)
                # Filtered scans are used for the active Sheet1 sync.  Returning a
                # partial/empty result would make _detect_deleted_rows forget valid
                # leads, so fail the whole cycle instead.
                if tabs_filter is not None:
                    raise
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

    # Extract contact name and ALL phone numbers from embedded contacts
    contact_name = lead.get("name", "")
    _phone_seen: list = []
    contacts = (lead.get("_embedded") or {}).get("contacts") or []
    for contact in contacts:
        if contact.get("name"):
            contact_name = contact["name"]
        # custom_fields_values may already be embedded (if fetched with full contact)
        for cf in contact.get("custom_fields_values") or []:
            if cf.get("field_code") == "PHONE" or cf.get("field_name", "").upper() in ("PHONE", "ТЕЛЕФОН"):
                for v in cf.get("values") or []:
                    num = str(v.get("value", "")).strip().lstrip("+")
                    if num and num not in _phone_seen:
                        _phone_seen.append(num)
    contact_phone = ", ".join(_phone_seen)

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
                        first_val = values[0].get("value", "")
                        converted = _ts_to_date(first_val)
                        # Only replace val if conversion succeeded; otherwise keep raw
                        if converted or first_val in (0, "0", "", None):
                            val = converted
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

    # If "Дата заказа" was not filled in AmoCRM, fall back to the lead's own
    # created_at timestamp (the moment the lead was created in AMO).
    # This is shown with time since it is a precise creation moment.
    if not mapped.get("Дата заказа"):
        mapped["Дата заказа"] = _ts_to_date(lead.get("created_at"), include_time=True)

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
        # A lead must be absent from three consecutive successful scans before
        # it is treated as deleted; one partial Sheets response is not evidence.
        self._missing_sheet_counts: Dict[str, int] = {}
        self._load_structure_mappings()
        self._load_users()
        self._print_config_warnings()
        # ── KPI event store ──────────────────────────────────────────────────
        _kpi_db = os.getenv("KPI_DB_PATH", "./data/kpi_events.db")
        self.kpi_store = KPIStore(
            db_path=_kpi_db,
            tz_offset=self.cfg.DISPLAY_TZ_OFFSET,
            dumka_recovery_days=int(os.getenv("DUMKA_RECOVERY_DAYS", "5")),
        )
        _log.info("KPI store initialised at %s", _kpi_db)

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
            _log.warning("Could not load users: %s", exc)

    def _load_structure_mappings(self) -> None:
        try:
            data = self.amo.get("/api/v4/leads/pipelines?with=statuses&limit=250")
            pipelines = data.get("_embedded", {}).get("pipelines", [])
        except Exception as exc:
            _log.warning("Could not load AMO structure, falling back to .env IDs: %s", exc)
            if "refresh token" in str(exc).lower() or "token" in str(exc).lower():
                _log.info("Complete OAuth first via POST /oauth/exchange, then restart service.")
                if self.cfg.AMO_CLIENT_ID and self.cfg.AMO_REDIRECT_URI:
                    _log.info("Open auth URL: %s", self.amo.auth_url())
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
                _log.info(
                    "Pipeline auto-registered: '%s' (set PIPELINE_DISPLAY_MAP_JSON to customise)",
                    pipeline_raw_name,
                )

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

                # Prefer exact match; fall back to normalized (handles mixed
                # Latin/Cyrillic chars and casing variants like Rushana's pipeline).
                display_name = (
                    STATUS_DISPLAY_MAP.get(status_name)
                    or _STATUS_DISPLAY_NORMALIZED.get(_normalize_amo_name(status_name))
                    or _STATUS_DISPLAY_LOWERED.get(status_name.lower())
                    or status_name
                )
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
            _log.warning("No trigger status IDs resolved. Leads will NOT be added from webhook.")
        if self.cfg.PIPELINE_ID == 0:
            _log.warning("PIPELINE_ID is 0. Service will use each lead's current pipeline dynamically.")
        zero_terminal = [name for name, sid in self.cfg.STATUS_MAP.items() if not sid]
        if zero_terminal:
            _log.warning("Terminal status IDs are not configured: %s", zero_terminal)
        _log.info("Resolved trigger status IDs: %s", sorted(self.trigger_status_ids))

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
            prev = self.state.get("sheet_status_by_lead", {}).get(str(lead_id), "")
            self.state.setdefault("sheet_status_by_lead", {})[str(lead_id)] = status_name
            self._state_dirty = True
        if prev != status_name:
            _log_lead.info("LEAD %s status tracked: '%s' → '%s'", lead_id, prev or "(new)", status_name)

    def get_known_sheet_status(self, lead_id: str) -> str:
        return self.state.get("sheet_status_by_lead", {}).get(str(lead_id), "")

    def remember_sheet_order_number(self, lead_id: str, order_number: str) -> None:
        with self.state_lock:
            prev = self.state.get("sheet_order_number_by_lead", {}).get(str(lead_id))
            self.state.setdefault("sheet_order_number_by_lead", {})[str(lead_id)] = order_number
            self._state_dirty = True
        if prev is not None and prev != order_number and order_number:
            _log_lead.info("LEAD %s order# tracked: '%s' → '%s'", lead_id, prev, order_number)

    def get_known_order_number(self, lead_id: str) -> str:
        return self.state.get("sheet_order_number_by_lead", {}).get(str(lead_id), "")

    # ── Lead lifetime / expiry ────────────────────────────────────────────────
    # When a lead reaches a terminal status we start a countdown. Once the
    # countdown expires every subsequent webhook for that lead is ignored and
    # the lead is removed from the state entirely.
    _EXPIRY_SECONDS: Dict[str, int] = {}

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
            prev_status = self.state.get("sheet_status_by_lead", {}).pop(lid, None)
            self.state.get("sheet_order_number_by_lead", {}).pop(lid, None)
            self.state.get("lead_expiry",                {}).pop(lid, None)
            self.state.get("lead_tab_by_lead",           {}).pop(lid, None)
            self._state_dirty = True
        _log_lead.info("LEAD %s forgotten (last status='%s')", lead_id, prev_status or "?")

    def expire_finished_leads(self) -> None:
        """Purge leads whose monitoring window has elapsed. Called from the worker loop."""
        now = time.time()
        expired = [
            lid for lid, ts in list(self.state.get("lead_expiry", {}).items())
            if now >= ts
        ]
        for lid in expired:
            _log_lead.info("LEAD %s monitoring window expired — removing from tracking.", lid)
            self.forget_lead(lid)
        if expired:
            self.flush_state()

    def _set_expiry_for_status(self, lead_id: str, status_display: str) -> None:
        """If status_display has a configured lifetime, start (or overwrite) the countdown."""
        seconds = self._EXPIRY_SECONDS.get(status_display)
        if seconds is not None:
            expiry_ts = time.time() + seconds
            self.remember_lead_expiry(lead_id, expiry_ts)
            expiry_str = datetime.fromtimestamp(expiry_ts).strftime("%Y-%m-%d %H:%M:%S")
            _log_lead.info(
                "LEAD %s expiry set: status='%s' — will be forgotten at %s (in %dh)",
                lead_id, status_display, expiry_str, seconds // 3600,
            )

    def _tab_for_lead(self, lead: Dict[str, Any]) -> str:  # noqa: ARG002
        """Return the active worksheet name for new lead writes.

        The current month's data always lives on GOOGLE_WORKSHEET_NAME ("Sheet1").
        When the month rolls over that tab is renamed to "MM.YYYY" and a fresh
        Sheet1 is created, so returning the configured name here is always correct
        for new leads.
        """
        return self.cfg.GOOGLE_WORKSHEET_NAME

    def remember_lead_tab(self, lead_id: str, tab_name: str) -> None:
        """Store which monthly sheet tab this lead was written to."""
        with self.state_lock:
            self.state.setdefault("lead_tab_by_lead", {})[str(lead_id)] = tab_name
            self._state_dirty = True

    def get_lead_tab(self, lead_id: str) -> str:
        """Return the tab name where this lead's row lives.

        Falls back to GOOGLE_WORKSHEET_NAME (Sheet1) if not recorded — i.e. the
        active sheet, which is always correct for leads written during the current
        month before a rotation occurred.
        """
        tab = self.state.get("lead_tab_by_lead", {}).get(str(lead_id), "")
        return tab if tab else self.cfg.GOOGLE_WORKSHEET_NAME

    def remember_lead_pipeline(self, lead_id: str, pipeline_id: int) -> None:
        """Cache the AMO pipeline_id so sync_sheet_to_amo can skip a GET per lead."""
        if not pipeline_id:
            return
        with self.state_lock:
            self.state.setdefault("lead_pipeline_by_lead", {})[str(lead_id)] = pipeline_id
            self._state_dirty = True

    def get_lead_pipeline(self, lead_id: str) -> int:
        """Return cached pipeline_id or 0 if not yet stored."""
        return int(self.state.get("lead_pipeline_by_lead", {}).get(str(lead_id), 0) or 0)

    # Set of display names considered valid for sheet status cells.
    _VALID_DISPLAY_STATUSES: frozenset = frozenset(STATUS_DISPLAY_MAP.values()) | frozenset(
        ["В процессе", "У курера", "Успешно", "Отказ", "Неразобранное",
         "Закрыто и не реализовано"]
    )

    def bootstrap_sheet_state(self) -> None:
        # ── Detect leads whose order-number was never re-registered after a row
        # deletion + re-add cycle.  The failure signature is: the lead exists in
        # sheet_status_by_lead (status was re-synced by sync_sheet_to_amo after the
        # row reappeared) but is absent from sheet_order_number_by_lead (the order#
        # path was never restored).  When bootstrap then reads the sheet and finds a
        # non-empty Заказ № for such a lead, the normal trigger will not fire because
        # the recorded known_order is now the filled value, not "".  We queue these
        # leads here and push the missed PATCH to AMO after the main bootstrap loop.
        # Include leads where known_order is absent OR empty ("") —
        # both mean the order-number PATCH has never been sent to AMO.
        _status_no_order: set = {
            lid for lid in self.state.get("sheet_status_by_lead", {})
            if not self.state.get("sheet_order_number_by_lead", {}).get(lid, "")
        }

        # Bootstrap only from Sheet1 — the active tab. Old month archive tabs are
        # never re-scanned to avoid the historical replay storm.
        rows = self.sheet.iter_lead_statuses(tabs_filter={self.cfg.GOOGLE_WORKSHEET_NAME})
        _missed_pushes: List[Dict[str, Any]] = []

        for item in rows:
            if item.get("duplicate"):
                _log_lead.error(
                    "BOOTSTRAP skipped duplicate lead=%s tab='%s' until the "
                    "duplicate rows are resolved manually",
                    item.get("lead_id"), item.get("tab_name", ""),
                )
                continue
            raw_status = item["status"]
            lead_id    = item["lead_id"]
            tab        = item.get("tab_name", "")
            order_number = item.get("order_number", "")

            # ── Heal stale / raw status names written before normalization was in place ──
            # If the cell contains a raw AMO status name (e.g. "заказ отпрAвлен" with
            # a Latin A, written by an older version of the service), correct it now
            # by looking it up through the same normalised map used at runtime.
            healed_status = raw_status
            if raw_status and raw_status not in self._VALID_DISPLAY_STATUSES:
                normalized = _normalize_amo_name(raw_status)
                candidate = (
                    STATUS_DISPLAY_MAP.get(raw_status)
                    or _STATUS_DISPLAY_NORMALIZED.get(normalized)
                    or _STATUS_DISPLAY_LOWERED.get(raw_status.lower())
                )
                if candidate and candidate != raw_status:
                    _log_lead.warning(
                        "BOOTSTRAP heal: lead=%s tab='%s' cell status '%s' → corrected to '%s'",
                        lead_id, tab, raw_status, candidate,
                    )
                    try:
                        if self.sheet.update_status(lead_id, candidate, tab):
                            healed_status = candidate
                    except Exception as exc:
                        _log.warning("BOOTSTRAP heal failed for lead %s: %s", lead_id, exc)

            # ── Queue leads whose Заказ № PATCH was never sent to AMO ──
            if (lead_id in _status_no_order
                    and order_number
                    and healed_status == "В процессе"):
                _missed_pushes.append({"lead_id": lead_id, "order_number": order_number})

            self.remember_sheet_status(lead_id, healed_status)
            # Snapshot the current order number so we can detect when it gets filled
            self.remember_sheet_order_number(lead_id, order_number)
            # Record which tab each lead lives on (used by update_status routing)
            if tab:
                self.remember_lead_tab(lead_id, tab)
        self.flush_state()  # Persist bootstrapped state in one write

        # ── Recovery: push missed Заказ № values to AMO ──
        # Only act if AMO's order# field is still empty — guards against double-push
        # on repeated restarts or when the field was already corrected manually.
        if not self.cfg.BOOTSTRAP_RECOVERY:
            _log.info("BOOTSTRAP RECOVERY skipped (BOOTSTRAP_RECOVERY=false)")
            _missed_pushes.clear()
        if not _missed_pushes:
            return

        # Run recovery in a background thread so the worker loop starts immediately
        # and can handle new leads while the replay processes historic rows.
        def _do_recovery(missed_list: list) -> None:
            _log.info("BOOTSTRAP RECOVERY starting: %d lead(s) to replay in background", len(missed_list))
            for _i, missed in enumerate(missed_list):
                _lid = missed["lead_id"]
                _order = missed["order_number"]
                try:
                    _lead_data = self.amo.get(f"/api/v4/leads/{_lid}")
                    if not _lead_data:
                        continue
                    _pipeline_id = int(_lead_data.get("pipeline_id", 0) or 0)
                    # Read the current AMO value of the Заказ № field.
                    _amo_order = ""
                    for _cf in (_lead_data.get("custom_fields_values") or []):
                        if _cf.get("field_id") == 987889:
                            _amo_order = str((((_cf.get("values") or [{}])[0]).get("value", "")) or "")
                            break
                    if _amo_order:
                        # AMO already has the value (manually corrected or from a prior run).
                        _log_lead.info(
                            "BOOTSTRAP RECOVERY: lead=%s order# already in AMO ('%s') — skipping",
                            _lid, _amo_order,
                        )
                        continue
                    _status_id = (
                        self.pipeline_status_display_to_id.get(_pipeline_id, {}).get(ORDER_NUM_FILLED_AMO_STATUS_DISPLAY)
                        or self.pipeline_status_name_to_id.get(_pipeline_id, {}).get("Заказ отправлен")
                    )
                    if not _status_id:
                        _log_lead.warning(
                            "BOOTSTRAP RECOVERY: lead=%s no '%s' status found for pipeline %d — skipping",
                            _lid, ORDER_NUM_FILLED_AMO_STATUS_DISPLAY, _pipeline_id,
                        )
                        continue
                    self.amo.patch(
                        f"/api/v4/leads/{_lid}",
                        {
                            "status_id": _status_id,
                            "pipeline_id": _pipeline_id or self.cfg.PIPELINE_ID,
                            "custom_fields_values": [
                                {"field_id": 987889, "values": [{"value": _order}]}
                            ],
                        },
                    )
                    _log_lead.info(
                        "BOOTSTRAP RECOVERY: lead=%s order# '%s' → AMO PATCH sent "
                        "(status_id=%d pipeline=%d)",
                        _lid, _order, _status_id, _pipeline_id,
                    )
                    # Sheet status stays at "В процессе" — the order number was entered
                    # but the parcel is not yet delivered.  The sheet must not be
                    # updated to "У курера" here; that transition only happens when the
                    # operator explicitly sets "У курера" in the sheet.
                    self.remember_sheet_status(_lid, "В процессе")
                except Exception as exc:
                    _log.error("BOOTSTRAP RECOVERY: lead=%s failed: %s", _lid, exc)
                # Flush state every 20 leads to guard against crash mid-recovery.
                if (_i + 1) % 20 == 0:
                    self.flush_state()
            self.flush_state()
            _log.info("BOOTSTRAP RECOVERY complete: %d lead(s) processed", len(missed_list))

        threading.Thread(target=_do_recovery, args=(_missed_pushes,), daemon=True, name="bootstrap-recovery").start()

    def initial_sync_leads(self, date_from: str, date_to: str) -> None:
        """Fetch all AMO leads created in [date_from, date_to] and upsert them into the sheet.

        Called once on startup when INITIAL_SYNC_DATE_FROM / INITIAL_SYNC_DATE_TO are set.
        Leads already present in the sheet are updated in-place; new ones are appended.
        """
        _log.info("Initial sync: fetching AMO leads created %s – %s …", date_from, date_to)
        try:
            leads = self.amo.fetch_leads_by_date_range(date_from, date_to)
        except Exception as exc:
            _log.error("Initial sync failed to fetch leads: %s", exc)
            return

        _log.info("Initial sync: %d lead(s) returned from AMO.", len(leads))
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

            tab_name = self._tab_for_lead(lead)
            row = build_row(lead, status_display, pipeline_name, responsible_name, staff_mapping)
            try:
                self.sheet.upsert_row(row, tab_name)
            except DuplicateLeadIdError:
                _log_lead.error(
                    "INITIAL SYNC skipped duplicate lead=%s tab='%s'",
                    lead_id, tab_name,
                )
                skipped += 1
                continue
            actual_sheet_status = str(row[STATUS_COL_INDEX]).strip() or status_display
            self.remember_sheet_status(lead_id, actual_sheet_status)
            self.remember_lead_tab(lead_id, tab_name)
            self.remember_lead_pipeline(lead_id, pipeline_id)
            self.remember_sheet_order_number(
                lead_id, str(row[ORDER_NUM_COL_INDEX]).strip()
            )
            written += 1

        self.flush_state()  # Persist all initial sync state in one write
        _log.info("Initial sync complete: %d written, %d skipped.", written, skipped)

    def catch_up_trigger_leads(self) -> int:
        """Poll AMO for leads currently in any trigger status and write missing ones to the sheet.

        Called periodically from the worker loop to self-heal leads that were
        missed because AMO stopped retrying webhook delivery after consecutive
        failures (e.g. during a service restart).

        Only writes leads that have no known sheet status — leads already
        tracked are left untouched.
        """
        if not self.trigger_status_ids:
            return 0

        written = 0
        try:
            # Build a reverse map: trigger_status_id → pipeline_id using the
            # structure loaded at startup (pipeline_status_name_to_id).
            status_to_pipeline: Dict[int, int] = {}
            for pid, statuses in self.pipeline_status_name_to_id.items():
                for _name, sid in statuses.items():
                    if sid in self.trigger_status_ids:
                        status_to_pipeline[sid] = pid

            # AMO API v4 requires paired pipeline+status filters:
            # filter[statuses][N][pipeline_id]=P&filter[statuses][N][status_id]=S
            pairs = [
                (status_to_pipeline[sid], sid)
                for sid in sorted(self.trigger_status_ids)
                if sid in status_to_pipeline
            ]

            if not pairs:
                _log.warning(
                    "CATCH-UP: could not resolve pipeline IDs for trigger statuses %s — skipping",
                    sorted(self.trigger_status_ids),
                )
                return 0

            ids_param = "&".join(
                f"filter[statuses][{i}][pipeline_id]={pid}"
                f"&filter[statuses][{i}][status_id]={sid}"
                for i, (pid, sid) in enumerate(pairs)
            )

            _log.debug(
                "CATCH-UP: querying AMO for %d trigger status(es) across %d pipeline(s)",
                len(pairs), len({p for p, _ in pairs}),
            )

            leads: List[Dict[str, Any]] = []
            page = 1
            # Only fetch leads updated within CATCH_UP_DAYS to avoid processing the
            # entire historical backlog of leads stuck in trigger status.
            _cutoff_ts = int(time.time()) - self.cfg.CATCH_UP_DAYS * 86400
            _date_filter = f"&filter[updated_at][from]={_cutoff_ts}"
            while True:
                data = self.amo.get(
                    f"/api/v4/leads?{ids_param}{_date_filter}&with=contacts,companies&limit=250&page={page}"
                )
                batch = (data.get("_embedded") or {}).get("leads") or []
                if not batch:
                    break
                leads.extend(batch)
                if not (data.get("_links") or {}).get("next"):
                    break
                page += 1

            _log.debug("CATCH-UP: AMO returned %d lead(s) in trigger status(es)", len(leads))

            if not leads:
                return 0

            # Enrich contacts in batches (phone numbers, names)
            self._batch_enrich_contacts(leads)
            staff_mapping = self.sheet.get_staff_mapping()

            for lead in leads:
                lead_id = str(lead.get("id", "")).strip()
                if not lead_id:
                    continue

                # Skip leads already tracked — they were handled by webhook
                if self.get_known_sheet_status(lead_id):
                    continue

                # Apply LEADS_CREATED_AFTER filter
                if self.cfg.LEADS_CREATED_AFTER:
                    updated_at = int(lead.get("updated_at", 0) or 0)
                    if updated_at and updated_at < self.cfg.LEADS_CREATED_AFTER:
                        continue

                # Apply PIPELINE_KEYWORD filter
                pipeline_id   = int(lead.get("pipeline_id", 0) or 0)
                pipeline_name = self.pipeline_id_to_name.get(pipeline_id, "")
                if self.cfg.PIPELINE_KEYWORD and self.cfg.PIPELINE_KEYWORD not in pipeline_name.lower():
                    continue

                status_id            = int(lead.get("status_id", 0) or 0)
                responsible_id       = int(lead.get("responsible_user_id", 0) or 0)
                responsible_name     = self.users_map.get(responsible_id, str(responsible_id))
                current_status_name  = self.status_id_to_display_name.get(status_id, "В процессе")

                tab_name = self._tab_for_lead(lead)
                row = build_row(lead, current_status_name, pipeline_name, responsible_name, staff_mapping)
                # Space out sheet writes to avoid Google Sheets quota (10 writes/sec limit).
                # Uses the same AMO throttle delay as a safe floor; avoids an extra config key.
                if written > 0:
                    time.sleep(max(self.cfg.AMO_REQUEST_DELAY_SEC, 0.5))
                try:
                    self.sheet.upsert_row(row, tab_name)
                except DuplicateLeadIdError:
                    _log_lead.error(
                        "CATCH-UP skipped duplicate lead=%s tab='%s'",
                        lead_id, tab_name,
                    )
                    continue
                actual_sheet_status = (
                    str(row[STATUS_COL_INDEX]).strip() or current_status_name
                )
                self.remember_sheet_status(lead_id, actual_sheet_status)
                self.remember_lead_tab(lead_id, tab_name)
                self.remember_lead_pipeline(lead_id, pipeline_id)
                actual_order_num = str(row[ORDER_NUM_COL_INDEX]) if len(row) > ORDER_NUM_COL_INDEX else ""
                self.remember_sheet_order_number(lead_id, actual_order_num)
                written += 1
                _log.info(
                    "CATCH-UP lead=%s pipeline='%s' status='%s' → written to sheet tab='%s'",
                    lead_id, pipeline_name, current_status_name, tab_name,
                )

            if written:
                self.flush_state()
                _log.info("CATCH-UP: wrote %d missed lead(s) to sheet", written)

        except Exception as exc:
            _log.error("CATCH-UP failed: %s", exc)

        return written

    def check_and_rotate_sheet(self) -> None:
        """Archive the active worksheet when the month rolls over.

        Sheet1 (GOOGLE_WORKSHEET_NAME) always holds the *current* month's data.
        On the first call of a new month:
          1. Sheet1 is renamed to "MM.YYYY" (e.g. "02.2026") — the archive tab.
          2. A fresh Sheet1 is created for the new month.
          3. Any lead whose tracked tab was "Sheet1" has its pointer updated to
             the new archive name so future status updates still find the right row.

        Safe to call every SYNC_POLL_SECONDS — exits in O(1) when nothing changed.
        """
        tz = timezone(timedelta(hours=self.cfg.DISPLAY_TZ_OFFSET))
        current_month = datetime.now(tz).strftime("%m.%Y")  # e.g. "03.2026"

        with self.state_lock:
            known_key = self.state.get("active_sheet_month", "")

        # Normalise legacy "YYYY-MM" key to "MM.YYYY"
        if known_key and known_key != current_month:
            try:
                known_key = datetime.strptime(known_key, "%Y-%m").strftime("%m.%Y")
            except ValueError:
                pass  # already "MM.YYYY" or some other format

        # Fast path: still in the same month — verify the active tab exists, but
        # never create an empty replacement.  A missing/renamed live tab requires
        # manual recovery so tracked leads are not mistaken for deleted rows.
        if known_key == current_month:
            try:
                self.sheet._get_or_create_month_sheet(self.cfg.GOOGLE_WORKSHEET_NAME)
            except Exception as exc:
                _log.warning("Could not ensure active tab '%s': %s",
                             self.cfg.GOOGLE_WORKSHEET_NAME, exc)
            return

        main_name = self.cfg.GOOGLE_WORKSHEET_NAME

        if not known_key:
            # First ever run — nothing to archive, just record the current month
            # and ensure the active tab exists.
            try:
                self.sheet._get_or_create_month_sheet(
                    main_name, allow_create=True
                )
            except Exception as exc:
                _log.warning("Could not ensure active tab '%s': %s", main_name, exc)
            with self.state_lock:
                self.state["active_sheet_month"] = current_month
            self._save_state()
            _log.info("Sheet rotation initialised: current month = '%s'", current_month)
            return

        # Month has rolled over — archive Sheet1 under the old month's name
        archive_name = known_key  # e.g. "02.2026"
        _log.info("Month changed '%s' → '%s': archiving '%s' as '%s'",
                  known_key, current_month, main_name, archive_name)
        try:
            self.sheet.rotate_to_archive(archive_name)
        except Exception as exc:
            _log.error("Sheet rotation failed: %s", exc)
            return

        # Update lead_tab_by_lead: every lead that was on "Sheet1" is now on the
        # archive tab so status updates keep routing to the correct sheet.
        with self.state_lock:
            lead_tabs = self.state.get("lead_tab_by_lead", {})
            updated = 0
            for lid, tab in lead_tabs.items():
                if tab == main_name:
                    lead_tabs[lid] = archive_name
                    updated += 1
            if updated:
                self._state_dirty = True
        if updated:
            _log.info("Updated tab pointer for %d lead(s): '%s' → '%s'",
                      updated, main_name, archive_name)

        with self.state_lock:
            self.state["active_sheet_month"] = current_month
        self._save_state()

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

    def _batch_enrich_contacts(self, leads: List[Dict[str, Any]]) -> None:
        """Fetch contact details (phone etc.) for multiple leads in one batch request.

        AMO embeds contacts in lead responses but omits custom_fields_values (phone).
        This collects all contact IDs missing that data, fetches them in chunks of 50,
        then updates each lead's embedded contacts in-place.
        """
        contact_ids: List[int] = []
        for lead in leads:
            for c in (lead.get("_embedded") or {}).get("contacts") or []:
                cid = c.get("id")
                if cid and not c.get("custom_fields_values") and cid not in contact_ids:
                    contact_ids.append(cid)
        if not contact_ids:
            return

        fetched: Dict[int, Dict[str, Any]] = {}
        CHUNK = 50
        for i in range(0, len(contact_ids), CHUNK):
            chunk = contact_ids[i : i + CHUNK]
            ids_param = "&".join(f"filter[id][]={cid}" for cid in chunk)
            try:
                data = self.amo.get(f"/api/v4/contacts?{ids_param}&limit={CHUNK}")
                for contact in (data.get("_embedded") or {}).get("contacts") or []:
                    fetched[int(contact["id"])] = contact
            except Exception as exc:
                _log.error("_batch_enrich_contacts chunk failed: %s", exc)

        if not fetched:
            return

        for lead in leads:
            contacts = (lead.get("_embedded") or {}).get("contacts") or []
            enriched = [
                fetched.get(int(c["id"]), c) if c.get("id") else c
                for c in contacts
            ]
            if enriched:
                lead.setdefault("_embedded", {})["contacts"] = enriched

    def _record_kpi_event(
        self, full_lead: Dict[str, Any], webhook_status_id: int
    ) -> None:
        """Record a KPI event (consul/zakas/dumka) based on the webhook transition.

        Uses webhook_status_id so we capture the transition that *happened*,
        even if the lead has since moved to a different status in AMO.
        """
        display_name = self.status_id_to_display_name.get(webhook_status_id, "")
        if not display_name:
            return

        lead_id = str(full_lead.get("id", "")).strip()
        if not lead_id:
            return

        pipeline_id   = int(full_lead.get("pipeline_id", 0) or 0)
        pipeline_name = self.pipeline_id_to_name.get(pipeline_id, "")
        budget        = float(full_lead.get("price", 0) or 0)

        # Pipeline keyword filter: only record KPI for sotuv-type pipelines
        if self.cfg.PIPELINE_KEYWORD:
            if self.cfg.PIPELINE_KEYWORD not in pipeline_name.lower():
                return

        try:
            if display_name in KPI_CONSUL_DISPLAY_NAMES:
                staff_code = _extract_staff_code(full_lead)
                if staff_code:
                    ok = self.kpi_store.record_consul(
                        lead_id, staff_code, None, pipeline_name, budget
                    )
                    if ok:
                        _log.info("KPI consul  lead=%s staff=%s", lead_id, staff_code)

            elif display_name in KPI_ZAKAS_DISPLAY_NAMES:
                ok = self.kpi_store.record_zakas(lead_id, None, budget, pipeline_name)
                if ok:
                    _log.info("KPI zakas   lead=%s", lead_id)

            elif display_name in KPI_DUMKA_DISPLAY_NAMES:
                ok = self.kpi_store.record_dumka(lead_id, None, pipeline_name)
                if ok:
                    _log.info("KPI dumka   lead=%s", lead_id)

            elif display_name in KPI_USPESHKA_DISPLAY_NAMES:
                ok = self.kpi_store.record_uspeshka(lead_id, None, budget, pipeline_name)
                if ok:
                    _log.info("KPI uspeshka lead=%s budget=%s", lead_id, budget)

            elif display_name in KPI_OTKAZ_FINAL_DISPLAY_NAMES:
                ok = self.kpi_store.record_otkaz(lead_id, None, pipeline_name)
                if ok:
                    _log.info("KPI otkaz   lead=%s", lead_id)

        except Exception as exc:
            _log.error("KPI error recording event for lead %s: %s", lead_id, exc)

    def run_kpi_backfill(self, date_from: str, date_to: str) -> Dict[str, int]:
        """Replay AMO events for the given date range and populate the KPI store."""
        pipeline_keyword = getattr(self.cfg, "PIPELINE_KEYWORD", "").lower()
        sotuv_pipeline_ids: set[int] = set()
        if pipeline_keyword:
            for pid, pname in self.pipeline_id_to_name.items():
                if pipeline_keyword in pname.lower():
                    sotuv_pipeline_ids.add(pid)

        all_pipeline_ids = set(self.pipeline_id_to_name.keys())
        scope = sotuv_pipeline_ids or all_pipeline_ids

        consul_status_ids = set(self.trigger_status_ids)  # КОНСУЛЬТАЦИЯ IDs

        zakas_status_ids: set[int] = {
            sid
            for pid in scope
            for dname, sid in self.pipeline_status_display_to_id.get(pid, {}).items()
            if dname in KPI_ZAKAS_DISPLAY_NAMES
        }
        dumka_status_ids: set[int] = {
            sid
            for pid in scope
            for dname, sid in self.pipeline_status_display_to_id.get(pid, {}).items()
            if dname in KPI_DUMKA_DISPLAY_NAMES
        }
        uspeshka_status_ids: set[int] = {
            sid
            for pid in scope
            for dname, sid in self.pipeline_status_display_to_id.get(pid, {}).items()
            if dname in KPI_USPESHKA_DISPLAY_NAMES
        }
        otkaz_status_ids: set[int] = {
            sid
            for pid in scope
            for dname, sid in self.pipeline_status_display_to_id.get(pid, {}).items()
            if dname in KPI_OTKAZ_FINAL_DISPLAY_NAMES
        }

        return self.kpi_store.backfill_from_amo(
            amo=self.amo,
            date_from=date_from,
            date_to=date_to,
            consul_status_ids=consul_status_ids,
            zakas_status_ids=zakas_status_ids,
            dumka_status_ids=dumka_status_ids,
            sotuv_pipeline_ids=sotuv_pipeline_ids,
            uspeshka_status_ids=uspeshka_status_ids,
            otkaz_status_ids=otkaz_status_ids,
        )

    def run_daily_catchup(self, date_str: str) -> Dict[str, int]:
        """Re-fetch AMO events for one day and update kpi_events (no snapshot)."""
        _log.info("[KPI-CATCHUP] Running for %s", date_str)
        try:
            counts = self.run_kpi_backfill(date_str, date_str)
            _log.info("[KPI-CATCHUP] Done for %s: %s", date_str, counts)
            return counts
        except Exception as exc:
            _log.error("[KPI-CATCHUP] Failed for %s: %s", date_str, exc)
            return {}

    def run_nightly_snapshot(self, date_str: str) -> None:
        """Re-fetch AMO events then freeze the day into manager_snapshots."""
        _log.info("[KPI-SNAPSHOT] Starting nightly snapshot for %s", date_str)
        try:
            self.run_daily_catchup(date_str)
            self.kpi_store.create_manager_snapshot(date_str)
            _log.info("[KPI-SNAPSHOT] Snapshot created for %s", date_str)
        except Exception as exc:
            _log.error("[KPI-SNAPSHOT] Failed for %s: %s", date_str, exc)

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

        # ── Pass 1: cheap local filtering — zero AMO API calls ────────────────────────────
        # Collect every lead that actually needs processing.  All checks here use
        # only local state so no network calls are made until the batch fetch below.
        qualifying: List[Dict[str, Any]] = []   # original webhook payloads
        wh_status_map: Dict[str, int] = {}       # lead_id → webhook_status_id
        pre_known: Dict[str, str]      = {}       # lead_id → known sheet status (snapshot)

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
                _log_wh.debug("WEBHOOK lead=%s status_id=%d — duplicate, skipped", lead_id, webhook_status_id)
                continue

            # Skip leads whose monitoring lifetime has ended
            if self.is_lead_expired(lead_id):
                skipped_status_mismatch += 1
                _log_wh.debug("WEBHOOK lead=%s status_id=%d — expired, skipped", lead_id, webhook_status_id)
                continue

            is_trigger  = webhook_status_id in self.trigger_status_ids
            is_terminal = str(webhook_status_id) in self.terminal_status_id_to_name
            known_status = self.get_known_sheet_status(lead_id)

            if not (is_trigger or is_terminal or known_status):
                skipped_status_mismatch += 1
                _log_wh.debug(
                    "WEBHOOK lead=%s status_id=%d — not trigger/terminal/known, skipped",
                    lead_id, webhook_status_id,
                )
                continue

            qualifying.append(lead)
            wh_status_map[lead_id] = webhook_status_id
            pre_known[lead_id]     = known_status

        # ── Batch fetch: ceil(N/50) lead GETs + ceil(C/50) contact GETs ───────────────
        # For a bulk of N qualifying leads this replaces N individual lead GETs
        # and N individual contact GETs with at most 2×ceil(N/50) requests.
        qualifying_ids  = [int(lead["id"]) for lead in qualifying]
        full_leads_map  = self.amo.batch_get_leads(qualifying_ids)
        self._batch_enrich_contacts(list(full_leads_map.values()))

        # ── Pass 2: business logic — all data already in memory ──────────────────────
        for lead in qualifying:
            lead_id           = str(lead["id"])
            webhook_status_id = wh_status_map[lead_id]
            known_status      = pre_known[lead_id]

            full_lead = full_leads_map.get(int(lead_id))
            if full_lead is None:
                # Batch fetch failed for this lead — fall back to webhook payload
                full_lead = lead
                status_id = webhook_status_id
            else:
                status_id = int(full_lead.get("status_id", 0) or 0)

            # KPI recording uses webhook_status_id so we capture what HAPPENED
            self._record_kpi_event(full_lead, webhook_status_id)

            # Skip leads last updated before the configured cutoff (ignores stale history)
            if self.cfg.LEADS_CREATED_AFTER:
                updated_at = int(full_lead.get("updated_at", 0) or 0)
                if updated_at and updated_at < self.cfg.LEADS_CREATED_AFTER:
                    skipped_too_old += 1
                    _log_wh.debug("WEBHOOK lead=%s — updated_at too old (%d), skipped", lead_id, updated_at)
                    continue

            # Skip leads from pipelines not matching the keyword filter.
            if self.cfg.PIPELINE_KEYWORD:
                wh_pipeline_id   = int(full_lead.get("pipeline_id", 0) or 0)
                wh_pipeline_name = self.pipeline_id_to_name.get(wh_pipeline_id, "")
                if self.cfg.PIPELINE_KEYWORD not in wh_pipeline_name.lower():
                    _log_wh.debug(
                        "WEBHOOK lead=%s pipeline='%s' — keyword filter mismatch, skipped",
                        lead_id, wh_pipeline_name,
                    )
                    continue

            # Also treat as trigger if the webhook itself said trigger AND this lead isn't
            # yet tracked — handles the race where the live status already moved on by the
            # time the batch fetch ran (e.g. after a server restart with a backlog of retries).
            webhook_is_trigger = webhook_status_id in self.trigger_status_ids
            if status_id in self.trigger_status_ids or (webhook_is_trigger and not known_status):
                trigger_matches  += 1
                trigger_display   = STATUS_DISPLAY_MAP.get(self.cfg.TRIGGER_STATUS_NAME, self.cfg.TRIGGER_STATUS_NAME)
                pipeline_id       = int(full_lead.get("pipeline_id", 0) or 0)
                pipeline_name     = self.pipeline_id_to_name.get(pipeline_id, "")
                responsible_id    = int(full_lead.get("responsible_user_id", 0) or 0)
                responsible_name  = self.users_map.get(responsible_id, str(responsible_id))
                # When the live status has already moved on, use the webhook status for display.
                effective_status_id = status_id if status_id in self.trigger_status_ids else webhook_status_id
                current_status_name = self.status_id_to_display_name.get(effective_status_id, trigger_display)

                tab_name = self._tab_for_lead(full_lead)
                row = build_row(full_lead, current_status_name, pipeline_name, responsible_name, staff_mapping)
                try:
                    self.sheet.upsert_row(row, tab_name)
                except DuplicateLeadIdError:
                    skipped_duplicate += 1
                    _log_wh.error(
                        "WEBHOOK TRIGGER skipped duplicate lead=%s tab='%s'",
                        lead_id, tab_name,
                    )
                    continue
                actual_sheet_status = (
                    str(row[STATUS_COL_INDEX]).strip() or current_status_name
                )
                self.remember_sheet_status(lead_id, actual_sheet_status)
                self.remember_lead_tab(lead_id, tab_name)
                self.remember_lead_pipeline(lead_id, pipeline_id)
                _log_wh.info(
                    "WEBHOOK TRIGGER lead=%s pipeline='%s' status='%s' → written to sheet tab='%s'",
                    lead_id, pipeline_name, current_status_name, tab_name,
                )
                # Preserve any Заказ № already stored in AMO so that if a lead returns
                # to the trigger status after the order number was filled, we do NOT
                # reset known_order to "" and accidentally re-trigger the Заказ № push.
                actual_order_num = str(row[ORDER_NUM_COL_INDEX]) if len(row) > ORDER_NUM_COL_INDEX else ""
                self.remember_sheet_order_number(lead_id, actual_order_num)
                written += 1
                continue

            terminal_name = self.terminal_status_id_to_name.get(str(status_id))
            if terminal_name:
                terminal_matches += 1
                lead_pipeline_id  = int(full_lead.get("pipeline_id", 0) or 0)
                sheet_display     = AMO_STATUS_TO_SHEET_OVERRIDE.get(terminal_name, terminal_name)
                # "Успешно" is display-only for staff and must NEVER be written to the
                # sheet by an incoming webhook.  The sheet already reflects what the
                # operator chose ("У курера", "В процессе", etc.) and that must be
                # preserved regardless of what AMO echoes back.
                if sheet_display == "Успешно":
                    skipped_status_mismatch += 1
                    _log_wh.debug(
                        "WEBHOOK TERMINAL lead=%s status='%s' — suppressed (Успешно is display-only)",
                        lead_id, terminal_name,
                    )
                    continue
                if not self.sheet.update_status(
                    lead_id, sheet_display, self.get_lead_tab(lead_id)
                ):
                    skipped_status_mismatch += 1
                    continue
                self.remember_sheet_status(lead_id, sheet_display)
                self.remember_lead_pipeline(lead_id, lead_pipeline_id)
                self._set_expiry_for_status(lead_id, sheet_display)
                _log_wh.info(
                    "WEBHOOK TERMINAL lead=%s amo_status='%s' → sheet='%s'",
                    lead_id, terminal_name, sheet_display,
                )
                written += 1
            else:
                if known_status:
                    new_status_display = self.status_id_to_display_name.get(status_id, str(status_id))
                    sheet_display      = AMO_STATUS_TO_SHEET_OVERRIDE.get(new_status_display, new_status_display)
                    # "Успешно" is display-only — never write it to the sheet from a webhook.
                    if sheet_display == "Успешно":
                        skipped_status_mismatch += 1
                        continue
                    if not self.sheet.update_status(
                        lead_id, sheet_display, self.get_lead_tab(lead_id)
                    ):
                        skipped_status_mismatch += 1
                        continue
                    self.remember_sheet_status(lead_id, sheet_display)
                    self.remember_lead_pipeline(lead_id, int(full_lead.get("pipeline_id", 0) or 0))
                    self._set_expiry_for_status(lead_id, sheet_display)
                    _log_wh.info(
                        "WEBHOOK STATUS lead=%s amo_status_id=%d → sheet='%s'",
                        lead_id, status_id, sheet_display,
                    )
                    written += 1
                else:
                    skipped_status_mismatch += 1
                    _log_wh.debug("WEBHOOK lead=%s status_id=%d — not known/tracked, skipped", lead_id, status_id)

        # Flush all state mutations accumulated during this batch in one disk write
        self.flush_state()
        _log_wh.info(
            "WEBHOOK BATCH done: received=%d written=%d triggers=%d terminals=%d "
            "skip_dup=%d skip_old=%d skip_mismatch=%d",
            len(leads), written, trigger_matches, terminal_matches,
            skipped_duplicate, skipped_too_old, skipped_status_mismatch,
        )
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

    def _detect_deleted_rows(
        self, visible_ids: set, scanned_tabs: Optional[set] = None
    ) -> None:
        """Compare leads we track in state against what is visible in the sheet.

        Only leads assigned to a successfully scanned tab are considered.  A lead
        must be absent in three consecutive scans before its tracking state is
        removed, which avoids treating one partial Sheets response as a deletion.
        """
        with self.state_lock:
            tracked = dict(self.state.get("lead_tab_by_lead", {}))
        for lead_id, tab in tracked.items():
            if lead_id in visible_ids:
                self._missing_sheet_counts.pop(lead_id, None)
                continue
            # An active-only scan says nothing about rows in archive tabs.
            if scanned_tabs is not None and tab not in scanned_tabs:
                continue
            # Skip leads that have already passed their expiry — expire_finished_leads
            # will handle those in its own loop.
            if self.is_lead_expired(lead_id):
                continue
            missing_count = self._missing_sheet_counts.get(lead_id, 0) + 1
            self._missing_sheet_counts[lead_id] = missing_count
            if missing_count < 3:
                _log_lead.warning(
                    "SHEET ROW MISSING (confirmation %d/3) — lead=%s tab='%s'; "
                    "no state was changed",
                    missing_count, lead_id, tab,
                )
                continue
            known_status = self.get_known_sheet_status(lead_id)
            _log_lead.warning(
                "SHEET ROW DELETED externally — lead=%s was in tab='%s' (last known status='%s')"
                " — row is no longer present in the sheet",
                lead_id, tab, known_status or "?",
            )
            # The absence is now confirmed; forget once and clear the counter.
            self.forget_lead(lead_id)
            self._missing_sheet_counts.pop(lead_id, None)
        self.flush_state()

    def sync_sheet_to_amo(self) -> None:
        # Always work with Sheet1 only. Sheet1 is the active tab; on month rollover
        # it gets renamed to MM.YYYY and a fresh Sheet1 is created automatically by
        # check_and_rotate_sheet(). Old month tabs are never scanned.
        _active_tabs: set = {self.cfg.GOOGLE_WORKSHEET_NAME}
        rows = self.sheet.iter_lead_statuses(tabs_filter=_active_tabs)
        visible_ids = {
            item["lead_id"] for item in rows
        } | self.sheet.recent_lead_ids()
        self._detect_deleted_rows(visible_ids, scanned_tabs=_active_tabs)
        for item in rows:
            if item.get("duplicate"):
                _log_lead.error(
                    "SHEET→AMO skipped duplicate lead=%s tab='%s' until the "
                    "duplicate rows are resolved manually",
                    item.get("lead_id"), item.get("tab_name", ""),
                )
                continue
            lead_id = item["lead_id"]
            status_name = item["status"]
            order_number = item.get("order_number", "")

            # ── Order-number trigger: Заказ № filled by admin → move to Заказ отправлен ──
            known_order = self.get_known_order_number(lead_id)
            # Only act if ALL of:
            #  • we have previously tracked this lead (key present in state),
            #  • the order number was empty before (known_order is ""),
            #  • it is now non-empty in the sheet,
            #  • the lead is still in "В процессе" — the stage where admin writes the
            #    order number.  If the lead is already at Отказ, Успешно, or У курера
            #    (came from a backward/forward AMO move), do NOT re-trigger this push.
            #    This prevents an infinite loop when a lead with a filled Заказ №
            #    is manually moved back to the trigger status by a manager.
            order_was_tracked = str(lead_id) in self.state.get("sheet_order_number_by_lead", {})
            # Self-heal: lead is visible in the sheet but was never registered in
            # sheet_order_number_by_lead (e.g. row was deleted and re-added externally,
            # or the lead was imported without going through the trigger webhook path).
            # Initialise with "" so the order-number trigger can fire this cycle if
            # the admin has already filled Заказ №.
            if not order_was_tracked:
                self.remember_sheet_order_number(lead_id, "")
                order_was_tracked = True
                # known_order is already "" — get_known_order_number returns "" for absent keys
            # Also self-heal tab tracking so _detect_deleted_rows and update_status
            # route to the correct worksheet.
            if str(lead_id) not in self.state.get("lead_tab_by_lead", {}):
                self.remember_lead_tab(lead_id, item.get("tab_name", self.cfg.GOOGLE_WORKSHEET_NAME))
            if order_was_tracked and not known_order and order_number and status_name == "В процессе":
                try:
                    lead_pipeline_id = self.get_lead_pipeline(lead_id)
                    if not lead_pipeline_id:
                        _p = self.amo.get(f"/api/v4/leads/{lead_id}")
                        lead_pipeline_id = int(_p.get("pipeline_id", 0) or 0)
                        if lead_pipeline_id:
                            self.remember_lead_pipeline(lead_id, lead_pipeline_id)
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
                        _log_lead.info(
                            "LEAD %s order# filled ('%s') → AMO PATCH sent (status_id=%d pipeline=%d)",
                            lead_id, order_number, status_id, lead_pipeline_id,
                        )
                        # Remember ONLY after a successful PATCH — if status_id was
                        # not found we must NOT update state so the trigger retries
                        # on the next poll cycle (after a service restart or fix).
                        self.remember_sheet_order_number(lead_id, order_number)
                        # Sheet status stays at "В процессе" — the order number was
                        # entered but the parcel is not yet delivered.  Only when the
                        # operator manually changes the sheet row to "У курера" will
                        # the lead progress to AMO "Успешно реализовано".
                        self.remember_sheet_status(lead_id, status_name)
                        # Order# trigger handled this lead fully — skip the status
                        # trigger below so it doesn't see the stale sheet status and
                        # fire a conflicting AMO PATCH in the same poll cycle.
                        continue
                    else:
                        _log_lead.warning(
                            "LEAD %s order# filled ('%s') but no 'Заказ отправлен' status ID found for pipeline %d",
                            lead_id, order_number, lead_pipeline_id,
                        )
                except Exception as exc:
                    if "Lead not found" in str(exc):
                        _log.warning(
                            "Lead %s no longer exists in AMO (deleted/merged) — "
                            "suppressing order# push and marking as handled",
                            lead_id,
                        )
                        self.remember_sheet_order_number(lead_id, order_number)
                    else:
                        _log.error("Failed to push order# for lead %s: %s", lead_id, exc)

            # ── Order-number update/clear: Заказ № changed or erased → sync to AMO field ──
            # This fires when the order number was already tracked (non-empty known_order)
            # and the sheet value differs — covers both edits and clearing the cell.
            elif order_was_tracked and known_order and known_order != order_number:
                try:
                    # For numeric AMO fields, sending "" raises a 400.
                    # When clearing the cell, omit custom_fields_values entirely —
                    # only push a new non-empty value to the AMO field.
                    patch_body: Dict[str, Any] = {}
                    if order_number:
                        patch_body["custom_fields_values"] = [
                            {"field_id": 987889, "values": [{"value": order_number}]}
                        ]
                    if not patch_body:
                        # Nothing to push to AMO for a clear; just update local state.
                        self.remember_sheet_order_number(lead_id, order_number)
                        continue
                    self.amo.patch(f"/api/v4/leads/{lead_id}", patch_body)
                    _log_lead.info(
                        "LEAD %s order# changed ('%s' → '%s') → AMO field updated",
                        lead_id, known_order, order_number,
                    )
                    self.remember_sheet_order_number(lead_id, order_number)
                except Exception as exc:
                    if "Lead not found" in str(exc):
                        _log.warning(
                            "Lead %s no longer exists in AMO (deleted/merged) — "
                            "suppressing order# update and marking as handled",
                            lead_id,
                        )
                        self.remember_sheet_order_number(lead_id, order_number)
                    elif "failed: 400" in str(exc):
                        # AMO rejected the value — most commonly happens when the field
                        # is of type "numeric" and the sheet cell was cleared to "".
                        # Record the current sheet value so we stop retrying this change.
                        _log.warning(
                            "LEAD %s order# update/clear rejected by AMO (400) — "
                            "recording current value '%s' to suppress retries. Error: %s",
                            lead_id, order_number, str(exc)[:200],
                        )
                        self.remember_sheet_order_number(lead_id, order_number)
                    else:
                        _log.error("Failed to update order# for lead %s: %s", lead_id, exc)

            # ── Status trigger: sheet status changed → push to AMO ──
            if status_name not in self.cfg.STATUS_MAP:
                continue

            # "Успешно" is a display-only label for staff — never push it to AMO.
            # (AMO itself writes "Успешно реализовано" which the webhook maps back to
            # the "Успешно" display label; we must not reverse-patch that back.)
            if status_name == "Успешно":
                continue

            known = self.get_known_sheet_status(lead_id)
            if known == status_name:
                continue

            try:
                lead_pipeline_id = self.get_lead_pipeline(lead_id)
                if not lead_pipeline_id:
                    _p = self.amo.get(f"/api/v4/leads/{lead_id}")
                    lead_pipeline_id = int(_p.get("pipeline_id", 0) or 0)
                    if lead_pipeline_id:
                        self.remember_lead_pipeline(lead_id, lead_pipeline_id)
                    else:
                        # AMO returned 204 / empty body — lead is deleted or trashed.
                        # Mark as handled so this lead never re-enters the polling loop.
                        _log.warning(
                            "Lead %s returned no pipeline_id from AMO (204/deleted/trashed) — "
                            "suppressing status sync and marking as handled to stop polling loop",
                            lead_id,
                        )
                        self.remember_sheet_status(lead_id, status_name)
                        continue

                # Translate the sheet status to the AMO display name we want to target
                amo_lookup = SHEET_STATUS_TO_AMO_DISPLAY.get(status_name, status_name)

                # When an operator explicitly sets the row back to "В процессе" and the
                # lead already has a Заказ №, push to ЗАКАЗ ОТПРАВЛЕН.  Without an
                # order number the standard "В процессе" lookup reaches ЗАКАЗ БЕЗ НУМЕРАЦИИ.
                if status_name == "В процессе" and order_number:
                    amo_lookup = ORDER_NUM_FILLED_AMO_STATUS_DISPLAY  # "У курера" → ЗАКАЗ ОТПРАВЛЕН

                status_id = self.pipeline_status_display_to_id.get(lead_pipeline_id, {}).get(amo_lookup)
                if not status_id:
                    status_id = self.pipeline_status_name_to_id.get(lead_pipeline_id, {}).get(amo_lookup)
                if not status_id:
                    status_id = self.cfg.STATUS_MAP.get(amo_lookup)

                if not status_id:
                    _log.warning(
                        "No status ID mapping for lead %s, sheet status '%s', pipeline %d",
                        lead_id, status_name, lead_pipeline_id,
                    )
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
                if "Lead not found" in str(exc):
                    _log.warning(
                        "Lead %s no longer exists in AMO (deleted/merged) — "
                        "suppressing status sync and marking as handled to stop retries",
                        lead_id,
                    )
                    self.remember_sheet_status(lead_id, status_name)
                else:
                    _log.error("Failed to sync sheet→amo for lead %s: %s", lead_id, exc)
        # One disk write for all status updates in this poll cycle
        self.flush_state()


# ────────────────────────────────────────────────────────────────────────────────
class DashboardContext:
    """Тонкий read-only фасад над SyncService — всё, что нужно dashboard_router.

    Дашборд никогда не получает прямой доступ к SheetSync: опасные операции
    Google Sheets (запись, создание листов) инкапсулированы в get_staff_list().
    """

    def __init__(self, svc: "SyncService") -> None:
        self.kpi_store               = svc.kpi_store
        self.amo                     = svc.amo
        self.cfg                     = svc.cfg
        # Live references — always reflect the latest AMO structure
        self.pipeline_id_to_name     = svc.pipeline_id_to_name
        self.status_id_to_display_name = svc.status_id_to_display_name
        self._sheet                  = svc.sheet  # private, never exposed directly
        self._svc                    = svc  # for scheduler-triggered operations

    def run_nightly_snapshot(self, date_str: str) -> None:
        """Trigger a KPI re-fetch + snapshot freeze for the given date."""
        self._svc.run_nightly_snapshot(date_str)

    def run_daily_catchup(self, date_str: str) -> Dict[str, int]:
        """Trigger a KPI re-fetch (no snapshot) for the given date."""
        return self._svc.run_daily_catchup(date_str)

    def get_staff_list(self) -> Dict[str, Dict]:
        """Return {code → {code, group, full_name}} read from the Staff worksheet.

        Callers are responsible for caching the result if rapid repeated
        calls must be avoided (dashboard_router has its own TTL cache).
        """
        ws = self._sheet._get_or_create_sheet("Staff")
        rows = ws.get_all_values()
        out: Dict[str, Dict] = {}
        for row in rows[1:]:
            if len(row) < 3:
                continue
            code      = str(row[1]).strip()
            full_name = str(row[2]).strip()
            dept      = str(row[3]).strip() if len(row) >= 4 else ""
            if not full_name or not code:
                continue
            info = {"code": code, "group": dept, "full_name": full_name}
            out[code] = info
            try:
                out[str(int(code))] = info
            except ValueError:
                pass
        return out


service = SyncService()
app = FastAPI(title="amoCRM <-> Google Sheets Sync")

# ── Webhook processing queue ─────────────────────────────────────────────────
# AMO disables webhooks when the endpoint doesn't respond within ~5 seconds.
# We return 200 immediately and process leads asynchronously in this queue.
_webhook_queue: queue.Queue = queue.Queue()


def _webhook_worker() -> None:
    """Background thread: drains _webhook_queue and processes each batch."""
    while True:
        try:
            leads = _webhook_queue.get()
            try:
                if not leads:
                    _log_wh.debug("WEBHOOK WORKER: received empty batch, nothing to process")
                else:
                    service.process_webhook_leads(leads)
            except Exception as exc:
                _log.error("Webhook worker error: %s", exc)
            finally:
                _webhook_queue.task_done()
        except Exception as exc:
            _log.error("Webhook worker fatal error: %s", exc)


threading.Thread(target=_webhook_worker, daemon=True, name="webhook-worker").start()

# Mount staff KPI dashboard via a read-only DashboardContext facade
# (dashboard_router never touches SheetSync write methods directly)
app.include_router(create_dashboard_router(DashboardContext(service)))


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
            _log.error("Initial date-range sync failed: %s", exc)

    # Retry bootstrap — back off on quota errors, log-and-continue on everything else.
    # We never let bootstrap crash the process: a failed bootstrap means the poll
    # worker starts with empty state (safe) rather than killing the service and
    # triggering a systemd restart that would cause AMO to blacklist the webhook.
    for attempt in range(1, 6):
        try:
            service.bootstrap_sheet_state()
            break
        except Exception as exc:
            msg = str(exc)
            if "429" in msg or "Quota exceeded" in msg:
                wait = attempt * 30
                _log.warning("Sheets quota hit during bootstrap (attempt %d), retrying in %ds...", attempt, wait)
                time.sleep(wait)
            elif attempt < 5:
                _log.error("Bootstrap attempt %d failed (%s) — retrying in 30s...", attempt, exc)
                time.sleep(30)
            else:
                _log.error("Bootstrap failed after 5 attempts (%s) — starting with empty state", exc)

    def worker() -> None:
        backoff = 0
        _catch_up_interval = max(1, 600 // max(1, service.cfg.SYNC_POLL_SECONDS))  # every ~10 min
        _catch_up_counter  = 0  # fire on very first cycle, then every ~10 min
        while True:
            try:
                service.check_and_rotate_sheet()
                service.expire_finished_leads()
                service.sync_sheet_to_amo()
                # Periodically poll AMO for leads in trigger status to catch any
                # that were missed because AMO stopped retrying webhook delivery
                # after consecutive failures (e.g. during a service restart).
                _catch_up_counter -= 1
                if _catch_up_counter <= 0:
                    service.catch_up_trigger_leads()
                    _catch_up_counter = _catch_up_interval
                backoff = 0
            except Exception as exc:
                msg = str(exc)
                _log.error("Sheet sync worker error: %s", msg)
                if "429" in msg or "Quota exceeded" in msg:
                    backoff = min(backoff + 60, 300)
                    _log.warning("Sheets quota hit — backing off %ds", backoff)
                    time.sleep(backoff)
                    continue
            time.sleep(service.cfg.SYNC_POLL_SECONDS)

    t = threading.Thread(target=worker, daemon=True)
    t.start()

    # ── KPI backfill (runs in background so startup is not blocked) ─────────
    kpi_backfill_date = os.getenv("KPI_BACKFILL_DATE", "").strip()
    if kpi_backfill_date and not service.kpi_store.is_backfill_done(kpi_backfill_date):
        def _run_backfill():
            try:
                from datetime import date as _date
                today_str = _date.today().strftime("%Y-%m-%d")
                _log.info("KPI backfill starting: %s → %s", kpi_backfill_date, today_str)
                counts = service.run_kpi_backfill(kpi_backfill_date, today_str)
                service.kpi_store.mark_backfill_done(kpi_backfill_date, today_str)
                _log.info("KPI backfill complete: %s", counts)
            except Exception as _exc:
                _log.error("KPI backfill failed: %s", _exc)
        threading.Thread(target=_run_backfill, daemon=True).start()

    # ── Twice-daily KPI scheduler ────────────────────────────────────────────
    # 13:00–14:00: midday live catch-up  (no snapshot, just refreshes kpi_events)
    # 23:00–00:00: nightly snapshot       (catch-up + freeze into manager_snapshots)
    # IMPORTANT: Use a SEPARATE file for KPI scheduler state so it never
    # collides with / overwrites the sync service state in .sync_state.json.
    _kpi_state_file = os.path.join(os.path.dirname(__file__), ".kpi_sched_state.json")

    def _load_sched_state() -> dict:
        try:
            with open(_kpi_state_file) as _f:
                return json.load(_f)
        except Exception:
            return {}

    def _save_sched_state(state: dict) -> None:
        try:
            with open(_kpi_state_file, "w") as _f:
                json.dump(state, _f)
        except Exception as _exc:
            _log.warning("Could not save sched state: %s", _exc)

    def _kpi_scheduler() -> None:
        import datetime as _dt
        tz_offset = getattr(service.cfg, "DISPLAY_TZ_OFFSET", 5)
        tz = _dt.timezone(_dt.timedelta(hours=tz_offset))
        state = _load_sched_state()
        while True:
            try:
                now = _dt.datetime.now(tz)
                today_str = now.strftime("%Y-%m-%d")
                hour = now.hour

                if 13 <= hour < 14 and state.get("kpi_midday_done") != today_str:
                    state["kpi_midday_done"] = today_str
                    _save_sched_state(state)
                    threading.Thread(
                        target=service.run_daily_catchup,
                        args=(today_str,),
                        daemon=True,
                        name="kpi-midday",
                    ).start()

                if 23 <= hour < 24 and state.get("kpi_nightly_done") != today_str:
                    state["kpi_nightly_done"] = today_str
                    _save_sched_state(state)
                    threading.Thread(
                        target=service.run_nightly_snapshot,
                        args=(today_str,),
                        daemon=True,
                        name="kpi-nightly",
                    ).start()

            except Exception as _exc:
                _log.error("KPI scheduler error: %s", _exc)
            time.sleep(60)

    threading.Thread(target=_kpi_scheduler, daemon=True, name="kpi-scheduler").start()


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"status": "ok"}


@app.post("/api/kpi/backfill")
async def kpi_backfill_endpoint(request: Request) -> Dict[str, Any]:
    """Manually trigger a KPI back-fill for a date range.

    Body JSON: {"date_from": "YYYY-MM-DD", "date_to": "YYYY-MM-DD"}
    Runs synchronously; may take a while for large date ranges.
    """
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    from datetime import date as _date
    date_from = (payload.get("date_from") or "").strip()
    date_to   = (payload.get("date_to")   or _date.today().strftime("%Y-%m-%d")).strip()
    if not date_from:
        return {"status": "error", "message": "date_from is required (YYYY-MM-DD)"}
    try:
        counts = service.run_kpi_backfill(date_from, date_to)
        service.kpi_store.mark_backfill_done(date_from, date_to)
        return {"status": "ok", "date_from": date_from, "date_to": date_to, **counts}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.post("/api/kpi/reset")
async def kpi_reset_endpoint(request: Request) -> Dict[str, Any]:
    """Wipe ALL KPI data and re-run a full backfill for the given date range.

    Body JSON: {"date_from": "YYYY-MM-DD", "date_to": "YYYY-MM-DD"}
    WARNING: this deletes every existing KPI event before re-filling.
    """
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    from datetime import date as _date
    date_from = (payload.get("date_from") or "").strip()
    date_to   = (payload.get("date_to")   or _date.today().strftime("%Y-%m-%d")).strip()
    if not date_from:
        return {"status": "error", "message": "date_from is required (YYYY-MM-DD)"}
    try:
        service.kpi_store.clear_all_data()
        counts = service.run_kpi_backfill(date_from, date_to)
        service.kpi_store.mark_backfill_done(date_from, date_to)
        return {"status": "ok", "date_from": date_from, "date_to": date_to, **counts}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.get("/api/kpi/raw")
def kpi_raw_events(
    date_from: str = "",
    date_to:   str = "",
) -> Dict[str, Any]:
    """Return raw KPI events for a date range (for debugging / audit)."""
    from datetime import date as _date
    if not date_from:
        date_from = _date.today().strftime("%Y-%m-%d")
    if not date_to:
        date_to = date_from
    events = service.kpi_store.get_daily_events(date_from, date_to)
    return {"date_from": date_from, "date_to": date_to, "count": len(events), "events": events}



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
    client_ip = request.headers.get("x-forwarded-for", request.client.host if request.client else "unknown")
    _log_wh.debug(
        "WEBHOOK ARRIVED: ip=%s content_type=%s body_size=%d",
        client_ip, content_type, len(raw),
    )
    payload = parse_payload(raw, content_type)
    leads = extract_leads(payload)
    if leads:
        status_ids = [int(l.get("status_id", 0) or 0) for l in leads]
        lead_ids   = [str(l.get("id", "")) for l in leads]
        _log_wh.info(
            "WEBHOOK PARSED: %d lead(s) extracted — ids=%s status_ids=%s",
            len(leads), lead_ids, status_ids,
        )
    else:
        _log_wh.warning(
            "WEBHOOK ARRIVED but 0 leads extracted — content_type=%s raw_body=%s",
            content_type, raw[:500].decode("utf-8", errors="replace"),
        )
    # Enqueue for async processing — return 200 immediately so AMO never
    # marks this webhook as failed due to a slow response.
    _webhook_queue.put(leads)
    return {"status": "ok"}


@app.post("/")
async def webhook_root(request: Request) -> Dict[str, Any]:
    return await webhook_amocrm(request)


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("sync_service:app", host=host, port=port, reload=True)
