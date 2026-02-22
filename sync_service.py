import json
import os
import re
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import gspread
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from gspread.utils import ValidationConditionType

load_dotenv()


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
    "Ответственный",
]

# Maps raw AmoCRM pipeline name → proper Russian display name written to Google Sheets
PIPELINE_DISPLAY_MAP: Dict[str, str] = {
    "Nilufar - Sotuv Bioflex":   "Нилуфар",
    "NILUFAR - SOTUV BIOFLEX":   "Нилуфар",
    "Munira - Sotuv Bioflex":    "Мунира",
    "MUNIRA - SOTUV BIOFLEX":    "Мунира",
    "Rushana  - Sotuv Bioflex":  "Рушана",
    "Rushana - Sotuv Bioflex":   "Рушана",
    "RUSHANA - SOTUV BIOFLEX":   "Рушана",
    "Baza Uspeshno":             "База (Успешно)",
    "BAZA USPESHNO":             "База (Успешно)",
    "Baza Dumka":                "База (Раздумье)",
    "BAZA DUMKA":                "База (Раздумье)",
    "Akobir - Sotuv Bioflex":    "Акобир",
    "AKOBIR - SOTUV BIOFLEX":    "Акобир",
}

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
STATUS_COL_INDEX = COLUMNS.index("статус")

# Maps what the user picks in Google Sheets → the AMO display name used for status ID lookup.
# Both "У курера" and "Успешно" physically move the lead to "Заказ отправлен" in AMO.
# "Отказ" moves the lead to the pipeline's reject step.
SHEET_STATUS_TO_AMO_DISPLAY: Dict[str, str] = {
    "В процессе": "В процессе",
    "У курера":   "У курера",
    "Успешно":    "У курера",
    "Отказ":      "Отказ",
}


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

    STATUS_MAP = json.loads(os.getenv("DROPDOWN_STATUS_MAP_JSON", "{}"))
    STATUS_ID_TO_NAME = {str(v): k for k, v in STATUS_MAP.items() if v}

    SYNC_POLL_SECONDS = int(os.getenv("SYNC_POLL_SECONDS", "60"))
    # Unix timestamp – webhook leads created BEFORE this are silently ignored. 0 = process all.
    LEADS_CREATED_AFTER = int(os.getenv("LEADS_CREATED_AFTER", "0"))
    # Minimum seconds between consecutive amoCRM API calls. Increase on prod if you see 429s.
    AMO_REQUEST_DELAY_SEC = float(os.getenv("AMO_REQUEST_DELAY_SEC", "0.2"))
    # How long (seconds) the Staff sheet mapping is cached before re-fetching.
    STAFF_CACHE_TTL_SEC = int(os.getenv("STAFF_CACHE_TTL_SEC", "300"))


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
        if r.status_code >= 400:
            raise RuntimeError(f"GET {endpoint} failed: {r.status_code} {r.text}")
        return r.json()

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

    def _get_or_create_sheet(self, name: str):
        if name in self._sheets:
            return self._sheets[name]
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

    def get_staff_mapping(self) -> Dict[str, str]:
        """Fetch the staff mapping from the 'Staff' sheet (result is cached for STAFF_CACHE_TTL_SEC)."""
        now = time.time()
        if self._staff_cache and now - self._staff_cache_ts < self.cfg.STAFF_CACHE_TTL_SEC:
            return self._staff_cache
        try:
            ws = self._get_or_create_sheet("Staff")
            values = ws.get_all_values()
            mapping = {}
            for row in values[1:]:  # Skip header
                if len(row) >= 2:
                    code = str(row[0]).strip()
                    name = str(row[1]).strip()
                    if code and name:
                        # Store the code without leading zeros to ensure matching
                        try:
                            code = str(int(code))
                        except ValueError:
                            pass
                        mapping[code] = name
            self._staff_cache = mapping
            self._staff_cache_ts = now
            return mapping
        except Exception as e:
            print(f"[WARN] Could not load Staff sheet: {e}")
            return self._staff_cache  # Return stale cache on error rather than empty

    # Statuses that can be chosen from the dropdown in the "статус" column
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

    def find_row(self, ws, lead_id: str) -> Optional[int]:
        rows = self._all_rows(ws)
        for idx, row in enumerate(rows, start=2):
            if len(row) > ID_COL_INDEX and str(row[ID_COL_INDEX]).strip() == str(lead_id):
                return idx
        return None

    def upsert_row(self, row_data: List[Any], pipeline_display: str = "") -> int:
        ws = self._sheet_for_pipeline(pipeline_display)
        lead_id = str(row_data[ID_COL_INDEX])
        with self.lock:
            row_num = self.find_row(ws, lead_id)
            if row_num:
                ws.update(values=[row_data], range_name=f"A{row_num}")
                return row_num
            
            all_vals = ws.get_all_values()
            next_row = len(all_vals) + 1
            # Find the first completely empty row to avoid skipping rows if user cleared contents
            for i, row in enumerate(all_vals):
                if i > 0 and not any(str(cell).strip() for cell in row):
                    next_row = i + 1
                    break
                    
            ws.update(values=[row_data], range_name=f"A{next_row}")
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
            if lead_id:
                out.append({"lead_id": lead_id, "status": status})
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
        "статус": display_status,
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
        self.trigger_status_ids: set[int] = set()
        self.terminal_status_id_to_name: Dict[str, str] = {}
        self.pipeline_status_name_to_id: Dict[int, Dict[str, int]] = {}
        self.pipeline_status_display_to_id: Dict[int, Dict[str, int]] = {}
        self.pipeline_id_to_name: Dict[int, str] = {}
        self.status_id_to_display_name: Dict[int, str] = {}
        self.users_map: Dict[int, str] = {}
        self._load_structure_mappings()
        self._load_users()
        self._print_config_warnings()

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

                # Match trigger by raw name OR display name (handles already-translated pipelines)
                trigger_display = STATUS_DISPLAY_MAP.get(
                    self.cfg.TRIGGER_STATUS_NAME, self.cfg.TRIGGER_STATUS_NAME
                )
                if status_name == self.cfg.TRIGGER_STATUS_NAME or display_name == trigger_display:
                    self.trigger_status_ids.add(status_id)

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
        with self.state_lock:
            self.state_path.write_text(
                json.dumps(self.state, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

    def remember_sheet_status(self, lead_id: str, status_name: str) -> None:
        with self.state_lock:
            self.state.setdefault("sheet_status_by_lead", {})[str(lead_id)] = status_name
        self._save_state()

    def get_known_sheet_status(self, lead_id: str) -> str:
        return self.state.get("sheet_status_by_lead", {}).get(str(lead_id), "")

    def bootstrap_sheet_state(self) -> None:
        rows = self.sheet.iter_lead_statuses()
        for item in rows:
            self.remember_sheet_status(item["lead_id"], item["status"])

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

            # Skip leads created before the configured cutoff (useful on prod to ignore old history)
            if self.cfg.LEADS_CREATED_AFTER:
                created_at = int(full_lead.get("created_at", 0) or 0)
                if created_at and created_at < self.cfg.LEADS_CREATED_AFTER:
                    skipped_too_old += 1
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
                written += 1
                continue

            terminal_name = self.terminal_status_id_to_name.get(str(status_id))
            if terminal_name:
                terminal_matches += 1
                lead_pipeline_id = int(full_lead.get("pipeline_id", 0) or 0)
                p_name = self.pipeline_id_to_name.get(lead_pipeline_id, "")
                p_display = PIPELINE_DISPLAY_MAP.get(p_name, p_name)
                self.sheet.update_status(lead_id, terminal_name, p_display)
                self.remember_sheet_status(lead_id, terminal_name)
                written += 1
            else:
                if known_status:
                    new_status_display = self.status_id_to_display_name.get(status_id, str(status_id))
                    lead_pipeline_id = int(full_lead.get("pipeline_id", 0) or 0)
                    p_name = self.pipeline_id_to_name.get(lead_pipeline_id, "")
                    p_display = PIPELINE_DISPLAY_MAP.get(p_name, p_name)
                    self.sheet.update_status(lead_id, new_status_display, p_display)
                    self.remember_sheet_status(lead_id, new_status_display)
                    written += 1
                else:
                    skipped_status_mismatch += 1

        return {
            "received": len(leads),
            "written": written,
            "trigger_matches": trigger_matches,
            "terminal_matches": terminal_matches,
            "skipped_no_id": skipped_no_id,
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


service = SyncService()
app = FastAPI(title="amoCRM <-> Google Sheets Sync")


@app.on_event("startup")
def on_startup() -> None:
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
    uvicorn.run(app, host=host, port=port, reload=False)
