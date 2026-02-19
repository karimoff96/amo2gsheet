import json
import os
import re
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import gspread
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, Request

load_dotenv()


COLUMNS = [
    "ID",
    "Ф.И.О.",
    "Контактный номер",
    "Компания",
    "№",
    "Дата Заказа",
    "Дата доставки",
    "Оператор (ПИН)",
    "Bo'lim",
    "Товар1",
    "кол-во1",
    "Товар2",
    "кол-во2",
    "Товар3",
    "кол-во3",
    "Сумма",
    "Регион",
    "Адрес",
    "статус",
    "Логистика",
    "Контакт",
    "Источник",
]

ID_COL_INDEX = COLUMNS.index("ID")
STATUS_COL_INDEX = COLUMNS.index("статус")


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

    SYNC_POLL_SECONDS = int(os.getenv("SYNC_POLL_SECONDS", "10"))


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
        tokens = self._token_data()
        access_token = tokens.get("access_token", "")
        refresh_token = tokens.get("refresh_token", "")

        if self._is_token_valid(access_token):
            return access_token
        if not refresh_token and self.cfg.AMO_AUTH_CODE:
            print("[INFO] No refresh token found, trying AMO_AUTH_CODE bootstrap...")
            try:
                data = self.exchange_code(self.cfg.AMO_AUTH_CODE)
                return data["access_token"]
            except Exception as exc:
                raise RuntimeError(f"AMO_AUTH_CODE bootstrap failed: {exc}")
        return self._refresh(refresh_token)

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
        r = requests.get(
            f"{self.base_url}{endpoint}",
            headers=self._headers(token),
            timeout=30,
        )
        if r.status_code >= 400:
            raise RuntimeError(f"GET {endpoint} failed: {r.status_code} {r.text}")
        return r.json()

    def patch(self, endpoint: str, body: Dict[str, Any]) -> Dict[str, Any]:
        token = self.get_access_token()
        r = requests.patch(
            f"{self.base_url}{endpoint}",
            headers={**self._headers(token), "Content-Type": "application/json"},
            json=body,
            timeout=30,
        )
        if r.status_code >= 400:
            raise RuntimeError(f"PATCH {endpoint} failed: {r.status_code} {r.text}")
        return r.json() if r.text else {}


class SheetSync:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.gc = gspread.service_account(filename=cfg.GOOGLE_SERVICE_ACCOUNT_FILE)
        self.spreadsheet = self.gc.open_by_key(cfg.GOOGLE_SHEET_ID)
        self.ws = self._get_or_create_sheet(cfg.GOOGLE_WORKSHEET_NAME)
        self.ensure_headers()
        self.lock = threading.Lock()

    def _get_or_create_sheet(self, name: str):
        try:
            return self.spreadsheet.worksheet(name)
        except gspread.WorksheetNotFound:
            return self.spreadsheet.add_worksheet(title=name, rows=2000, cols=max(26, len(COLUMNS)))

    def ensure_headers(self) -> None:
        first_row = self.ws.row_values(1)
        if first_row != COLUMNS:
            self.ws.update("A1", [COLUMNS])
            self.ws.freeze(rows=1)

    def _all_rows(self) -> List[List[str]]:
        values = self.ws.get_all_values()
        if not values:
            return []
        return values[1:]

    def find_row(self, lead_id: str) -> Optional[int]:
        rows = self._all_rows()
        for idx, row in enumerate(rows, start=2):
            if len(row) > ID_COL_INDEX and str(row[ID_COL_INDEX]).strip() == str(lead_id):
                return idx
        return None

    def upsert_row(self, row_data: List[Any]) -> int:
        lead_id = str(row_data[ID_COL_INDEX])
        with self.lock:
            row_num = self.find_row(lead_id)
            if row_num:
                self.ws.update(f"A{row_num}", [row_data])
                return row_num
            self.ws.append_row(row_data, value_input_option="USER_ENTERED")
            return len(self._all_rows()) + 1

    def update_status(self, lead_id: str, status_name: str) -> None:
        with self.lock:
            row_num = self.find_row(lead_id)
            if not row_num:
                return
            col = STATUS_COL_INDEX + 1
            self.ws.update_cell(row_num, col, status_name)

    def iter_lead_statuses(self) -> List[Dict[str, str]]:
        rows = self._all_rows()
        out: List[Dict[str, str]] = []
        for row in rows:
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
    pattern = re.compile(r"^leads\[(?:add|update|status)\]\[(\d+)\]\[(.+)\]$")

    for key, value in data.items():
        m = pattern.match(key)
        if not m:
            continue
        idx, field = m.groups()
        grouped.setdefault(idx, {})[field] = value

    return list(grouped.values())


def build_row(lead: Dict[str, Any], status_name: str) -> List[Any]:
    mapped: Dict[str, Any] = {
        "ID": lead.get("id", ""),
        "Сумма": lead.get("price", ""),
        "статус": status_name,
        "Ф.И.О.": lead.get("name", ""),
    }

    if isinstance(lead.get("custom_fields_values"), list):
        for cf in lead["custom_fields_values"]:
            field_name = cf.get("field_name")
            values = cf.get("values") or []
            if field_name in COLUMNS and values:
                mapped[field_name] = values[0].get("value", "")

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
        self._load_structure_mappings()
        self._print_config_warnings()

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
            statuses = pipeline.get("_embedded", {}).get("statuses", [])
            if pipeline_id not in self.pipeline_status_name_to_id:
                self.pipeline_status_name_to_id[pipeline_id] = {}

            for status in statuses:
                status_name = str(status.get("name", "")).strip()
                status_id = int(status.get("id", 0) or 0)
                if not status_id or not status_name:
                    continue

                self.pipeline_status_name_to_id[pipeline_id][status_name] = status_id

                if status_name == self.cfg.TRIGGER_STATUS_NAME:
                    self.trigger_status_ids.add(status_id)

                if status_name in self.cfg.STATUS_MAP:
                    self.terminal_status_id_to_name[str(status_id)] = status_name

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

    def process_webhook_leads(self, leads: List[Dict[str, Any]]) -> Dict[str, Any]:
        written = 0
        trigger_matches = 0
        terminal_matches = 0
        skipped_no_id = 0
        skipped_status_mismatch = 0
        seen_status_ids: List[int] = []

        for lead in leads:
            lead_id = str(lead.get("id", "")).strip()
            if not lead_id:
                skipped_no_id += 1
                continue

            status_id = int(lead.get("status_id", 0) or 0)
            seen_status_ids.append(status_id)

            if status_id in self.trigger_status_ids:
                trigger_matches += 1
                try:
                    full_lead = self.amo.get(f"/api/v4/leads/{lead_id}")
                except Exception:
                    full_lead = lead

                row = build_row(full_lead, self.cfg.TRIGGER_STATUS_NAME)
                self.sheet.upsert_row(row)
                self.remember_sheet_status(lead_id, self.cfg.TRIGGER_STATUS_NAME)
                written += 1
                continue

            terminal_name = self.terminal_status_id_to_name.get(str(status_id))
            if terminal_name:
                terminal_matches += 1
                self.sheet.update_status(lead_id, terminal_name)
                self.remember_sheet_status(lead_id, terminal_name)
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

                status_id = self.pipeline_status_name_to_id.get(lead_pipeline_id, {}).get(status_name)
                if not status_id:
                    status_id = self.cfg.STATUS_MAP.get(status_name)

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
    service.bootstrap_sheet_state()

    def worker() -> None:
        while True:
            try:
                service.sync_sheet_to_amo()
            except Exception as exc:
                print(f"Sheet sync worker error: {exc}")
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
