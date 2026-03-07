"""Diagnose loop for lead 33762109."""
import json, sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
sys.path.insert(0, str(Path(__file__).parent))
from env_loader import load_env
load_env()
from sync_service import Config, TokenStore, AmoClient

cfg = Config()
ts  = TokenStore(Config.TOKEN_STORE_PATH)
amo = AmoClient(cfg, ts)

LEAD_ID = 33762109
TZ = timezone(timedelta(hours=5))

def fmt(ts): return datetime.fromtimestamp(ts, TZ).strftime("%Y-%m-%d %H:%M:%S") if ts else "?"

# --- Current lead state ---
lead = amo.get(f"/api/v4/leads/{LEAD_ID}?with=contacts,companies")
print(f"\n=== LEAD {LEAD_ID} CURRENT STATE ===")
print(f"  Name       : {lead.get('name')}")
print(f"  pipeline_id: {lead.get('pipeline_id')}")
print(f"  status_id  : {lead.get('status_id')}")
print(f"  updated_at : {fmt(lead.get('updated_at',0))}")

# --- AMO event history ---
print(f"\n=== AMO EVENTS (status changes, last 90 days) ===")
all_events = []
page = 1
import time as _time
ts_from = int(_time.time()) - 90 * 86400
ts_to   = int(_time.time()) + 3600
while True:
    data = amo.get(
        f"/api/v4/events?filter[type][]=lead_status_changed"
        f"&filter[created_at][from]={ts_from}"
        f"&filter[created_at][to]={ts_to}"
        f"&limit=250&page={page}"
    )
    batch = (data.get("_embedded") or {}).get("events") or []
    if not batch:
        break
    # filter to only our lead
    for ev in batch:
        if str(ev.get("entity_id", "")) == str(LEAD_ID):
            all_events.append(ev)
    if not (data.get("_links") or {}).get("next"):
        break
    page += 1

# Sort ascending
all_events.sort(key=lambda e: e.get("created_at", 0))
for ev in all_events:
    ts_val = ev.get("created_at", 0)
    by     = ev.get("created_by", "?")
    vvv    = (ev.get("value_after") or [{}])
    val_a  = vvv[0].get("lead_status", {}) if vvv else {}
    status_name = val_a.get("name", "?")
    status_id   = val_a.get("id", "?")
    pipeline    = val_a.get("pipeline_id", "?")
    print(f"  {fmt(ts_val)}  by={by}  → status='{status_name}' (id={status_id}, pipeline={pipeline})")

# --- State file ---
import json as _json
state_path = Path(".sync_state.json")
print(f"\n=== STATE FILE ===")
if state_path.exists():
    state = _json.loads(state_path.read_text("utf-8"))
    lid = str(LEAD_ID)
    print(f"  known_status : {state.get('sheet_status_by_lead',{}).get(lid, 'NOT TRACKED')}")
    print(f"  known_order  : {state.get('sheet_order_number_by_lead',{}).get(lid, 'NOT TRACKED')}")
    print(f"  tab          : {state.get('lead_tab_by_lead',{}).get(lid, 'NOT TRACKED')}")
    print(f"  pipeline     : {state.get('lead_pipeline_by_lead',{}).get(lid, 'NOT TRACKED')}")
    expiry = state.get('lead_expiry',{}).get(lid)
    print(f"  expiry       : {fmt(expiry) if expiry else 'none'}")
else:
    print("  (state file not found)")

# --- Sheet row ---
print(f"\n=== SHEET ROW SEARCH ===")
import gspread, os
gc = gspread.service_account(filename=os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE","prod_gsheet.json"))
sh = gc.open_by_key(os.getenv("GOOGLE_SHEET_ID"))
found = False
for ws in sh.worksheets():
    cells = ws.findall(str(LEAD_ID))
    for cell in cells:
        row = ws.row_values(cell.row)
        print(f"  Sheet='{ws.title}' row={cell.row}: {row}")
        found = True
if not found:
    print("  NOT FOUND in any sheet tab")
