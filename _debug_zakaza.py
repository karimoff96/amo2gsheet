"""Test: does filtering by Data zakaza (field 981185) = 24.02 give zakas=106?"""
from env_loader import load_env
load_env()

import os, json, time, requests, gspread
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

SUBDOMAIN  = os.getenv("AMO_SUBDOMAIN", "")
TOKEN_PATH = Path(os.getenv("AMO_TOKEN_STORE", ".amo_tokens.json"))
GS_FILE    = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "prod_gsheet.json")
GS_ID      = os.getenv("GOOGLE_SHEET_ID", "")
BASE       = f"https://{SUBDOMAIN}.amocrm.ru"

tokens = json.loads(TOKEN_PATH.read_text(encoding="utf-8"))
HDR    = {"Authorization": "Bearer " + tokens["access_token"]}

def amo_get(ep):
    r = requests.get(f"{BASE}{ep}", headers=HDR, timeout=30)
    if r.status_code == 204 or not r.text:
        return {}
    r.raise_for_status()
    return r.json()

# pipelines
print("Loading pipelines...")
data = amo_get("/api/v4/leads/pipelines?with=statuses&limit=250")
pipelines = data.get("_embedded", {}).get("pipelines", [])
pid_name  = {int(p["id"]): p["name"] for p in pipelines}
sotuv_ids = {pid for pid, pn in pid_name.items() if "sotuv" in pn.lower()}

# staff
print("Loading staff sheet...")
gc    = gspread.service_account(filename=GS_FILE)
sp    = gc.open_by_key(GS_ID)
staff = {}
for row in sp.worksheet("Staff").get_all_values()[1:]:
    if len(row) < 4:
        continue
    try:
        code = str(int(row[1].strip()))
    except ValueError:
        continue
    staff[code] = {"group": row[3].strip()}
print(f"Staff: {len(staff)}")

# --- Test 1: Дата Заказа (field 981185) = 24.02 ---------------------------
TF = int(datetime(2026, 2, 24, tzinfo=timezone.utc).timestamp())
TT = TF + 86399

def data_zakaza_ts(lead):
    """Return the Unix timestamp of Дата заказа (field 981185), or None."""
    for f in (lead.get("custom_fields_values") or []):
        if f.get("field_id") == 981185:
            vals = f.get("values") or []
            if vals:
                return int(vals[0].get("value", 0) or 0)
    return None

# Fetch all updated_at=24.02 leads, then post-filter by Дата заказа = 24.02
print("\nFetching updated_at=24.02 leads (will post-filter by Data Zakaza)...")
leads_upd, page = [], 1
while True:
    ep = (f"/api/v4/leads?filter[updated_at][from]={TF}"
          f"&filter[updated_at][to]={TT}"
          f"&with=contacts,companies&limit=250&page={page}")
    d = amo_get(ep)
    batch = (d.get("_embedded") or {}).get("leads") or []
    if not batch:
        break
    leads_upd.extend(batch)
    if not (d.get("_links") or {}).get("next"):
        break
    page += 1
    time.sleep(0.2)
leads_dz = [l for l in leads_upd if TF <= (data_zakaza_ts(l) or 0) <= TT]
print(f"  -> {len(leads_upd)} updated_at leads, {len(leads_dz)} have Data Zakaza=24.02")

grp = defaultdict(lambda: {"z": 0, "s": 0.0})
for lead in leads_dz:
    if int(lead.get("pipeline_id", 0) or 0) not in sotuv_ids:
        continue
    budget = float(lead.get("price", 0) or 0)
    cf     = lead.get("custom_fields_values") or []
    code   = ""
    for f in cf:
        if "сотрудника" in (f.get("field_name") or "").lower():
            v = f.get("values") or []
            if v:
                code = str(v[0].get("value", "")).strip()
            break
    try:
        code = str(int(code))
    except ValueError:
        continue
    if code not in staff:
        continue
    g = staff[code]["group"]
    grp[g]["z"] += 1
    grp[g]["s"] += budget

tz = 0
ts = 0.0
print("\nData Zakaza=24.02 | zakas by group:")
for g in sorted(grp):
    v = grp[g]
    print(f"  [{g:4}]  zakas={v['z']:>3}  summa={v['s']/1e6:>6.2f}M")
    tz += v["z"]
    ts += v["s"]
print(f"  [TOT ]  zakas={tz:>3}  summa={ts/1e6:>6.2f}M")

print()
print("REFERENCE (24.02.2026):")
print("  [A   ]  zakas= 44  summa= 43.47M")
print("  [B   ]  zakas= 30  summa= 28.45M")
print("  [C   ]  zakas= 24  summa= 25.55M")
print("  [D   ]  zakas=  8  summa=  7.40M")
print("  [TOT ]  zakas=106  summa=104.87M")
