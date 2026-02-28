"""Test: Events API approach — find leads that first entered an order stage on 24.02."""
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

# pipelines + status maps
print("Loading pipelines...")
data = amo_get("/api/v4/leads/pipelines?with=statuses&limit=250")
pipelines = data.get("_embedded", {}).get("pipelines", [])
pid_name  = {int(p["id"]): p["name"] for p in pipelines}
sotuv_ids = {pid for pid, pn in pid_name.items() if "sotuv" in pn.lower()}

# Build map: status_id -> display_name + status_id -> pipeline_id
SDM = {
    "КОНСУЛТАЦИЯ":"Консультация","Консультация":"Консультация",
    "ДУМКА":"Раздумье","Раздумье":"Раздумье","думка":"Раздумье",
    "ЗАКАЗ":"Заказ","Заказ":"Заказ","заказ":"Заказ",
    "NOMERATSIYALANMAGAN ZAKAZ":"В процессе","ЗАКАЗ БЕЗ НУМЕРАЦИИ":"В процессе",
    "ЗАЗАЗ БЕЗ НУМЕРАЦИИ":"В процессе",
    "ЗАКАЗ ОТПРАВЛЕН":"У курера","ЗАКАЗ ОТПАРВЛЕН":"У курера",
    "заказ отпровлен":"У курера",
    "OTKAZ":"Отказ","ОТКАЗ":"Отказ","Отказ":"Отказ","Oтказ":"Отказ",
    "Успешно":"Успешно","Успешно ":"Успешно","Успешно реализовано":"Успешно",
    "Закрыто и не реализовано":"Закрыто и не реализовано",
}
sid_disp   = {}   # status_id -> display_name
sid_to_pid = {}   # status_id -> pipeline_id
ORDER_SIDS = set()  # status IDs that are order stages in sotuv pipelines
ZAKAS_DISPLAY  = {"Заказ", "В процессе", "У курера", "Успешно"}

for p in pipelines:
    pid = int(p["id"])
    for s in p.get("_embedded", {}).get("statuses", []):
        sid  = int(s["id"])
        disp = SDM.get(s["name"], s["name"])
        sid_disp[sid]   = disp
        sid_to_pid[sid] = pid
        if pid in sotuv_ids and disp in ZAKAS_DISPLAY:
            ORDER_SIDS.add(sid)

print(f"Order status IDs in sotuv pipelines: {len(ORDER_SIDS)} statuses")

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
    staff[code] = {"group": row[3].strip(), "name": row[2].strip()}
print(f"Staff: {len(staff)}")

TF = int(datetime(2026, 2, 24, tzinfo=timezone.utc).timestamp())
TT = TF + 86399

# ── Fetch status_changed events for 24.02 ─────────────────────────────────
print("\nFetching lead_status_changed events for 24.02...")
events, page = [], 1
while True:
    ep = (f"/api/v4/events?filter[type][]=lead_status_changed"
          f"&filter[created_at][from]={TF}&filter[created_at][to]={TT}"
          f"&limit=100&page={page}")
    d = amo_get(ep)
    batch = (d.get("_embedded") or {}).get("events") or []
    if not batch:
        break
    events.extend(batch)
    if not (d.get("_links") or {}).get("next"):
        break
    page += 1
    time.sleep(0.15)
print(f"  -> {len(events)} status_changed events on 24.02")

# ── Find leads that FIRST entered an order stage (from non-order) on 24.02 ─
# Only count transitions FROM a non-order stage TO an order stage.
# This excludes: Zakas→V Protsesse, V Protsesse→U Kurera (internal hops).
first_order_event: dict = {}  # lead_id -> event_ts
for ev in events:
    lead_id = int(ev.get("entity_id", 0) or 0)
    before_ls = (ev.get("value_before") or [{}])[0]
    after_ls  = (ev.get("value_after")  or [{}])[0]
    old_sid   = int((before_ls.get("lead_status") or {}).get("id", 0) or 0)
    new_sid   = int((after_ls.get("lead_status")  or {}).get("id", 0) or 0)
    # Only count: transition INTO order stage FROM a non-order stage
    if new_sid in ORDER_SIDS and old_sid not in ORDER_SIDS:
        ev_ts = int(ev.get("created_at", 0) or 0)
        if lead_id not in first_order_event or ev_ts < first_order_event[lead_id]:
            first_order_event[lead_id] = ev_ts

print(f"  -> {len(first_order_event)} leads FIRST entered order stage on 24.02 (from non-order)")


# ── Fetch those leads to get staff code + price ────────────────────────────
order_lead_ids = list(first_order_event.keys())
print(f"\nFetching {len(order_lead_ids)} order leads in batches...")

def fetch_leads_by_ids(ids):
    """Fetch leads by ID list using filter[id][]."""
    leads_out = []
    batch_size = 50
    for i in range(0, len(ids), batch_size):
        chunk = ids[i:i+batch_size]
        id_params = "&".join(f"filter[id][]={lid}" for lid in chunk)
        ep = f"/api/v4/leads?{id_params}&with=contacts,companies&limit=250"
        d  = amo_get(ep)
        batch = (d.get("_embedded") or {}).get("leads") or []
        leads_out.extend(batch)
        time.sleep(0.15)
    return leads_out

order_leads = fetch_leads_by_ids(order_lead_ids)
print(f"  -> fetched {len(order_leads)} order leads")

# Build created_on_24 set from order leads
order_lead_created_24 = {
    int(l["id"]): l for l in order_leads
    if TF <= int(l.get("created_at", 0) or 0) <= TT
}
print(f"  -> {len(order_lead_created_24)} of those were also CREATED on 24.02")

# ── Aggregate zakas by group — ALL leads that first-entered order on 24.02 ──
zakas_all_grp = defaultdict(lambda: {"z": 0, "s": 0.0})
zakas_c24_grp = defaultdict(lambda: {"z": 0, "s": 0.0})  # only created on 24.02

for lead in order_leads:
    pid = int(lead.get("pipeline_id", 0) or 0)
    if pid not in sotuv_ids:
        continue
    lead_id = int(lead["id"])
    budget = float(lead.get("price", 0) or 0)
    cf = lead.get("custom_fields_values") or []
    code = ""
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
    zakas_all_grp[g]["z"] += 1
    zakas_all_grp[g]["s"] += budget
    if lead_id in order_lead_created_24:
        zakas_c24_grp[g]["z"] += 1
        zakas_c24_grp[g]["s"] += budget

# ── Also get consul from created_at (known correct) ───────────────────────
print("\nFetching created_at=24.02 leads for consul...")
consul_leads, page = [], 1
while True:
    ep = (f"/api/v4/leads?filter[created_at][from]={TF}&filter[created_at][to]={TT}"
          f"&with=contacts,companies&limit=250&page={page}")
    d = amo_get(ep)
    batch = (d.get("_embedded") or {}).get("leads") or []
    if not batch:
        break
    consul_leads.extend(batch)
    if not (d.get("_links") or {}).get("next"):
        break
    page += 1
    time.sleep(0.15)
print(f"  -> {len(consul_leads)} leads created on 24.02")

consul_grp = defaultdict(int)
for lead in consul_leads:
    if int(lead.get("pipeline_id", 0) or 0) not in sotuv_ids:
        continue
    cf = lead.get("custom_fields_values") or []
    code = ""
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
    consul_grp[g] += 1

# ── Print comparison ───────────────────────────────────────────────────────
all_g = sorted(set(list(consul_grp.keys()) + list(zakas_all_grp.keys()) + list(zakas_c24_grp.keys())))

def print_table(label, c_grp, z_grp):
    print(f"\n{'='*65}")
    print(f"  {label}")
    tc = tz = 0; ts = 0.0
    for g in all_g:
        c = c_grp.get(g, 0)
        z = z_grp.get(g, {}).get("z", 0)
        s = z_grp.get(g, {}).get("s", 0.0)
        conv = round(z / c * 100, 1) if c else 0.0
        print(f"  [{g:4}]  consul={c:>4}  zakas={z:>3}  summa={s/1e6:>6.2f}M  conv={conv}%")
        tc += c; tz += z; ts += s
    tot_conv = round(tz / tc * 100, 1) if tc else 0.0
    print(f"  [TOT ]  consul={tc:>4}  zakas={tz:>3}  summa={ts/1e6:>6.2f}M  conv={tot_conv}%")

print_table(
    "TEST A: consul=created_at  |  zakas=first-entry-to-order on 24.02 (any created date)",
    consul_grp, zakas_all_grp
)
print_table(
    "TEST B: consul=created_at  |  zakas=first-entry-to-order on 24.02 (only if also created 24.02)",
    consul_grp, zakas_c24_grp
)

print(f"\n{'='*65}")
print("  REFERENCE (24.02.2026):")
print("  [A   ]  consul= 107  zakas= 44  summa= 43.47M  conv=41.1%")
print("  [B   ]  consul=  74  zakas= 30  summa= 28.45M  conv=40.5%")
print("  [C   ]  consul=  36  zakas= 24  summa= 25.55M  conv=66.7%")
print("  [D   ]  consul=  13  zakas=  8  summa=  7.40M")
print("  [TOT ]  consul= 230  zakas=106  summa=104.87M")
print("="*65)

