"""Standalone debug: compare created_at vs updated_at for 24.02.2026
Does NOT import sync_service (avoids triggering SyncService() at module level).
"""
from env_loader import load_env
load_env()

import os, json, time, requests, gspread
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

# ── credentials ───────────────────────────────────────────────────────────────
SUBDOMAIN  = os.getenv("AMO_SUBDOMAIN","")
TOKEN_PATH = Path(os.getenv("AMO_TOKEN_STORE", ".amo_tokens.json"))
GS_FILE    = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE","prod_gsheet.json")
GS_ID      = os.getenv("GOOGLE_SHEET_ID","")
BASE       = f"https://{SUBDOMAIN}.amocrm.ru"

tokens = json.loads(TOKEN_PATH.read_text(encoding="utf-8"))
ACCESS = tokens["access_token"]
HDR    = {"Authorization": f"Bearer {ACCESS}"}

def amo_get(ep):
    r = requests.get(f"{BASE}{ep}", headers=HDR, timeout=30)
    if r.status_code == 204 or not r.text: return {}
    if r.status_code >= 400:
        raise RuntimeError(f"GET {ep} -> {r.status_code} {r.text[:200]}")
    return r.json()

# STATUS_DISPLAY_MAP (inline minimal copy)
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

# ── Pipeline / status maps ────────────────────────────────────────────────────
data = amo_get("/api/v4/leads/pipelines?with=statuses&limit=250")
pipelines = data.get("_embedded",{}).get("pipelines",[])
pid_name  = {int(p["id"]): p["name"] for p in pipelines}
sid_disp  = {}
for p in pipelines:
    for s in p.get("_embedded",{}).get("statuses",[]):
        sid_disp[int(s["id"])] = SDM.get(s["name"], s["name"])
sotuv_ids = {pid for pid,pn in pid_name.items() if "sotuv" in pn.lower()}
print(f"Sotuv pipelines: {[pid_name[i] for i in sotuv_ids]}")

# ── Staff sheet ───────────────────────────────────────────────────────────────
gc  = gspread.service_account(filename=GS_FILE)
sp  = gc.open_by_key(GS_ID)
sws = sp.worksheet("Staff")
staff = {}
for row in sws.get_all_values()[1:]:
    if len(row) < 4: continue
    try: code = str(int(row[1].strip()))
    except: continue
    staff[code] = {"name": row[2].strip(), "group": row[3].strip()}
print(f"Staff loaded: {len(staff)} entries")

ZAKAS_ALL    = {"Заказ","В процессе","У курера","Успешно"}
ZAKAS_STRICT = {"Заказ"}

def fetch_leads_paged(filter_key, ts_from, ts_to):
    leads, page = [], 1
    while True:
        ep = (f"/api/v4/leads?filter[{filter_key}][from]={ts_from}"
              f"&filter[{filter_key}][to]={ts_to}"
              f"&with=contacts,companies&limit=250&page={page}")
        d = amo_get(ep)
        batch = (d.get("_embedded") or {}).get("leads") or []
        if not batch: break
        leads.extend(batch)
        if not (d.get("_links") or {}).get("next"): break
        page += 1
        time.sleep(0.2)
    return leads

def analyze(leads, zakas_set, label):
    grp = defaultdict(lambda: {"c":0,"z":0,"s":0.0})
    for lead in leads:
        if int(lead.get("pipeline_id",0) or 0) not in sotuv_ids: continue
        disp   = sid_disp.get(int(lead.get("status_id",0) or 0),"")
        budget = float(lead.get("price",0) or 0)
        cf     = lead.get("custom_fields_values") or []
        code   = ""
        for f in cf:
            if " ".join((f.get("field_name") or "").split()) == "Код сотрудника":
                v = f.get("values") or []
                if v: code = str(v[0].get("value","")).strip()
                break
        try: code = str(int(code))
        except: continue
        if code not in staff: continue
        g = staff[code]["group"]
        grp[g]["c"] += 1
        if disp in zakas_set:
            grp[g]["z"] += 1
            grp[g]["s"] += budget
    tc = tz = 0; ts = 0.0
    print(f"\n{'='*55}")
    print(f"  {label}  (total fetched: {len(leads)})")
    for g in sorted(grp):
        v = grp[g]
        cv = round(v["z"]/v["c"]*100,1) if v["c"] else 0
        print(f"  [{g:4}]  consul={v['c']:>4}  zakas={v['z']:>3}  summa={v['s']/1e6:>6.2f}M  conv={cv}%")
        tc+=v["c"]; tz+=v["z"]; ts+=v["s"]
    cv = round(tz/tc*100,1) if tc else 0
    print(f"  [TOT ]  consul={tc:>4}  zakas={tz:>3}  summa={ts/1e6:>6.2f}M  conv={cv}%")

TF = int(datetime(2026,2,24,tzinfo=timezone.utc).timestamp())
TT = TF + 86399

print("\nFetching created_at leads...")
leads_c = fetch_leads_paged("created_at", TF, TT)
print(f"  -> {len(leads_c)} leads")

print("Fetching updated_at leads...")
leads_u = fetch_leads_paged("updated_at", TF, TT)
print(f"  -> {len(leads_u)} leads")

analyze(leads_c, ZAKAS_ALL,    "created_at=24.02  |  zakas=ALL stages")
analyze(leads_c, ZAKAS_STRICT, "created_at=24.02  |  zakas=STRICT (Заказ only)")
analyze(leads_u, ZAKAS_ALL,    "updated_at=24.02  |  zakas=ALL stages")
analyze(leads_u, ZAKAS_STRICT, "updated_at=24.02  |  zakas=STRICT (Заказ only)")

print("\n" + "="*55)
print("REFERENCE SHEET (24.02.2026):")
print("  [A   ]  consul= 107  zakas= 44  summa= 43.47M  conv=41.1%")
print("  [B   ]  consul=  74  zakas= 30  summa= 28.45M  conv=40.5%")
print("  [C   ]  consul=  36  zakas= 24  summa= 25.55M  conv=66.7%")
print("  [D   ]  consul=  13  zakas=  8  summa=  7.40M")
print("  [TOT ]  consul= 230  zakas=106  summa=104.87M")
