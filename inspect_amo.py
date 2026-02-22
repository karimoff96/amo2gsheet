import json
import requests

token_data = json.loads(open(".amo_tokens.json", encoding="utf-8").read())
TOKEN = token_data["access_token"]
BASE = "https://bioflextest.amocrm.ru"
H = {"Authorization": f"Bearer {TOKEN}"}


# ── Pipelines & Statuses ──────────────────────────────────────────────────────
print("=" * 60)
print("PIPELINES & STATUSES")
print("=" * 60)
r = requests.get(f"{BASE}/api/v4/leads/pipelines?with=statuses&limit=250", headers=H, timeout=30)
data = r.json()
for p in data.get("_embedded", {}).get("pipelines", []):
    print(f"  Pipeline [{p['id']}]: {p['name']}")
    for s in p.get("_embedded", {}).get("statuses", []):
        print(f"    Status  [{s['id']}]: {s['name']}")


# ── Lead Custom Fields ────────────────────────────────────────────────────────
print()
print("=" * 60)
print("LEAD CUSTOM FIELDS")
print("=" * 60)
r2 = requests.get(f"{BASE}/api/v4/leads/custom_fields?limit=250", headers=H, timeout=30)
d2 = r2.json()
for cf in d2.get("_embedded", {}).get("custom_fields", []):
    opts = ""
    if cf.get("enums"):
        opts = "  values: " + ", ".join(e.get("value", "") for e in cf["enums"])
    print(f"  [{cf['id']}] {cf.get('name', cf.get('field_name', '?'))}  (type={cf.get('type', cf.get('field_type', '?'))}){opts}")


# ── Sample lead (first one found) ────────────────────────────────────────────
print()
print("=" * 60)
print("SAMPLE LEAD (first lead, all raw fields)")
print("=" * 60)
r3 = requests.get(
    f"{BASE}/api/v4/leads?with=contacts,companies,tags&limit=1",
    headers=H,
    timeout=30,
)
leads = r3.json().get("_embedded", {}).get("leads", [])
if leads:
    lead = leads[0]
    print(f"  id          : {lead.get('id')}")
    print(f"  name        : {lead.get('name')}")
    print(f"  price       : {lead.get('price')}")
    print(f"  pipeline_id : {lead.get('pipeline_id')}")
    print(f"  status_id   : {lead.get('status_id')}")
    print(f"  responsible : {lead.get('responsible_user_id')}")
    print(f"  created_at  : {lead.get('created_at')}")
    print(f"  updated_at  : {lead.get('updated_at')}")
    print()
    print("  --- custom_fields_values ---")
    for cf in lead.get("custom_fields_values") or []:
        vals = [v.get("value") for v in (cf.get("values") or [])]
        print(f"    [{cf['field_id']}] {cf['field_name']}: {vals}")
    print()
    print("  --- tags ---")
    for t in (lead.get("_embedded") or {}).get("tags") or []:
        print(f"    {t.get('name')}")
else:
    print("  No leads found in account.")
