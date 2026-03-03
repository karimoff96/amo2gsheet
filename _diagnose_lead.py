from env_loader import load_env
load_env()

from sync_service import Config, TokenStore, AmoClient, STATUS_DISPLAY_MAP
import datetime, time

cfg = Config()
ts  = TokenStore(cfg.TOKEN_STORE_PATH)
amo = AmoClient(cfg, ts)

LEAD_ID = 37850979

print("=== CONFIG ===")
print(f"  TRIGGER_STATUS_NAME      : {cfg.TRIGGER_STATUS_NAME!r}")
print(f"  TRIGGER_STATUS_NAMES_EXTRA: {cfg.TRIGGER_STATUS_NAMES_EXTRA!r}")
print(f"  PIPELINE_KEYWORD         : {cfg.PIPELINE_KEYWORD!r}")
print(f"  LEADS_CREATED_AFTER (ts) : {cfg.LEADS_CREATED_AFTER}")
if cfg.LEADS_CREATED_AFTER:
    dt = datetime.datetime.utcfromtimestamp(cfg.LEADS_CREATED_AFTER)
    print(f"  LEADS_CREATED_AFTER (UTC): {dt.strftime('%d.%m.%Y %H:%M:%S')}")
print()

# ── Lead ──────────────────────────────────────────────────────────────────
lead = amo.get(f"/api/v4/leads/{LEAD_ID}?with=contacts,companies")
pid  = lead.get("pipeline_id")
sid  = lead.get("status_id")
created_at  = int(lead.get("created_at")  or 0)
updated_at  = int(lead.get("updated_at")  or 0)

print("=== LEAD ===")
print(f"  id          : {lead.get('id')}")
print(f"  name        : {lead.get('name')}")
print(f"  status_id   : {sid}")
print(f"  pipeline_id : {pid}")
print(f"  created_at  : {created_at}  ({datetime.datetime.utcfromtimestamp(created_at).strftime('%d.%m.%Y %H:%M:%S')} UTC)")
print(f"  updated_at  : {updated_at}  ({datetime.datetime.utcfromtimestamp(updated_at).strftime('%d.%m.%Y %H:%M:%S')} UTC)")
print()

# ── Pipeline / status resolution ─────────────────────────────────────────
data      = amo.get("/api/v4/leads/pipelines?with=statuses&limit=250")
pipelines = data.get("_embedded", {}).get("pipelines", [])

trigger_names = [cfg.TRIGGER_STATUS_NAME]
for tn in cfg.TRIGGER_STATUS_NAMES_EXTRA.split(","):
    tn = tn.strip()
    if tn and tn not in trigger_names:
        trigger_names.append(tn)

trigger_displays = {STATUS_DISPLAY_MAP.get(n, n) for n in trigger_names}

found_pipeline = None
for p in pipelines:
    if p["id"] == pid:
        found_pipeline = p
        break

if not found_pipeline:
    print(f"[ERROR] Pipeline {pid} not found in AMO response!")
else:
    pname = found_pipeline["name"]
    kw    = cfg.PIPELINE_KEYWORD.lower()
    kw_ok = (not kw) or (kw in pname.lower())

    print("=== PIPELINE ===")
    print(f"  name          : {pname!r}")
    print(f"  keyword check : PIPELINE_KEYWORD={cfg.PIPELINE_KEYWORD!r}  match={kw_ok}")
    if not kw_ok:
        print(f"  [PROBLEM] Lead would be SKIPPED because pipeline name '{pname}' does not contain '{cfg.PIPELINE_KEYWORD}'")
    print()

    resolved_trigger_ids = set()
    print("=== ALL STATUSES IN THIS PIPELINE ===")
    for s in found_pipeline.get("_embedded", {}).get("statuses", []):
        sname   = s["name"]
        s_id    = s["id"]
        display = STATUS_DISPLAY_MAP.get(sname, sname)
        is_trigger = sname in trigger_names or display in trigger_displays
        if is_trigger:
            resolved_trigger_ids.add(s_id)
        current = "  <-- CURRENT LEAD STATUS" if s_id == sid else ""
        trigger = "  <-- TRIGGER" if is_trigger else ""
        print(f"  id={s_id:>12}  {sname!r:<45} -> {display!r}{trigger}{current}")

    print()
    print("=== DIAGNOSIS ===")
    print(f"  resolved trigger IDs for this pipeline : {sorted(resolved_trigger_ids)}")
    print(f"  lead current status_id                 : {sid}")
    print(f"  is_trigger                             : {sid in resolved_trigger_ids}")
    print()

    if not kw_ok:
        print("[VERDICT] PIPELINE_KEYWORD mismatch — lead is silently skipped by the webhook handler.")
    elif sid not in resolved_trigger_ids:
        print("[VERDICT] current status_id is NOT in trigger IDs — lead would be skipped unless it was on the trigger status at the moment the webhook fired.")
        print("          Check: was the lead on ЗАКАЗ БЕЗ НУМЕРАЦИИ when the webhook arrived, or had it already moved?")
    else:
        print("[VERDICT] Lead looks like it SHOULD be processed. Check LEADS_CREATED_AFTER vs lead's updated_at:")
        if cfg.LEADS_CREATED_AFTER and updated_at and updated_at < cfg.LEADS_CREATED_AFTER:
            cutoff_dt = datetime.datetime.utcfromtimestamp(cfg.LEADS_CREATED_AFTER)
            updated_dt = datetime.datetime.utcfromtimestamp(updated_at)
            print(f"  [PROBLEM] updated_at ({updated_dt}) < LEADS_CREATED_AFTER ({cutoff_dt}) — lead is SKIPPED as too old.")
        else:
            print("  updated_at is within the allowed range — no cutoff issue.")

    if not resolved_trigger_ids:
        print("[VERDICT] No trigger status IDs resolved for this pipeline at all.")
        print(f"          Trigger names being looked for: {trigger_names}")
        print("          None of the statuses in this pipeline matched.")
