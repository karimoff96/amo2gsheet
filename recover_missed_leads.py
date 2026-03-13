"""
recover_missed_leads.py
───────────────────────
Recovers leads that reached trigger status during the Mar 7-14 downtime
but were never written to the sheet.

Writes ONLY leads that:
  • are currently at a ЗАКАЗ БЕЗ НУМЕРАЦИИ trigger status
  • belong to a Sotuv pipeline
  • were created on or after RECOVER_FROM date
  • are NOT already present in the sheet (upsert is safe — won't duplicate)

Run on the server:
    cd /home/amo2gsheet && venv/bin/python recover_missed_leads.py
"""

import sys, os
sys.path.insert(0, "/home/amo2gsheet")
from env_loader import load_env
load_env()

import json, time
from pathlib import Path
from datetime import datetime, timezone

# ── Bootstrap the full service (same as the real app) ─────────────────────────
from sync_service import SyncService, build_row, _log

service = SyncService()

# ── Config ────────────────────────────────────────────────────────────────────
# Recover leads created on or after this date (start of downtime)
RECOVER_FROM = "2026-03-07"

TRIGGER_IDS = {
    77555170,  # Nilufar - Sotuv
    78235518,  # Baza Uspeshno - Sotuv
    80872798,  # Rushana - Sotuv
    82272914,  # Munira - Sotuv
    83322542,  # Akobir - Sotuv
    83339774,  # Baza Dumka - Sotuv
}

SOTUV_PIPELINE_IDS = {9617690, 10410262, 10215182, 9753178, 10564782, 10520726}

ts_from = int(datetime.strptime(RECOVER_FROM, "%Y-%m-%d")
              .replace(tzinfo=timezone.utc).timestamp())
ts_to   = int(time.time()) + 3600

print(f"\n{'─'*60}")
print(f"  Recovery: leads created since {RECOVER_FROM} at trigger status")
print(f"{'─'*60}")

# ── Fetch candidates ──────────────────────────────────────────────────────────
candidates = []
amo = service.amo

for status_id in TRIGGER_IDS:
    pipeline_filter = "&".join(
        f"filter[pipeline_id][]={p}" for p in SOTUV_PIPELINE_IDS
    )
    page = 1
    while True:
        try:
            data = amo.get(
                f"/api/v4/leads"
                f"?filter[status_id][]={status_id}"
                f"&{pipeline_filter}"
                f"&with=contacts,companies"
                f"&limit=250&page={page}"
            )
        except RuntimeError as exc:
            if "204" in str(exc):
                break
            print(f"  [WARN] fetch error for status {status_id}: {exc}")
            break

        leads = (data.get("_embedded") or {}).get("leads") or []
        if not leads:
            break

        for lead in leads:
            created = lead.get("created_at", 0)
            if created >= ts_from:
                candidates.append(lead)

        if not (data.get("_links") or {}).get("next"):
            break
        page += 1

print(f"\n  Found {len(candidates)} candidate lead(s) at trigger status since {RECOVER_FROM}")

if not candidates:
    print("  Nothing to recover. Exiting.")
    sys.exit(0)

# ── Write to sheet ────────────────────────────────────────────────────────────
staff_mapping = service.sheet.get_staff_mapping()
written = 0
skipped = 0
errors  = 0

for lead in candidates:
    lead_id     = str(lead.get("id", "")).strip()
    pipeline_id = int(lead.get("pipeline_id", 0) or 0)
    status_id   = int(lead.get("status_id",   0) or 0)
    pipeline_name = service.pipeline_id_to_name.get(pipeline_id, "")

    # Enrich contacts
    try:
        lead = service._enrich_lead_contacts(lead)
    except Exception:
        pass

    status_display = service.status_id_to_display_name.get(status_id, str(status_id))
    responsible_id = int(lead.get("responsible_user_id", 0) or 0)
    responsible_name = service.users_map.get(responsible_id, str(responsible_id))

    tab_name = service._tab_for_lead(lead)
    row = build_row(lead, status_display, pipeline_name, responsible_name, staff_mapping)

    try:
        service.sheet.upsert_row(row, tab_name)
        service.remember_sheet_status(lead_id, status_display)
        service.remember_lead_tab(lead_id, tab_name)
        service.remember_lead_pipeline(lead_id, pipeline_id)
        service.remember_sheet_order_number(lead_id, "")

        created_dt = datetime.fromtimestamp(lead["created_at"]).strftime("%Y-%m-%d")
        print(f"  [OK] lead={lead_id} created={created_dt} "
              f"pipeline='{pipeline_name}' status='{status_display}' tab='{tab_name}'")
        written += 1
        time.sleep(0.3)  # avoid Sheets quota
    except Exception as exc:
        print(f"  [ERR] lead={lead_id}: {exc}")
        errors += 1

service.flush_state()

print(f"\n{'─'*60}")
print(f"  Recovery complete: {written} written, {skipped} skipped, {errors} errors")
print(f"{'─'*60}\n")
