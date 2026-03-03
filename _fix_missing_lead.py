"""
Manually upsert one or more leads into the Google Sheet,
exactly as the webhook handler would.  Run this when a lead
was missed (service down, webhook not fired, etc.).

Usage:
    python _fix_missing_lead.py 37850979
    python _fix_missing_lead.py 37850979 37851234 37851999
"""
import sys
from env_loader import load_env
load_env()

from sync_service import (
    Config, TokenStore, AmoClient, SheetSync,
    STATUS_DISPLAY_MAP, PIPELINE_DISPLAY_MAP,
    build_row, ID_COL_INDEX, ORDER_NUM_COL_INDEX,
    SyncService,
)

LEAD_IDS = [int(x) for x in (sys.argv[1:] or ["37850979"])]

svc = SyncService()

staff_mapping = svc.sheet.get_staff_mapping()

for lead_id in LEAD_IDS:
    print(f"\n── Processing lead {lead_id} ──")
    try:
        full_lead = svc.amo.get(f"/api/v4/leads/{lead_id}?with=contacts,companies")
        full_lead = svc._enrich_lead_contacts(full_lead)
    except Exception as exc:
        print(f"  [ERROR] Could not fetch lead: {exc}")
        continue

    sid          = int(full_lead.get("status_id", 0) or 0)
    pipeline_id  = int(full_lead.get("pipeline_id", 0) or 0)
    pipeline_name = svc.pipeline_id_to_name.get(pipeline_id, "")
    pipeline_display = PIPELINE_DISPLAY_MAP.get(pipeline_name, pipeline_name)
    status_display   = svc.status_id_to_display_name.get(sid, STATUS_DISPLAY_MAP.get(str(sid), str(sid)))
    responsible_id   = int(full_lead.get("responsible_user_id", 0) or 0)
    responsible_name = svc.users_map.get(responsible_id, str(responsible_id))

    print(f"  pipeline  : {pipeline_name!r}  ({pipeline_id})")
    print(f"  status    : {status_display!r}  (id={sid})")
    print(f"  responsible: {responsible_name!r}")

    row      = build_row(full_lead, status_display, pipeline_name, responsible_name, staff_mapping)
    tab_name = svc._tab_for_lead(full_lead)
    row_num  = svc.sheet.upsert_row(row, tab_name)

    lead_id_str = str(full_lead.get("id", lead_id))
    actual_order_num = str(row[ORDER_NUM_COL_INDEX]) if len(row) > ORDER_NUM_COL_INDEX else ""
    svc.remember_sheet_status(lead_id_str, status_display)
    svc.remember_lead_tab(lead_id_str, tab_name)
    svc.remember_sheet_order_number(lead_id_str, actual_order_num)

    print(f"  [OK] Written to sheet '{tab_name}' row {row_num}")

svc.flush_state()
print("\nDone.")
