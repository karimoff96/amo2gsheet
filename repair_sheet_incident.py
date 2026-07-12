#!/usr/bin/env python3
"""Repair the July 2026 Sheet1 row/header corruption incident.

The command is intentionally dry-run by default.  ``--apply`` is accepted only
with ``--confirm-service-stopped`` and only when the lead log has stopped moving.

The repair never mutates the existing Sheet1 in place.  It duplicates Sheet1,
rebuilds and verifies the duplicate, records duplicate-row decisions in a
separate conflict tab, then swaps worksheet titles.  The original and the
pre-existing timestamped backup remain available for rollback.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import tempfile
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

import gspread
from gspread.utils import ValidationConditionType

from env_loader import load_env


ROOT = Path(__file__).resolve().parent
LOG_DIR = ROOT / "logs"
STATE_PATH = ROOT / ".sync_state.json"
EXPECTED_BACKUP_PREFIX = "Sheet1_BACKUP_"
MANAGED_TAB_RE = re.compile(r"^\d{2}\.\d{4}$")


class RepairError(RuntimeError):
    pass


def _pad(row: Sequence[Any], width: int) -> List[Any]:
    return list(row[:width]) + [""] * max(0, width - len(row))


def _chunks(rows: Sequence[List[Any]], size: int = 300) -> Iterable[Tuple[int, List[List[Any]]]]:
    for start in range(0, len(rows), size):
        yield start, list(rows[start:start + size])


def _column_letter(index_zero_based: int) -> str:
    value = index_zero_based + 1
    out = ""
    while value:
        value, remainder = divmod(value - 1, 26)
        out = chr(ord("A") + remainder) + out
    return out


def _service_stopped_preflight(confirm: bool) -> None:
    if not confirm:
        raise RepairError("--apply requires --confirm-service-stopped")
    lead_log = LOG_DIR / "leads.log"
    if not lead_log.exists():
        raise RepairError(f"Cannot confirm stopped service: {lead_log} is missing")
    age = time.time() - lead_log.stat().st_mtime
    if age < 30:
        raise RepairError(
            f"leads.log changed {age:.1f}s ago; service may still be running"
        )


def _sheet_metadata(spreadsheet) -> Dict[str, Any]:
    return spreadsheet.client.fetch_sheet_metadata(
        spreadsheet.id, params={"includeGridData": "false"}
    )


def _metadata_by_id(spreadsheet) -> Dict[int, Dict[str, Any]]:
    return {
        int(sheet["properties"]["sheetId"]): sheet
        for sheet in _sheet_metadata(spreadsheet).get("sheets", [])
    }


def _protection_request(sheet_id: int, service_account_email: str) -> Dict[str, Any]:
    # Protect all structural/content cells.  Operators may edit only order number
    # (C) and status (U), from row 2 downward.  Owners retain Google's implicit
    # override; the bot still validates every invariant before a read/write.
    return {
        "addProtectedRange": {
            "protectedRange": {
                "range": {"sheetId": sheet_id},
                "unprotectedRanges": [
                    {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "startColumnIndex": 2,
                        "endColumnIndex": 3,
                    },
                    {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "startColumnIndex": 20,
                        "endColumnIndex": 21,
                    },
                ],
                "description": (
                    "amo2gsheet structural lock; operators may edit only "
                    "order number and status"
                ),
                "warningOnly": False,
                "editors": {"users": [service_account_email]},
            }
        }
    }


def _has_required_protection(sheet_meta: Dict[str, Any], email: str) -> bool:
    trusted_users = {
        email.strip().casefold(),
        *{
            item.strip().casefold()
            for item in os.environ.get("GOOGLE_SHEET_OWNER_EMAILS", "").split(",")
            if item.strip()
        },
    }
    for protected in sheet_meta.get("protectedRanges", []):
        grid = protected.get("range") or {}
        if any(k in grid for k in ("endRowIndex", "endColumnIndex")):
            continue
        if int(grid.get("startRowIndex", 0) or 0) != 0:
            continue
        if int(grid.get("startColumnIndex", 0) or 0) != 0:
            continue
        if protected.get("warningOnly", False):
            continue
        allowed = set()
        valid = True
        for item in protected.get("unprotectedRanges", []):
            if int(item.get("startRowIndex", -1)) != 1 or "endRowIndex" in item:
                valid = False
                break
            allowed.add((
                int(item.get("startColumnIndex", -1)),
                int(item.get("endColumnIndex", -1)),
            ))
        editors = protected.get("editors") or {}
        users = {
            str(user).strip().casefold()
            for user in editors.get("users", [])
            if str(user).strip()
        }
        if (
            valid
            and allowed == {(2, 3), (20, 21)}
            and users == trusted_users
            and not editors.get("groups")
            and not editors.get("domainUsersCanEdit", False)
        ):
            return True
    return False


def _find_header(values: List[List[Any]], columns: List[str]) -> int:
    candidates = []
    for index, row in enumerate(values):
        padded = _pad(row, len(columns))
        if padded[1:] == columns[1:] and padded[0] in ("", columns[0]):
            candidates.append(index)
    if len(candidates) != 1:
        raise RepairError(f"Expected exactly one embedded header, found {candidates}")
    return candidates[0]


def _reconstruct(values: List[List[Any]], columns: List[str]) -> Tuple[List[List[Any]], List[Dict[str, Any]]]:
    width = len(columns)
    header_index = _find_header(values, columns)
    source_rows: List[Tuple[int, List[Any]]] = []
    for physical_index, raw in enumerate(values):
        if physical_index == header_index:
            continue
        row = _pad(raw, width)
        lead_id = str(row[1]).strip()
        if lead_id.isdigit():
            source_rows.append((physical_index, row))

    occurrences: Dict[str, List[Tuple[int, List[Any]]]] = {}
    for physical_index, row in source_rows:
        occurrences.setdefault(str(row[1]).strip(), []).append((physical_index, row))

    # This matches the old service's documented duplicate policy and the live-AMO
    # reconciliation: retain the last physical occurrence, preserve every source
    # copy in the backup/conflict tab.
    canonical_physical = {
        lead_id: entries[-1][0] for lead_id, entries in occurrences.items()
    }
    conflicts: List[Dict[str, Any]] = []
    for lead_id, entries in sorted(occurrences.items(), key=lambda item: int(item[0])):
        if len(entries) < 2:
            continue
        chosen = canonical_physical[lead_id]
        for physical_index, row in entries:
            conflicts.append({
                "lead_id": lead_id,
                "source_row": physical_index + 1,
                "decision": "KEEP" if physical_index == chosen else "REMOVE_FROM_ACTIVE",
                "reason": "last physical occurrence retained; all copies remain in backup",
                "row": row,
            })

    # Records below the displaced header were the original table.  Each row-1
    # insert then stacked newer records above it in reverse insertion order.
    ordered_source = (
        [(i, _pad(values[i], width)) for i in range(header_index + 1, len(values))]
        + [(i, _pad(values[i], width)) for i in range(header_index - 1, -1, -1)]
    )
    repaired = [columns]
    seen = set()
    for physical_index, row in ordered_source:
        lead_id = str(row[1]).strip()
        if not lead_id.isdigit():
            continue
        if canonical_physical.get(lead_id) != physical_index:
            continue
        if lead_id in seen:
            raise RepairError(f"Internal deduplication failure for lead {lead_id}")
        seen.add(lead_id)
        repaired.append(row)
    return repaired, conflicts


def _log_written_ids() -> Tuple[set[str], Dict[str, str]]:
    month_start = datetime.now().strftime("%Y-%m-01 00:00:00")
    written: set[str] = set()
    first_timestamp: Dict[str, str] = {}
    pattern = re.compile(r"SHEET (?:INSERT|UPDATE) row=\d+ lead=(\d+) tab='Sheet1'")
    for path in sorted(LOG_DIR.glob("leads.log*")):
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if len(line) < 19 or line[:19] < month_start:
                continue
            match = pattern.search(line)
            if not match:
                continue
            lead_id = match.group(1)
            written.add(lead_id)
            timestamp = line[:19]
            first_timestamp[lead_id] = min(first_timestamp.get(lead_id, timestamp), timestamp)
    return written, first_timestamp


def _ids_across_production_tabs(spreadsheet) -> set[str]:
    ids: set[str] = set()
    ignored_prefixes = (
        "Sheet1_BACKUP_", "Sheet1_REPAIR_", "Sheet1_CORRUPT_", "Repair_conflicts_"
    )
    for ws in spreadsheet.worksheets():
        if ws.title == "Staff" or ws.title.startswith(ignored_prefixes):
            continue
        for row in ws.get(f"B1:B{ws.row_count}", value_render_option="FORMATTED_VALUE"):
            value = str(row[0]).strip() if row else ""
            if value.isdigit():
                ids.add(value)
    return ids


def _build_missing_rows(missing_ids: List[str]):
    # Import only after environment loading.  sync_service constructs its normal
    # clients at module import, but does not start FastAPI's polling worker until
    # application startup; every call below is read-only.
    import sync_service as app

    service = app.service
    full_leads = service.amo.batch_get_leads([int(item) for item in missing_ids])
    service._batch_enrich_contacts(list(full_leads.values()))
    staff_mapping = service.sheet.get_staff_mapping()
    rows: Dict[str, List[Any]] = {}
    baseline: Dict[str, Dict[str, Any]] = {}
    for lead_id in missing_ids:
        lead = full_leads.get(int(lead_id))
        if not lead:
            raise RepairError(f"amoCRM did not return missing lead {lead_id}")
        pipeline_id = int(lead.get("pipeline_id", 0) or 0)
        status_id = int(lead.get("status_id", 0) or 0)
        pipeline_name = service.pipeline_id_to_name.get(pipeline_id, "")
        responsible_id = int(lead.get("responsible_user_id", 0) or 0)
        responsible_name = service.users_map.get(responsible_id, str(responsible_id))
        display_status = service.status_id_to_display_name.get(status_id, str(status_id))
        sheet_status = app.AMO_STATUS_TO_SHEET_OVERRIDE.get(display_status, display_status)
        row = app.build_row(
            lead, sheet_status, pipeline_name, responsible_name, staff_mapping
        )
        row[app.ID_COL_INDEX] = lead_id
        rows[lead_id] = row
        baseline[lead_id] = {
            "pipeline_id": pipeline_id,
            "status": str(row[app.STATUS_COL_INDEX]).strip(),
            "order": str(row[app.ORDER_NUM_COL_INDEX]).strip(),
        }
    return app, rows, baseline


def _write_rows(ws, rows: List[List[Any]], width: int) -> None:
    last_col = _column_letter(width - 1)
    for offset, chunk in _chunks(rows):
        start_row = offset + 1
        end_row = start_row + len(chunk) - 1
        ws.update(
            values=chunk,
            range_name=f"A{start_row}:{last_col}{end_row}",
            value_input_option="RAW",
        )


def _seed_state(repaired_rows: List[List[Any]], baseline: Dict[str, Dict[str, Any]], run_id: str) -> Path:
    if not STATE_PATH.exists():
        raise RepairError(f"Runtime state is missing: {STATE_PATH}")
    state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    state_backup = STATE_PATH.with_name(f".sync_state.backup_{run_id}.json")
    shutil.copy2(STATE_PATH, state_backup)
    status_map = state.setdefault("sheet_status_by_lead", {})
    order_map = state.setdefault("sheet_order_number_by_lead", {})
    tab_map = state.setdefault("lead_tab_by_lead", {})
    pipeline_map = state.setdefault("lead_pipeline_by_lead", {})
    # Older corrupted scans could accidentally persist the header label as if it
    # were a lead ID.  Remove it from every lead-keyed state map before seeding
    # the repaired rows.
    for value in state.values():
        if isinstance(value, dict):
            value.pop("ID", None)
    for row in repaired_rows[1:]:
        lead_id = str(row[1]).strip()
        status_map[lead_id] = str(row[20]).strip()
        order_map[lead_id] = str(row[2]).strip()
        tab_map[lead_id] = "Sheet1"
    for lead_id, item in baseline.items():
        if item["pipeline_id"]:
            pipeline_map[lead_id] = int(item["pipeline_id"])

    fd, tmp_name = tempfile.mkstemp(prefix=".sync_state.repair.", dir=str(ROOT))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, STATE_PATH)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
    return state_backup


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="perform the staged repair")
    parser.add_argument(
        "--confirm-service-stopped", action="store_true",
        help="confirm amo2gsheet has been stopped before applying",
    )
    args = parser.parse_args()

    load_env()
    sheet_id = os.environ["GOOGLE_SHEET_ID"]
    credentials_path = Path(os.environ["GOOGLE_SERVICE_ACCOUNT_FILE"])
    credentials_data = json.loads(credentials_path.read_text(encoding="utf-8"))
    service_email = str(credentials_data.get("client_email", "")).strip()
    if not service_email:
        raise RepairError("Service-account client_email is missing")

    gc = gspread.service_account(filename=str(credentials_path))
    spreadsheet = gc.open_by_key(sheet_id)
    source = spreadsheet.worksheet("Sheet1")
    backups = [ws for ws in spreadsheet.worksheets() if ws.title.startswith(EXPECTED_BACKUP_PREFIX)]
    if not backups:
        raise RepairError("Timestamped Sheet1 backup is missing; refusing repair")

    source_values = source.get(f"A1:U{source.row_count}", value_render_option="FORMATTED_VALUE")
    repaired, conflicts = _reconstruct(source_values, [
        "Компания", "ID", "Заказ №", "Ф.И.О.", "Контактный номер",
        "Дата заказа", "Дата доставка", "Код сотрудника", "Ответственный",
        "Группа", "Продукт 1", "Количество 1", "Продукт 2",
        "Количество 2", "Бюджет сделки", "Регион", "Адрес",
        "Тип продажи", "Продажа в рассрочку", "Воронка", "Статус",
    ])
    written_ids, first_written = _log_written_ids()
    workbook_ids = _ids_across_production_tabs(spreadsheet)
    missing_ids = sorted(
        written_ids - workbook_ids,
        key=lambda item: (first_written.get(item, "9999"), int(item)),
    )
    app, missing_rows, baseline = _build_missing_rows(missing_ids)
    for lead_id in missing_ids:
        repaired.append(_pad(missing_rows[lead_id], len(app.COLUMNS)))

    ids = [str(row[app.ID_COL_INDEX]).strip() for row in repaired[1:]]
    duplicate_counts = {key: value for key, value in Counter(ids).items() if value > 1}
    if duplicate_counts:
        raise RepairError(f"Repaired result still has duplicates: {duplicate_counts}")
    if len(ids) != len(set(ids)):
        raise RepairError("Repaired IDs are not unique")
    if set(missing_ids) - set(ids):
        raise RepairError("Not every missing lead was restored")

    print(f"Spreadsheet: {spreadsheet.title} ({spreadsheet.id})")
    print(f"Source: Sheet1 sheetId={source.id}; backup={sorted(ws.title for ws in backups)[-1]}")
    print(f"Canonical existing records: {len(repaired) - 1 - len(missing_ids)}")
    print(f"Duplicate source copies removed from active: {len(conflicts) // 2 if conflicts else 0}")
    print(f"Missing July records restored from amoCRM: {len(missing_ids)}")
    print("Missing IDs: " + ",".join(missing_ids))
    print(f"Target: {len(repaired) - 1} unique records + one header")

    if not args.apply:
        print("DRY RUN ONLY — no Google Sheet or state changes were made")
        return 0

    _service_stopped_preflight(args.confirm_service_stopped)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    staging_name = f"Sheet1_REPAIR_{run_id}"
    corrupt_name = f"Sheet1_CORRUPT_{run_id}"
    conflicts_name = f"Repair_conflicts_{run_id}"

    staging = spreadsheet.duplicate_sheet(source.id, new_sheet_name=staging_name)
    conflict_ws = spreadsheet.add_worksheet(
        title=conflicts_name, rows=max(100, len(conflicts) + 1), cols=26
    )
    conflict_header = ["Lead ID", "Source row", "Decision", "Reason"] + app.COLUMNS
    conflict_values = [conflict_header] + [
        [item["lead_id"], item["source_row"], item["decision"], item["reason"]]
        + item["row"]
        for item in conflicts
    ]
    _write_rows(conflict_ws, conflict_values, len(conflict_header))

    # Values only: duplicateSheet already preserved column widths, formatting and
    # conditional formats.  Rebuild all table values in canonical order.
    staging.clear()
    _write_rows(staging, repaired, len(app.COLUMNS))

    metadata = _metadata_by_id(spreadsheet)
    existing_protections = metadata[staging.id].get("protectedRanges", [])
    requests: List[Dict[str, Any]] = []
    for protected in existing_protections:
        if "protectedRangeId" in protected:
            requests.append({
                "deleteProtectedRange": {
                    "protectedRangeId": protected["protectedRangeId"]
                }
            })
    # Copy the original embedded header's format to the repaired row 1.
    source_header_index = _find_header(source_values, app.COLUMNS)
    requests.extend([
        {
            "copyPaste": {
                "source": {
                    "sheetId": staging.id,
                    "startRowIndex": source_header_index,
                    "endRowIndex": source_header_index + 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": len(app.COLUMNS),
                },
                "destination": {
                    "sheetId": staging.id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": len(app.COLUMNS),
                },
                "pasteType": "PASTE_FORMAT",
            }
        },
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": staging.id,
                    "gridProperties": {"frozenRowCount": 1},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        },
        _protection_request(staging.id, service_email),
    ])
    spreadsheet.client.batch_update(spreadsheet.id, {"requests": requests})
    staging.add_validation(
        f"U2:U{staging.row_count}",
        ValidationConditionType.one_of_list,
        ["В процессе", "У курера", "Успешно", "Отказ"],
        strict=True,
        showCustomUi=True,
    )

    # Verify staging values and structural metadata before the title swap.
    staged_values = staging.get(
        f"A1:U{len(repaired)}", value_render_option="UNFORMATTED_VALUE"
    )
    staged_rows = [_pad(row, len(app.COLUMNS)) for row in staged_values]
    normalize = lambda rows: [
        ["" if value is None else str(value) for value in row]
        for row in rows
    ]
    if normalize(staged_rows) != normalize([
        _pad(row, len(app.COLUMNS)) for row in repaired
    ]):
        raise RepairError("Staging readback does not exactly match repaired values")
    staged_ids = [str(row[app.ID_COL_INDEX]).strip() for row in staged_rows[1:]]
    if len(staged_ids) != len(set(staged_ids)) or set(staged_ids) != set(ids):
        raise RepairError("Staging ID verification failed")
    stage_meta = _metadata_by_id(spreadsheet)[staging.id]
    frozen = int((stage_meta["properties"].get("gridProperties") or {}).get("frozenRowCount", 0) or 0)
    if frozen != 1 or not _has_required_protection(stage_meta, service_email):
        raise RepairError("Staging freeze/protection verification failed")

    # Atomic title transition: old Sheet1 remains intact under a corruption label.
    spreadsheet.client.batch_update(
        spreadsheet.id,
        {
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {"sheetId": source.id, "title": corrupt_name},
                        "fields": "title",
                    }
                },
                {
                    "updateSheetProperties": {
                        "properties": {"sheetId": staging.id, "title": "Sheet1"},
                        "fields": "title",
                    }
                },
            ]
        },
    )

    # Protect existing generated month archives too; future rotations create this
    # protection in sync_service.py.
    spreadsheet = gc.open_by_key(sheet_id)
    metadata = _metadata_by_id(spreadsheet)
    archive_requests = []
    for ws in spreadsheet.worksheets():
        if not MANAGED_TAB_RE.match(ws.title):
            continue
        sheet_meta = metadata[ws.id]
        if not _has_required_protection(sheet_meta, service_email):
            archive_requests.extend([
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": ws.id,
                            "gridProperties": {"frozenRowCount": 1},
                        },
                        "fields": "gridProperties.frozenRowCount",
                    }
                },
                _protection_request(ws.id, service_email),
            ])
    if archive_requests:
        spreadsheet.client.batch_update(
            spreadsheet.id, {"requests": archive_requests}
        )

    state_backup = _seed_state(repaired, baseline, run_id)

    # Final readback from the newly named Sheet1.
    spreadsheet = gc.open_by_key(sheet_id)
    live = spreadsheet.worksheet("Sheet1")
    final_header = _pad(live.row_values(1), len(app.COLUMNS))
    final_ids = {
        str(row[0]).strip()
        for row in live.get(f"B2:B{len(repaired)}", value_render_option="FORMATTED_VALUE")
        if row and str(row[0]).strip()
    }
    final_meta = _metadata_by_id(spreadsheet)[live.id]
    if final_header != app.COLUMNS or final_ids != set(ids):
        raise RepairError("Final Sheet1 content verification failed after title swap")
    if not _has_required_protection(final_meta, service_email):
        raise RepairError("Final Sheet1 structural protection verification failed")

    print(f"APPLIED: repaired Sheet1 sheetId={live.id}")
    print(f"Original preserved as: {corrupt_name}")
    print(f"Conflict audit tab: {conflicts_name}")
    print(f"State backup: {state_backup.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
