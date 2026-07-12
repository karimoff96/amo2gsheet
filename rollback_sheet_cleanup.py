#!/usr/bin/env python3
"""Remove only the July 2026 repair artifacts from the live spreadsheet.

This command is deliberately structural.  It never writes cell values, copies
rows, changes sheet titles, or seeds sync state.  ``--apply`` is accepted only
after the sync service has been stopped and the expected live sheet IDs and
repair metadata have been re-read and verified.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

import gspread

from env_loader import load_env


ROOT = Path(__file__).resolve().parent
SERVICE_NAME = "amo2gsheet"
SHEET1 = "Sheet1"
BACKUP = "Sheet1_BACKUP_20260712_1334"
CORRUPT = "Sheet1_CORRUPT_20260712_142431"
CONFLICTS = "Repair_conflicts_20260712_142431"
REPAIR_PROTECTION_ID = 1724633048
REPAIR_PROTECTION_DESCRIPTION = (
    "amo2gsheet structural lock; operators may edit only order number and status"
)
STATUS_COLUMN_INDEX = 20  # U, zero-based
STATUS_OPTIONS = ("В процессе", "У курера", "Успешно", "Отказ")

# These IDs were captured from the live metadata immediately before the
# cleanup.  Refusing a title-to-ID mismatch prevents a similarly named tab from
# being deleted after an unrelated spreadsheet change.
EXPECTED_SHEET_IDS = {
    SHEET1: 542089377,
    BACKUP: 420242054,
    CORRUPT: 1035453320,
    CONFLICTS: 2129358340,
}

EXPECTED_HEADER = [
    "Компания",
    "ID",
    "Заказ №",
    "Ф.И.О.",
    "Контактный номер",
    "Дата заказа",
    "Дата доставка",
    "Код сотрудника",
    "Ответственный",
    "Группа",
    "Продукт 1",
    "Количество 1",
    "Продукт 2",
    "Количество 2",
    "Бюджет сделки",
    "Регион",
    "Адрес",
    "Тип продажи",
    "Продажа в рассрочку",
    "Воронка",
    "Статус",
]


class CleanupError(RuntimeError):
    """Raised when the live spreadsheet is not the expected cleanup target."""


def _metadata(spreadsheet) -> Dict[str, Any]:
    return spreadsheet.client.fetch_sheet_metadata(
        spreadsheet.id,
        params={"includeGridData": "false"},
    )


def _records(metadata: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {
        str(sheet.get("properties", {}).get("title", "")): sheet
        for sheet in metadata.get("sheets", [])
    }


def _values_digest(values: List[List[Any]]) -> str:
    payload = json.dumps(
        values,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _value_snapshot(ws) -> Dict[str, Any]:
    values = [list(row) for row in ws.get_all_values()]
    ids = [str(row[1]).strip() if len(row) > 1 else "" for row in values[1:]]
    return {
        "sha256": _values_digest(values),
        "rows": len(values),
        "ids": ids,
    }


def _grid_rows(record: Dict[str, Any]) -> int:
    return int(
        (record.get("properties", {}).get("gridProperties") or {}).get(
            "rowCount", 0
        )
        or 0
    )


def _validation_cells(spreadsheet, sheet_title: str, row_count: int) -> List[Dict[str, Any]]:
    if row_count < 1:
        return []
    metadata = spreadsheet.client.fetch_sheet_metadata(
        spreadsheet.id,
        params={
            "includeGridData": "true",
            "ranges": [f"'{sheet_title.replace(chr(39), chr(39) * 2)}'!U1:U{row_count}"],
        },
    )
    cells: List[Dict[str, Any]] = []
    for sheet in metadata.get("sheets", []):
        for block in sheet.get("data", []):
            start_row = int(block.get("startRow", 0) or 0)
            start_col = int(block.get("startColumn", STATUS_COLUMN_INDEX) or 0)
            for row_offset, row in enumerate(block.get("rowData", [])):
                for col_offset, cell in enumerate(row.get("values", [])):
                    rule = cell.get("dataValidation")
                    if rule:
                        cells.append({
                            "row": start_row + row_offset + 1,
                            "column": start_col + col_offset + 1,
                            "rule": rule,
                        })
    return cells


def _is_expected_status_rule(rule: Dict[str, Any]) -> bool:
    condition = rule.get("condition") or {}
    values = tuple(
        str(item.get("userEnteredValue", ""))
        for item in condition.get("values", [])
    )
    return condition.get("type") == "ONE_OF_LIST" and values == STATUS_OPTIONS


def _assert_service_stopped(service_name: str) -> None:
    result = subprocess.run(
        ["systemctl", "show", service_name, "--property=ActiveState", "--value"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise CleanupError(
            f"Could not verify {service_name}.service state: "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )
    state = result.stdout.strip()
    if state in {"active", "activating", "reloading"}:
        raise CleanupError(
            f"{service_name}.service is still {state}; stop it before --apply"
        )


def _assert_expected_targets(records: Dict[str, Dict[str, Any]]) -> None:
    missing = [title for title in EXPECTED_SHEET_IDS if title not in records]
    if missing:
        raise CleanupError(f"Expected live tabs are missing: {', '.join(missing)}")
    mismatches = []
    for title, expected_id in EXPECTED_SHEET_IDS.items():
        actual_id = int(records[title]["properties"].get("sheetId", -1))
        if actual_id != expected_id:
            mismatches.append(f"{title}: expected {expected_id}, found {actual_id}")
    if mismatches:
        raise CleanupError("Live sheet ID verification failed: " + "; ".join(mismatches))


def _assert_preserved_sheet_ids(
    before_records: Dict[str, Dict[str, Any]],
    after_records: Dict[str, Dict[str, Any]],
) -> None:
    for title in before_records:
        if title in {CORRUPT, CONFLICTS}:
            continue
        if title not in after_records:
            raise CleanupError(f"Preserved worksheet '{title}' disappeared")
        before_id = int(before_records[title]["properties"].get("sheetId", -1))
        after_id = int(after_records[title]["properties"].get("sheetId", -1))
        if before_id != after_id:
            raise CleanupError(
                f"Preserved worksheet '{title}' changed ID {before_id} → {after_id}"
            )


def _assert_repair_metadata(
    spreadsheet,
    records: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    sheet1_record = records[SHEET1]
    sheet1_id = EXPECTED_SHEET_IDS[SHEET1]
    protections = [
        item
        for item in sheet1_record.get("protectedRanges", [])
        if int(item.get("protectedRangeId", -1)) == REPAIR_PROTECTION_ID
    ]
    if len(protections) != 1:
        raise CleanupError(
            f"Expected exactly one repair protection {REPAIR_PROTECTION_ID} "
            f"on {SHEET1}, found {len(protections)}"
        )
    protection = protections[0]
    if protection.get("description") != REPAIR_PROTECTION_DESCRIPTION:
        raise CleanupError(
            f"Protected range {REPAIR_PROTECTION_ID} has an unexpected description"
        )
    protected_range = protection.get("range") or {}
    if int(protected_range.get("sheetId", -1)) != sheet1_id:
        raise CleanupError("Repair protection does not belong to Sheet1")

    row_count = _grid_rows(sheet1_record)
    validation_cells = _validation_cells(spreadsheet, SHEET1, row_count)
    if not validation_cells:
        raise CleanupError(f"No status validation found on {SHEET1}; refusing cleanup")
    unexpected = [
        cell for cell in validation_cells
        if cell["column"] != STATUS_COLUMN_INDEX + 1
        or not _is_expected_status_rule(cell["rule"])
    ]
    if unexpected:
        raise CleanupError(
            "Sheet1 contains validation outside the expected repair dropdown; "
            "refusing to clear it"
        )
    return validation_cells


def _kept_titles(records: Dict[str, Dict[str, Any]]) -> Iterable[str]:
    return (title for title in records if title not in {CORRUPT, CONFLICTS})


def _structural_snapshot(records: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    result = {}
    for title in _kept_titles(records):
        properties = records[title].get("properties", {})
        grid = properties.get("gridProperties") or {}
        result[title] = {
            "id": int(properties.get("sheetId", -1)),
            "title": str(properties.get("title", "")),
            "rowCount": int(grid.get("rowCount", 0) or 0),
            "columnCount": int(grid.get("columnCount", 0) or 0),
            "frozenRowCount": int(grid.get("frozenRowCount", 0) or 0),
            "protectedRangeIds": sorted(
                int(item.get("protectedRangeId", -1))
                for item in records[title].get("protectedRanges", [])
                if item.get("protectedRangeId") is not None
            ),
        }
    return result


def _snapshot_values(spreadsheet, records: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    result = {}
    worksheets = {ws.title: ws for ws in spreadsheet.worksheets()}
    for title in _kept_titles(records):
        if title not in worksheets:
            raise CleanupError(f"Worksheet '{title}' disappeared during snapshot")
        result[title] = _value_snapshot(worksheets[title])
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="apply the verified structural cleanup",
    )
    parser.add_argument(
        "--confirm-service-stopped",
        action="store_true",
        help="confirm that amo2gsheet.service is stopped before applying",
    )
    parser.add_argument(
        "--service-name",
        default=SERVICE_NAME,
        help=f"systemd service to verify (default: {SERVICE_NAME})",
    )
    args = parser.parse_args()

    os.chdir(ROOT)
    load_env()
    if args.apply and not args.confirm_service_stopped:
        raise CleanupError("--apply requires --confirm-service-stopped")
    if args.apply:
        _assert_service_stopped(args.service_name)

    credentials_path = Path(os.environ["GOOGLE_SERVICE_ACCOUNT_FILE"])
    if not credentials_path.is_absolute():
        credentials_path = ROOT / credentials_path
    spreadsheet = gspread.service_account(filename=str(credentials_path)).open_by_key(
        os.environ["GOOGLE_SHEET_ID"]
    )

    before_metadata = _metadata(spreadsheet)
    before_records = _records(before_metadata)
    _assert_expected_targets(before_records)
    validation_cells = _assert_repair_metadata(spreadsheet, before_records)

    sheet1 = spreadsheet.worksheet(SHEET1)
    before_sheet1_values = _value_snapshot(sheet1)
    raw_header = sheet1.row_values(1)
    if raw_header != EXPECTED_HEADER:
        raise CleanupError("Sheet1 row 1 is not the expected header; refusing cleanup")
    before_structural = _structural_snapshot(before_records)
    before_values = _snapshot_values(spreadsheet, before_records)

    print(f"Spreadsheet: {spreadsheet.title} ({spreadsheet.id})")
    print(
        "Targets: "
        f"{CORRUPT}={EXPECTED_SHEET_IDS[CORRUPT]}, "
        f"{CONFLICTS}={EXPECTED_SHEET_IDS[CONFLICTS]}"
    )
    print(
        f"Sheet1: id={EXPECTED_SHEET_IDS[SHEET1]}, rows={before_sheet1_values['rows']}, "
        f"IDs={len([item for item in before_sheet1_values['ids'] if item])}, "
        f"validation_cells={len(validation_cells)}, "
        f"sha256={before_sheet1_values['sha256'][:16]}"
    )
    print(f"Preserved tabs: {len(before_values)}")

    if not args.apply:
        print("DRY RUN ONLY — no Google Sheet changes were made")
        return 0

    sheet1_record = before_records[SHEET1]
    grid_rows = _grid_rows(sheet1_record)
    requests = [
        {"deleteSheet": {"sheetId": EXPECTED_SHEET_IDS[CORRUPT]}},
        {"deleteSheet": {"sheetId": EXPECTED_SHEET_IDS[CONFLICTS]}},
        {"deleteProtectedRange": {"protectedRangeId": REPAIR_PROTECTION_ID}},
        {
            "setDataValidation": {
                "range": {
                    "sheetId": EXPECTED_SHEET_IDS[SHEET1],
                    "startRowIndex": 0,
                    "endRowIndex": grid_rows,
                    "startColumnIndex": STATUS_COLUMN_INDEX,
                    "endColumnIndex": STATUS_COLUMN_INDEX + 1,
                }
            }
        },
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": EXPECTED_SHEET_IDS[SHEET1],
                    "gridProperties": {"frozenRowCount": 1},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        },
    ]
    spreadsheet.client.batch_update(spreadsheet.id, {"requests": requests})

    after_metadata = _metadata(spreadsheet)
    after_records = _records(after_metadata)
    for title in (CORRUPT, CONFLICTS):
        if title in after_records:
            raise CleanupError(f"Repair tab '{title}' still exists after cleanup")
    _assert_preserved_sheet_ids(before_records, after_records)

    after_structural = _structural_snapshot(after_records)
    for title, snapshot in before_structural.items():
        if title == SHEET1:
            continue
        if after_structural.get(title) != snapshot:
            raise CleanupError(f"Non-target tab metadata changed: {title}")

    after_sheet1 = after_records[SHEET1]
    after_grid = after_sheet1.get("properties", {}).get("gridProperties") or {}
    if int(after_grid.get("frozenRowCount", 0) or 0) != 1:
        raise CleanupError("Sheet1 does not have exactly one frozen header row")
    remaining_protections = {
        int(item.get("protectedRangeId", -1))
        for item in after_sheet1.get("protectedRanges", [])
        if item.get("protectedRangeId") is not None
    }
    before_protections = set(before_structural[SHEET1]["protectedRangeIds"])
    if remaining_protections != before_protections - {REPAIR_PROTECTION_ID}:
        raise CleanupError("Sheet1 protection metadata changed beyond the repair range")
    if _validation_cells(spreadsheet, SHEET1, grid_rows):
        raise CleanupError("Sheet1 status validation still exists after cleanup")

    after_values = _snapshot_values(spreadsheet, after_records)
    if after_values != before_values:
        raise CleanupError("One or more preserved tab values changed during cleanup")
    if after_values[SHEET1] != before_sheet1_values:
        raise CleanupError("Sheet1 values, row count, or IDs changed during cleanup")

    print(
        f"APPLIED: deleted {CORRUPT} and {CONFLICTS}; "
        "Sheet1 values and preserved tabs verified unchanged"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CleanupError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
