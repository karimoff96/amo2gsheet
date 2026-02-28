"""
setup_sheet.py — One-time Google Sheet initialiser.

Run this script ONCE on a new environment to:
  1. Create / verify the main data worksheet with correct headers and freeze.
  2. Apply a status-column dropdown to all data rows.
  3. Create / verify the 'Staff' sheet with headers.
  4. Optionally populate the Staff sheet from a CSV or interactive input.

Usage:
    python setup_sheet.py                    # interactive menu
    python setup_sheet.py --staff staff.csv  # import staff from CSV (code,name)
    python setup_sheet.py --check            # only verify, no changes
"""

import argparse
import csv
import os
import sys
from pathlib import Path

import gspread
from env_loader import load_env
from gspread.utils import ValidationConditionType

load_env()

# ── Config from .env ──────────────────────────────────────────────────────────
SA_FILE    = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "gsheet.json").strip()
SHEET_ID   = os.getenv("GOOGLE_SHEET_ID", "").strip()
WS_NAME    = os.getenv("GOOGLE_WORKSHEET_NAME", "Sheet1").strip()

if not SHEET_ID:
    sys.exit("[ERROR] GOOGLE_SHEET_ID is not set in .env")
if not Path(SA_FILE).exists():
    sys.exit(f"[ERROR] Service account file not found: {SA_FILE}")

# ── Column definitions (must match sync_service.py COLUMNS) ──────────────────
COLUMNS = [
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
    "статус",
]

STATUS_COL_INDEX     = COLUMNS.index("статус")
STATUS_COL_LETTER    = chr(ord("A") + STATUS_COL_INDEX)   # e.g. "T"
STATUS_DROPDOWN_OPTS = ["В процессе", "У курера", "Успешно", "Отказ"]

SEP = "─" * 60


def ok(msg: str)  -> None: print(f"  [✓] {msg}")
def info(msg: str)-> None: print(f"  [i] {msg}")
def warn(msg: str)-> None: print(f"  [!] {msg}")


# ── Sheet helpers ─────────────────────────────────────────────────────────────
def get_or_create(spreadsheet, name: str, rows: int = 2000, cols: int = 30):
    try:
        ws = spreadsheet.worksheet(name)
        info(f"Worksheet '{name}' already exists.")
        return ws
    except gspread.WorksheetNotFound:
        pass
    try:
        ws = spreadsheet.add_worksheet(title=name, rows=rows, cols=cols)
        ok(f"Created worksheet '{name}'.")
    except Exception:
        # Sheet exists in Google but wasn't in the cached metadata — fetch it fresh.
        ws = spreadsheet.worksheet(name)
        info(f"Worksheet '{name}' already exists (refreshed from API).")
    return ws


def setup_main_sheet(spreadsheet, check_only: bool) -> None:
    print(f"\n{SEP}")
    print("  MAIN DATA SHEET")
    print(SEP)

    ws = get_or_create(spreadsheet, WS_NAME)

    # Check / set headers
    current = ws.row_values(1)
    if current == COLUMNS:
        ok("Headers are already correct.")
    elif check_only:
        warn(f"Headers mismatch!\n  Expected : {COLUMNS}\n  Got      : {current}")
    else:
        ws.update(values=[COLUMNS], range_name="A1")
        ok(f"Headers written ({len(COLUMNS)} columns).")

    # Freeze row 1
    if not check_only:
        ws.freeze(rows=1)
        ok("Row 1 frozen.")

    # Status dropdown
    dropdown_range = f"{STATUS_COL_LETTER}2:{STATUS_COL_LETTER}2000"
    if not check_only:
        try:
            ws.add_validation(
                dropdown_range,
                ValidationConditionType.one_of_list,
                STATUS_DROPDOWN_OPTS,
                showCustomUi=True,
            )
            ok(f"Status dropdown applied to column {STATUS_COL_LETTER} "
               f"(rows 2-2000): {STATUS_DROPDOWN_OPTS}")
        except Exception as e:
            warn(f"Could not apply dropdown: {e}")
    else:
        info(f"Status column: {STATUS_COL_LETTER}  "
             f"(would apply dropdown: {STATUS_DROPDOWN_OPTS})")

    # Column widths — make the sheet readable
    if not check_only:
        try:
            # Set all columns to auto-resize
            ws.columns_auto_resize(0, len(COLUMNS))
            ok("Columns auto-resized.")
        except Exception:
            pass  # non-critical

    info(f"Sheet URL: https://docs.google.com/spreadsheets/d/{SHEET_ID}")


def setup_staff_sheet(spreadsheet, check_only: bool,
                      csv_path: str = None) -> None:
    print(f"\n{SEP}")
    print("  STAFF SHEET")
    print(SEP)

    ws = get_or_create(spreadsheet, "Staff", rows=500, cols=3)

    STAFF_HEADERS = ["Код сотрудника", "Имя"]
    current = ws.row_values(1)

    if current[:2] == STAFF_HEADERS:
        ok("Staff headers are already correct.")
    elif check_only:
        warn(f"Staff headers mismatch: got {current}")
    else:
        ws.update(values=[STAFF_HEADERS], range_name="A1")
        ws.freeze(rows=1)
        ok("Staff headers written and row 1 frozen.")

    if check_only:
        rows = ws.get_all_values()
        info(f"Staff rows (including header): {len(rows)}")
        for r in rows[:6]:
            info(f"  {r}")
        if len(rows) > 6:
            info(f"  ... (+{len(rows)-6} more rows)")
        return

    # ── Import from CSV if provided ───────────────────────────────────────────
    if csv_path:
        path = Path(csv_path)
        if not path.exists():
            warn(f"CSV file not found: {csv_path}")
            return
        rows = []
        with open(path, encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                if i == 0 and row and row[0].strip().lower() in ("код", "kod", "code",
                                                                  "код сотрудника"):
                    continue  # skip header row in CSV
                if len(row) >= 2:
                    code = str(row[0]).strip()
                    name = str(row[1]).strip()
                    if code and name:
                        rows.append([code, name])

        if not rows:
            warn("CSV contained no valid data rows (expected: code, name).")
            return

        all_vals = ws.get_all_values()
        next_row = len(all_vals) + 1
        ws.update(values=rows, range_name=f"A{next_row}")
        ok(f"Imported {len(rows)} staff records from {csv_path}.")
        return

    # ── Interactive entry ─────────────────────────────────────────────────────
    existing = ws.get_all_values()
    existing_count = max(0, len(existing) - 1)   # minus header

    if existing_count > 0:
        info(f"Staff sheet already has {existing_count} data rows.")
        ans = input("  Add more entries interactively? [y/N] ").strip().lower()
        if ans != "y":
            return
    else:
        info("Staff sheet is empty. Enter staff members now, or press Enter to skip.")
        info("Format: <staff code>, <full name>  (e.g.: 101, Nilufar Karimova)")
        info("Type 'done' when finished.\n")

    new_rows = []
    while True:
        line = input("  Add staff (code, name) or 'done': ").strip()
        if line.lower() in ("done", "exit", "q", ""):
            break
        parts = [p.strip() for p in line.split(",", 1)]
        if len(parts) != 2 or not parts[0] or not parts[1]:
            warn("Format must be:  code, name  — try again.")
            continue
        new_rows.append(parts)
        ok(f"Queued: {parts[0]} → {parts[1]}")

    if new_rows:
        all_now = ws.get_all_values()
        next_row = len(all_now) + 1
        ws.update(values=new_rows, range_name=f"A{next_row}")
        ok(f"Written {len(new_rows)} staff records to the Staff sheet.")
    else:
        info("No staff entries added.")


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Initialise Google Sheets for amo2gsheet."
    )
    parser.add_argument("--staff",  metavar="FILE",
                        help="CSV file (code,name) to populate the Staff sheet")
    parser.add_argument("--check",  action="store_true",
                        help="Verify only — make no changes")
    parser.add_argument("--skip-staff", action="store_true",
                        help="Skip Staff sheet setup (use when it already exists)")
    args = parser.parse_args()

    mode = "CHECK ONLY" if args.check else "SETUP"
    print(f"\n{'='*60}")
    print(f"  amo2gsheet  —  Google Sheet {mode}")
    print(f"{'='*60}")
    print(f"  Spreadsheet : https://docs.google.com/spreadsheets/d/{SHEET_ID}")
    print(f"  Main tab    : {WS_NAME}")
    print(f"  Service acct: {SA_FILE}")

    try:
        gc = gspread.service_account(filename=SA_FILE)
        spreadsheet = gc.open_by_key(SHEET_ID)
        ok(f"Connected to spreadsheet: \"{spreadsheet.title}\"")
    except Exception as e:
        sys.exit(f"\n[ERROR] Could not open spreadsheet: {e}\n"
                 "Make sure the service account email is shared on the sheet "
                 "(Editor access).")

    setup_main_sheet(spreadsheet, check_only=args.check)
    if not args.skip_staff:
        setup_staff_sheet(spreadsheet, check_only=args.check, csv_path=args.staff)
    else:
        info("Skipping Staff sheet setup (--skip-staff).")

    print(f"\n{'='*60}")
    print("  All done.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
