import copy
import json
import os
import re
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch


os.environ.update({
    "ENVIRONMENT": "test",
    "AMO_SUBDOMAIN": "test",
    "AMO_CLIENT_ID": "test",
    "AMO_CLIENT_SECRET": "test",
    "AMO_REDIRECT_URI": "https://localhost/callback",
    "AMO_TOKEN_STORE": "/tmp/amo2gsheet-test-tokens.json",
    "GOOGLE_SHEET_ID": "test-sheet",
    "GOOGLE_SERVICE_ACCOUNT_FILE": "/tmp/amo2gsheet-test-service-account.json",
    "LOG_DIR": "/tmp/amo2gsheet-test-logs",
})


class _BootstrapSpreadsheet:
    pass


class _BootstrapGC:
    auth = SimpleNamespace(service_account_email="bot@example.test")

    def open_by_key(self, _key):
        return _BootstrapSpreadsheet()


class _Response:
    status_code = 200
    text = json.dumps({"_embedded": {}})

    def json(self):
        return {"_embedded": {}}


# sync_service creates its production service at import time.  Isolate that
# bootstrap from external systems; individual tests construct SheetSync directly.
with (
    patch("gspread.service_account", return_value=_BootstrapGC()),
    patch("requests.get", return_value=_Response()),
    patch("requests.post", return_value=_Response()),
    patch("requests.request", return_value=_Response()),
):
    import sync_service as subject


def _row(lead_id, order_number="", status="В процессе"):
    row = [""] * len(subject.COLUMNS)
    row[subject.ID_COL_INDEX] = str(lead_id)
    row[subject.ORDER_NUM_COL_INDEX] = order_number
    row[subject.STATUS_COL_INDEX] = status
    return row


def _column_number(letters):
    value = 0
    for char in letters:
        value = value * 26 + ord(char.upper()) - ord("A") + 1
    return value


class _FakeClient:
    def __init__(self, worksheet):
        self.worksheet = worksheet

    def fetch_sheet_metadata(self, _spreadsheet_id, params=None):
        return {
            "sheets": [
                {
                    "properties": {
                        "sheetId": self.worksheet.id,
                        "title": self.worksheet.title,
                        "gridProperties": {
                            "frozenRowCount": self.worksheet.frozen_row_count,
                        },
                    },
                }
            ]
        }

    def batch_update(self, _spreadsheet_id, body):
        self.worksheet.api_batch_updates.append(copy.deepcopy(body))
        for request in body.get("requests", []):
            if "appendCells" in request:
                if self.worksheet.before_append_cells:
                    callback = self.worksheet.before_append_cells
                    self.worksheet.before_append_cells = None
                    callback()
                append_request = request["appendCells"]
                encoded_cells = append_request["rows"][0]["values"]
                decoded = []
                for cell in encoded_cells:
                    value = cell.get("userEnteredValue") or {}
                    decoded.append(next(iter(value.values()), ""))
                last_nonempty = 0
                for index, row in enumerate(self.worksheet.values):
                    if any(str(cell).strip() for cell in row):
                        last_nonempty = index + 1
                while len(self.worksheet.values) < last_nonempty + 1:
                    self.worksheet.values.append([])
                if len(self.worksheet.values) == last_nonempty:
                    self.worksheet.values.append(decoded)
                else:
                    self.worksheet.values[last_nonempty] = decoded
        return {}


class _FakeWorksheet:
    def __init__(self, values, frozen_rows=1, title="Sheet1", sheet_id=101, spreadsheet=None):
        self.title = title
        self.id = sheet_id
        self.spreadsheet_id = "spreadsheet"
        self.spreadsheet = spreadsheet
        self.row_count = 2000
        self.frozen_row_count = frozen_rows
        self.values = copy.deepcopy(values)
        self.get_all_values_results = []
        self.updates = []
        self.cell_values = []
        self.before_batch_update = None
        self.before_append_cells = None
        self.append_called = False
        self.api_batch_updates = []
        self.client = _FakeClient(self)

    def get_all_values(self):
        if self.get_all_values_results:
            return copy.deepcopy(self.get_all_values_results.pop(0))
        return copy.deepcopy(self.values)

    def get(self, range_name, **_kwargs):
        match = re.match(r"([A-Z]+)(\d+):([A-Z]+)(\d+)$", range_name)
        if not match:
            raise AssertionError(f"unexpected range {range_name}")
        start_col = _column_number(match.group(1))
        start_row = int(match.group(2))
        end_col = _column_number(match.group(3))
        end_row = int(match.group(4))
        result = []
        for row_number in range(start_row, end_row + 1):
            if row_number > len(self.values):
                result.append([])
                continue
            row = self.values[row_number - 1]
            result.append(copy.deepcopy(row[start_col - 1:end_col]))
        return result

    def cell(self, row, col):
        if self.cell_values:
            return SimpleNamespace(value=self.cell_values.pop(0))
        value = ""
        if row <= len(self.values) and col <= len(self.values[row - 1]):
            value = self.values[row - 1][col - 1]
        return SimpleNamespace(value=value)

    def update(self, values, range_name=None, **_kwargs):
        self.updates.append((range_name, copy.deepcopy(values)))
        match = re.match(r"([A-Z]+)(\d+):([A-Z]+)(\d+)$", range_name)
        if not match:
            raise AssertionError(f"expected an explicit bounded range, got {range_name}")
        start_col = _column_number(match.group(1))
        start_row = int(match.group(2))
        while len(self.values) < start_row:
            self.values.append([])
        for row_offset, source_row in enumerate(values):
            target_row = start_row + row_offset
            while len(self.values) < target_row:
                self.values.append([])
            needed = start_col - 1 + len(source_row)
            self.values[target_row - 1].extend(
                [""] * max(0, needed - len(self.values[target_row - 1]))
            )
            for col_offset, value in enumerate(source_row):
                self.values[target_row - 1][start_col - 1 + col_offset] = value
        return {}

    def batch_update(self, data, **kwargs):
        if self.before_batch_update:
            callback = self.before_batch_update
            self.before_batch_update = None
            callback()
        for item in data:
            self.update(
                item["values"],
                range_name=item["range"],
                **kwargs,
            )
        return {}

    def append_rows(self, *_args, **_kwargs):
        self.append_called = True
        raise AssertionError("append_rows must never be used for lead records")

    def add_rows(self, count):
        self.row_count += count

    def freeze(self, rows=None, cols=None):
        if rows is not None:
            self.frozen_row_count = rows
        return {}

    def update_title(self, title):
        if self.spreadsheet is not None:
            self.spreadsheet.rename(self, title)
        else:
            self.title = title
        return {}


class _FakeSpreadsheet:
    def __init__(self, worksheets):
        self.id = "spreadsheet"
        self._worksheets = {}
        self._next_id = 200
        for worksheet in worksheets:
            worksheet.spreadsheet = self
            self._worksheets[worksheet.title] = worksheet

    def worksheet(self, title):
        try:
            return self._worksheets[title]
        except KeyError as exc:
            raise subject.gspread.WorksheetNotFound(title) from exc

    def add_worksheet(self, title, rows, cols):
        if title in self._worksheets:
            raise AssertionError(f"worksheet already exists: {title}")
        worksheet = _FakeWorksheet(
            [],
            frozen_rows=0,
            title=title,
            sheet_id=self._next_id,
            spreadsheet=self,
        )
        self._next_id += 1
        self._worksheets[title] = worksheet
        return worksheet

    def rename(self, worksheet, title):
        if title in self._worksheets and self._worksheets[title] is not worksheet:
            raise AssertionError(f"worksheet already exists: {title}")
        self._worksheets.pop(worksheet.title, None)
        worksheet.title = title
        self._worksheets[title] = worksheet

def _sheet_sync(worksheet):
    sync = subject.SheetSync.__new__(subject.SheetSync)
    sync.cfg = SimpleNamespace(
        GOOGLE_SHEET_ID="spreadsheet",
        GOOGLE_WORKSHEET_NAME="Sheet1",
        STAFF_CACHE_TTL_SEC=300,
    )
    sync.lock = threading.RLock()
    sync._sheets = {worksheet.title: worksheet}
    sync._staff_cache = {}
    sync._staff_cache_ts = 0.0
    sync._row_index = {}
    sync._row_count = {}
    sync._duplicate_ids = {}
    sync._row_snapshots = {}
    sync._recent_verified_rows = {}
    sync._ws_titles_cache = [worksheet.title]
    sync._ws_titles_ts = time.time()
    sync.spreadsheet = SimpleNamespace()
    sync.gc = SimpleNamespace()
    return sync


def _rotation_sheet_sync(spreadsheet, worksheet):
    sync = _sheet_sync(worksheet)
    sync.spreadsheet = spreadsheet
    sync.gc = SimpleNamespace(open_by_key=lambda _key: spreadsheet)
    return sync


class _FailingActiveSheet:
    def __init__(self):
        self.rotation_names = []

    def _get_or_create_month_sheet(self, _name, **_kwargs):
        raise subject.SheetIntegrityError("row 1 is not the exact expected header")

    def rotate_to_archive(self, archive_name):
        self.rotation_names.append(archive_name)
        return archive_name


class SheetSafetyTests(unittest.TestCase):
    def test_column_letter_supports_schema_growth_after_z(self):
        self.assertEqual("A", subject._column_letter(0))
        self.assertEqual("Z", subject._column_letter(25))
        self.assertEqual("AA", subject._column_letter(26))

    def test_append_cells_is_atomic_below_last_nonempty_row(self):
        ws = _FakeWorksheet([subject.COLUMNS, _row("100"), [], _row("200")])
        sync = _sheet_sync(ws)

        written_row = sync.upsert_row(_row("300"), "Sheet1")

        self.assertEqual(5, written_row)
        self.assertEqual("300", ws.values[4][subject.ID_COL_INDEX])
        self.assertFalse(ws.append_called)
        append_requests = [
            request["appendCells"]
            for body in ws.api_batch_updates
            for request in body.get("requests", [])
            if "appendCells" in request
        ]
        self.assertEqual(1, len(append_requests))
        self.assertEqual(ws.id, append_requests[0]["sheetId"])

    def test_stale_scan_cannot_overwrite_recent_trailing_write(self):
        ws = _FakeWorksheet([subject.COLUMNS, _row("100")])
        sync = _sheet_sync(ws)
        self.assertEqual(3, sync.upsert_row(_row("200"), "Sheet1"))

        # Simulate a valid but stale Sheets response that omits row 3.
        ws.get_all_values_results = [[subject.COLUMNS, _row("100")]]
        self.assertEqual(4, sync.upsert_row(_row("300"), "Sheet1"))

        self.assertEqual(
            ["ID", "100", "200", "300"],
            [row[subject.ID_COL_INDEX] for row in ws.values],
        )
        self.assertFalse(ws.append_called)

    def test_retry_preserves_id_order_number_and_status(self):
        existing = _row("100", order_number="52973", status="Отказ")
        incoming = _row("100", order_number="", status="В процессе")
        incoming[3] = "Updated customer name"
        ws = _FakeWorksheet([subject.COLUMNS, existing])
        sync = _sheet_sync(ws)

        self.assertEqual(2, sync.upsert_row(incoming, "Sheet1"))

        self.assertEqual("100", ws.values[1][subject.ID_COL_INDEX])
        self.assertEqual("52973", ws.values[1][subject.ORDER_NUM_COL_INDEX])
        self.assertEqual("Отказ", ws.values[1][subject.STATUS_COL_INDEX])
        self.assertEqual("Updated customer name", ws.values[1][3])
        self.assertEqual(["A2:A2", "D2:T2"], [item[0] for item in ws.updates])

    def test_concurrent_row_shift_cannot_replace_another_lead_id(self):
        ws = _FakeWorksheet([subject.COLUMNS, _row("100")])
        sync = _sheet_sync(ws)

        # Simulate a structural edit in the narrow verify→write window.
        ws.before_batch_update = lambda: ws.values.insert(1, _row("999"))
        with self.assertRaises(subject.SheetIntegrityError):
            sync.upsert_row(_row("100"), "Sheet1")

        self.assertEqual(
            ["ID", "999", "100"],
            [row[subject.ID_COL_INDEX] for row in ws.values],
        )

    def test_concurrent_shift_during_new_insert_still_appends(self):
        ws = _FakeWorksheet([subject.COLUMNS, _row("100")])
        sync = _sheet_sync(ws)

        # A structural edit in the integrity-check→append window cannot choose
        # a stale target: appendCells appends after the shifted last row atomically.
        ws.before_append_cells = lambda: ws.values.insert(1, _row("999"))
        self.assertEqual(4, sync.upsert_row(_row("200"), "Sheet1"))

        self.assertEqual(
            ["ID", "999", "100", "200"],
            [row[subject.ID_COL_INDEX] for row in ws.values],
        )

    def test_moved_header_fails_closed_without_writing(self):
        ws = _FakeWorksheet([_row("100"), subject.COLUMNS])
        sync = _sheet_sync(ws)

        with self.assertLogs("amo2gsheet.leads", level="CRITICAL") as logs:
            with self.assertRaises(subject.SheetIntegrityError):
                sync.upsert_row(_row("200"), "Sheet1")

        self.assertEqual([], ws.updates)
        self.assertFalse(ws.append_called)
        self.assertTrue(any("SHEET INTEGRITY BLOCKED" in item for item in logs.output))

    def test_unfrozen_exact_header_is_refrozen_before_writing(self):
        ws = _FakeWorksheet([subject.COLUMNS, _row("100")], frozen_rows=0)
        sync = _sheet_sync(ws)

        written_row = sync.upsert_row(_row("200"), "Sheet1")

        self.assertEqual(3, written_row)
        self.assertEqual(1, ws.frozen_row_count)

    def test_new_sheet_initialization_writes_only_header_and_freeze(self):
        ws = _FakeWorksheet([], frozen_rows=0)
        sync = _sheet_sync(ws)

        sync._initialize_lead_sheet_locked(ws)

        self.assertEqual([("A1:U1", [subject.COLUMNS])], ws.updates)
        self.assertEqual(1, ws.frozen_row_count)
        self.assertEqual([], ws.api_batch_updates)

    def test_rotation_archives_corrupt_tab_and_creates_canonical_sheet(self):
        corrupt_header = list(subject.COLUMNS)
        corrupt_header[0] = ""
        old_sheet = _FakeWorksheet(
            [corrupt_header, _row("100")],
            title="Sheet1",
        )
        spreadsheet = _FakeSpreadsheet([old_sheet])
        sync = _rotation_sheet_sync(spreadsheet, old_sheet)

        archive_title = sync.rotate_to_archive("07.2026")

        self.assertEqual("07.2026", archive_title)
        archived = spreadsheet.worksheet("07.2026")
        self.assertEqual(corrupt_header, archived.values[0])
        self.assertEqual("100", archived.values[1][subject.ID_COL_INDEX])

        active = spreadsheet.worksheet("Sheet1")
        self.assertEqual(subject.COLUMNS, active.values[0])
        self.assertEqual(1, active.frozen_row_count)

    def test_rotation_does_not_overwrite_existing_archive_title(self):
        old_sheet = _FakeWorksheet(
            [subject.COLUMNS, _row("100")],
            title="Sheet1",
        )
        existing_archive = _FakeWorksheet(
            [subject.COLUMNS, _row("200")],
            title="07.2026",
            sheet_id=102,
        )
        spreadsheet = _FakeSpreadsheet([old_sheet, existing_archive])
        sync = _rotation_sheet_sync(spreadsheet, old_sheet)

        archive_title = sync.rotate_to_archive("07.2026")

        self.assertEqual("07.2026 (2)", archive_title)
        self.assertEqual("200", spreadsheet.worksheet("07.2026").values[1][subject.ID_COL_INDEX])
        self.assertEqual("100", spreadsheet.worksheet("07.2026 (2)").values[1][subject.ID_COL_INDEX])
        self.assertEqual(subject.COLUMNS, spreadsheet.worksheet("Sheet1").values[0])

    def test_same_month_corruption_is_quarantined_and_state_pointer_moves(self):
        sync = subject.SyncService.__new__(subject.SyncService)
        current_month = subject.datetime.now(
            subject.timezone(subject.timedelta(hours=5))
        ).strftime("%m.%Y")
        sync.cfg = SimpleNamespace(
            DISPLAY_TZ_OFFSET=5,
            GOOGLE_WORKSHEET_NAME="Sheet1",
        )
        sync.state_lock = threading.RLock()
        sync.state = {
            "active_sheet_month": current_month,
            "lead_tab_by_lead": {"100": "Sheet1"},
        }
        sync._state_dirty = False
        sync.sheet = _FailingActiveSheet()
        sync.saved = False
        sync._save_state = lambda: setattr(sync, "saved", True)

        sync.check_and_rotate_sheet()

        self.assertEqual(1, len(sync.sheet.rotation_names))
        self.assertIn("_CORRUPT_", sync.sheet.rotation_names[0])
        self.assertEqual(sync.sheet.rotation_names[0], sync.state["lead_tab_by_lead"]["100"])
        self.assertTrue(sync.saved)

    def test_header_inside_data_rows_fails_closed(self):
        ws = _FakeWorksheet([subject.COLUMNS, _row("100"), subject.COLUMNS])
        sync = _sheet_sync(ws)

        with self.assertRaises(subject.SheetIntegrityError):
            sync.upsert_row(_row("200"), "Sheet1")

        self.assertEqual([], ws.updates)

    def test_duplicate_id_is_quarantined_from_updates_and_amo_scan(self):
        ws = _FakeWorksheet(
            [
                subject.COLUMNS,
                _row("100", order_number="51615"),
                _row("100", order_number="52801"),
            ]
        )
        sync = _sheet_sync(ws)

        rows = sync.iter_lead_statuses(tabs_filter={"Sheet1"})

        self.assertEqual(1, len(rows))
        self.assertTrue(rows[0]["duplicate"])
        self.assertEqual("100", rows[0]["lead_id"])
        with self.assertRaises(subject.DuplicateLeadIdError):
            sync.upsert_row(_row("100"), "Sheet1")
        self.assertEqual([], ws.updates)

    def test_id_is_rechecked_and_index_refreshed_before_status_update(self):
        ws = _FakeWorksheet([subject.COLUMNS, _row("100")])
        # First point lookup observes a concurrent row move; the second lookup,
        # after the locked refresh, sees the expected lead ID.
        ws.cell_values = ["999", "100"]
        sync = _sheet_sync(ws)

        sync.update_status("100", "Отказ", "Sheet1")

        self.assertEqual("U2:U2", ws.updates[-1][0])
        self.assertEqual("Отказ", ws.values[1][subject.STATUS_COL_INDEX])

    def test_removed_dashboard_and_kpi_routes_are_not_mounted(self):
        routes = {route.path for route in subject.app.routes}

        self.assertIn("/webhook/amocrm", routes)
        self.assertIn("/health", routes)
        self.assertNotIn("/dashboard", routes)
        self.assertNotIn("/api/dashboard/stats", routes)
        self.assertNotIn("/api/kpi/backfill", routes)


if __name__ == "__main__":
    unittest.main()
