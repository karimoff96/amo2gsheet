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
    "GOOGLE_SHEET_OWNER_EMAILS": "owner@example.test",
    "GOOGLE_SERVICE_ACCOUNT_FILE": "/tmp/amo2gsheet-test-service-account.json",
    "KPI_DB_PATH": ":memory:",
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
        protected_ranges = []
        if self.worksheet.sheet_protected:
            protected_ranges.append({
                "range": {
                    "sheetId": self.worksheet.id,
                },
                "unprotectedRanges": [
                    {
                        "sheetId": self.worksheet.id,
                        "startRowIndex": 1,
                        "startColumnIndex": subject.ORDER_NUM_COL_INDEX,
                        "endColumnIndex": subject.ORDER_NUM_COL_INDEX + 1,
                    },
                    {
                        "sheetId": self.worksheet.id,
                        "startRowIndex": 1,
                        "startColumnIndex": subject.STATUS_COL_INDEX,
                        "endColumnIndex": subject.STATUS_COL_INDEX + 1,
                    },
                ],
                "warningOnly": False,
                "editors": {
                    "users": self.worksheet.protection_editor_users,
                    "groups": self.worksheet.protection_editor_groups,
                    "domainUsersCanEdit": (
                        self.worksheet.protection_domain_users_can_edit
                    ),
                },
            })
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
                    "protectedRanges": protected_ranges,
                }
            ]
        }

    def batch_update(self, _spreadsheet_id, body):
        self.worksheet.api_batch_updates.append(copy.deepcopy(body))
        for request in body.get("requests", []):
            if "addProtectedRange" in request:
                self.worksheet.sheet_protected = True
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
    def __init__(self, values, frozen_rows=1, sheet_protected=True):
        self.title = "Sheet1"
        self.id = 101
        self.spreadsheet_id = "spreadsheet"
        self.row_count = 2000
        self.frozen_row_count = frozen_rows
        self.sheet_protected = sheet_protected
        self.protection_editor_users = ["bot@example.test"]
        self.protection_editor_groups = []
        self.protection_domain_users_can_edit = False
        self.values = copy.deepcopy(values)
        self.get_all_values_results = []
        self.updates = []
        self.validations = []
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

    def add_validation(self, cell_range, *_args, **_kwargs):
        self.validations.append(cell_range)


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
    sync._service_account_email = "bot@example.test"
    sync._trusted_protection_editor_users = {"bot@example.test"}
    sync.spreadsheet = SimpleNamespace()
    sync.gc = SimpleNamespace()
    return sync


class SheetSafetyTests(unittest.TestCase):
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

        # Even an owner override in the validation→append window cannot choose
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

        with self.assertRaises(subject.SheetIntegrityError):
            sync.upsert_row(_row("200"), "Sheet1")

        self.assertEqual([], ws.updates)
        self.assertFalse(ws.append_called)

    def test_unfrozen_exact_header_is_refrozen_before_writing(self):
        ws = _FakeWorksheet([subject.COLUMNS, _row("100")], frozen_rows=0)
        sync = _sheet_sync(ws)

        written_row = sync.upsert_row(_row("200"), "Sheet1")

        self.assertEqual(3, written_row)
        self.assertEqual(1, ws.frozen_row_count)

    def test_unprotected_sheet_fails_closed(self):
        ws = _FakeWorksheet(
            [subject.COLUMNS, _row("100")], sheet_protected=False
        )
        sync = _sheet_sync(ws)

        with self.assertRaises(subject.SheetIntegrityError):
            sync.upsert_row(_row("200"), "Sheet1")

        self.assertEqual([], ws.updates)

    def test_google_owner_added_to_protection_metadata_is_accepted(self):
        ws = _FakeWorksheet([subject.COLUMNS, _row("100")])
        # Sheets adds the file owner to the metadata readback even though the
        # create request explicitly names only the service account.
        ws.protection_editor_users = ["bot@example.test", "owner@example.test"]
        sync = _sheet_sync(ws)
        sync._trusted_protection_editor_users.add("owner@example.test")

        self.assertEqual(3, sync.upsert_row(_row("200"), "Sheet1"))

    def test_protection_editor_normalization_is_accepted(self):
        ws = _FakeWorksheet([subject.COLUMNS, _row("100")])
        ws.protection_editor_users = [
            " BOT@EXAMPLE.TEST ",
            " OWNER@EXAMPLE.TEST ",
        ]
        sync = _sheet_sync(ws)
        sync._trusted_protection_editor_users.add("owner@example.test")

        self.assertEqual(3, sync.upsert_row(_row("200"), "Sheet1"))

    def test_unknown_second_protection_editor_fails_closed(self):
        ws = _FakeWorksheet([subject.COLUMNS, _row("100")])
        ws.protection_editor_users = [
            "bot@example.test", "unexpected-editor@example.test"
        ]
        sync = _sheet_sync(ws)

        with self.assertRaises(subject.SheetIntegrityError):
            sync.upsert_row(_row("200"), "Sheet1")

        self.assertEqual([], ws.updates)

    def test_additional_protection_editor_fails_closed(self):
        ws = _FakeWorksheet([subject.COLUMNS, _row("100")])
        ws.protection_editor_users = [
            "bot@example.test",
            "owner@example.test",
            "unexpected-editor@example.test",
        ]
        sync = _sheet_sync(ws)
        sync._trusted_protection_editor_users.add("owner@example.test")

        with self.assertRaises(subject.SheetIntegrityError):
            sync.upsert_row(_row("200"), "Sheet1")

        self.assertEqual([], ws.updates)

    def test_protection_group_fails_closed(self):
        ws = _FakeWorksheet([subject.COLUMNS, _row("100")])
        ws.protection_editor_groups = ["operators@example.test"]
        sync = _sheet_sync(ws)

        with self.assertRaises(subject.SheetIntegrityError):
            sync.upsert_row(_row("200"), "Sheet1")

    def test_domain_wide_protection_editing_fails_closed(self):
        ws = _FakeWorksheet([subject.COLUMNS, _row("100")])
        ws.protection_domain_users_can_edit = True
        sync = _sheet_sync(ws)

        with self.assertRaises(subject.SheetIntegrityError):
            sync.upsert_row(_row("200"), "Sheet1")

    def test_missing_service_account_protection_editor_fails_closed(self):
        ws = _FakeWorksheet([subject.COLUMNS, _row("100")])
        ws.protection_editor_users = ["owner@example.test"]
        sync = _sheet_sync(ws)
        sync._trusted_protection_editor_users.add("owner@example.test")

        with self.assertRaises(subject.SheetIntegrityError):
            sync.upsert_row(_row("200"), "Sheet1")

    def test_new_sheet_gets_exact_whole_sheet_protection_shape(self):
        ws = _FakeWorksheet([], frozen_rows=0, sheet_protected=False)
        sync = _sheet_sync(ws)

        sync._initialize_lead_sheet_locked(ws)

        add_requests = [
            request["addProtectedRange"]["protectedRange"]
            for body in ws.api_batch_updates
            for request in body.get("requests", [])
            if "addProtectedRange" in request
        ]
        self.assertEqual(1, len(add_requests))
        protected = add_requests[0]
        self.assertEqual({"sheetId": ws.id}, protected["range"])
        self.assertEqual(
            [
                {
                    "sheetId": ws.id,
                    "startRowIndex": 1,
                    "startColumnIndex": subject.ORDER_NUM_COL_INDEX,
                    "endColumnIndex": subject.ORDER_NUM_COL_INDEX + 1,
                },
                {
                    "sheetId": ws.id,
                    "startRowIndex": 1,
                    "startColumnIndex": subject.STATUS_COL_INDEX,
                    "endColumnIndex": subject.STATUS_COL_INDEX + 1,
                },
            ],
            protected["unprotectedRanges"],
        )
        self.assertEqual(
            ["bot@example.test"], protected["editors"]["users"]
        )

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


if __name__ == "__main__":
    unittest.main()
