# AMO CRM ↔ Google Sheets Bot — Behavior Reference

This document describes **exactly** how the sync bot should behave for the
Google Sheet / AMO CRM integration. Use it when debugging regressions so that
fixing one bug does not break another working flow.

---

## 1. The Big Picture

The bot runs as a **FastAPI service** (via systemd `amo2gsheet.service`).
It bridges two systems:

- **amoCRM** — source of truth for lead status, pipeline, responsible user,
  contact info and the custom field "Заказ №" (order number, field_id 987889).
- **Google Sheets** — a human-readable table that admins edit. The active
  worksheet is always **Sheet1** (`GOOGLE_WORKSHEET_NAME`).

The sync is **bidirectional**:

| Direction | What triggers it | What happens |
|---|---|---|
| AMO → Sheet | Webhook POST arrives at `/webhook/amocrm` | Lead row written/updated in Sheet1 |
| Sheet → AMO | Admin fills "Заказ №" cell (row is "В процессе") | Bot PATCHes AMO to "ЗАКАЗ ОТПРАВЛЕН"; **sheet row stays "В процессе"** |
| Sheet → AMO | Admin changes "Статус" dropdown in Sheet1 | Bot PATCHes AMO to the matching status |

---

## 2. Google Sheet Structure

### Active tab: always "Sheet1"

- **Sheet1** is always the current working tab.
- On the first call of a new calendar month `check_and_rotate_sheet()` fires:
  1. Renames Sheet1 → `MM.YYYY` (e.g. `04.2026`) — the archive.
  2. Creates a fresh empty Sheet1 for the new month.
  3. Updates in-memory state so old leads point to the archive tab name.
- **Old month tabs (04.2026, 03.2026, …) are NEVER scanned** after archiving.
  They are read-only history. The bot ignores them completely.

### Column layout (`COLUMNS` list, 0-indexed)

| Col | Name | Notes |
|---|---|---|
| 0 | Компания | Pipeline display name |
| 1 | ID | AMO lead ID — used as row key |
| 2 | Заказ № | Order number written by admin |
| 3 | Ф.И.О. | Contact full name |
| 4 | Контактный номер | Phone |
| 5 | Дата заказа | Lead creation date |
| 6 | Дата доставка | Delivery date (custom field) |
| 7 | Код сотрудника | Staff code (custom field) |
| 8 | Ответственный | Responsible user name |
| 9 | Группа | Staff group/department |
| 10–13 | Продукт 1/2, Количество 1/2 | Products from AMO |
| 14 | Бюджет сделки | Deal budget |
| 15 | Регион | Region (custom field) |
| 16 | Адрес | Address (custom field) |
| 17 | Тип продажи | Sale type (custom field) |
| 18 | Продажа в рассрочку | Instalment flag |
| 19 | Воронка | Pipeline name |
| **20** | **Статус** | **Status display name — this is what admins change** |

`ID_COL_INDEX = 1`, `STATUS_COL_INDEX = 20`, `ORDER_NUM_COL_INDEX = 2`.

---

## 3. Status Names: AMO vs Sheet

The bot translates between AMO's raw status names and display names shown in
the sheet. The canonical mapping is `STATUS_DISPLAY_MAP`:

| AMO raw name | Sheet display name |
|---|---|
| NOMERATSIYALANMAGAN ZAKAZ | **В процессе** |
| ЗАКАЗ БЕЗ НУМЕРАЦИИ | **В процессе** |
| Заказ без нумерации | **В процессе** |
| ЗАКАЗ ОТПРАВЛЕН | **У курера** |
| Заказ отправлен | **У курера** |
| Консультация / КОНСУЛТАЦИЯ | Консультация |
| Заказ / ЗАКАЗ | Заказ |
| ДУМКА / Раздумье | Раздумье |
| Успешно реализовано | Успешно |
| OTKAZ / ОТКАЗ / Отказ | Отказ |
| Закрыто и не реализовано | Закрыто и не реализовано |

**Key constant:**
```
ORDER_NUM_FILLED_AMO_STATUS_DISPLAY = "У курера"
```
This is the AMO **display** name the bot targets when an admin fills Заказ №.
It resolves to AMO status "Заказ отправлен" (or its pipeline-specific equivalent).

**Reverse (Sheet → AMO) mapping** `SHEET_STATUS_TO_AMO_DISPLAY`:

| Sheet status | AMO status targeted | Notes |
|---|---|---|
| В процессе | ЗАКАЗ БЕЗ НУМЕРАЦИИ | Default; see special case below |
| В процессе + Заказ № filled | ЗАКАЗ ОТПРАВЛЕН | When the row already has an order number |
| У курера | Успешно реализовано | Operator confirms delivery |
| Отказ | Отказ | Resolves via display name "Отказ" → pipeline step |
| **Успешно** | **NEVER pushed to AMO** | Display-only for staff; bot ignores it entirely |

**Override** `AMO_STATUS_TO_SHEET_OVERRIDE` — applied when AMO webhooks write back to the sheet:

| AMO display name | Written to sheet as |
|---|---|
| Раздумье | Отказ |
| У курера | В процессе |

The "У курера" override is critical: when the bot PATCHes AMO to "ЗАКАЗ ОТПРАВЛЕН",
AMO fires a return webhook. That webhook must NOT flip the sheet row to "У курера" —
the override redirects it to "В процессе" so the sheet stays unchanged.

---

## 4. Trigger Status (ЗАКАЗ БЕЗ НУМЕРАЦИИ / В процессе)

The **trigger status** is the AMO pipeline stage that means "new order, waiting
for order number". Its display name in the sheet is **"В процессе"**.

Config:
- `TRIGGER_STATUS_ID` — numeric AMO status ID (set in `.env`)
- `TRIGGER_STATUS_NAME` — raw AMO name, default `"NOMERATSIYALANMAGAN ZAKAZ"`
- `TRIGGER_STATUS_NAMES` — comma-separated extras for multi-pipeline setups

When a lead enters the trigger status → bot **writes a full new row** to Sheet1.

---

## 5. AMO → Sheet Flow (Webhooks)

### 5a. Webhook arrival

1. AMO POSTs to `/webhook/amocrm` (form-encoded body).
2. `parse_amocrm_webhook()` extracts `(lead_id, status_id)` pairs.
3. The batch is put on `_webhook_queue`.
4. HTTP 200 is returned immediately (AMO requires response < 5s or it disables
   the webhook endpoint).

### 5b. Webhook worker (`_webhook_worker` thread)

Runs in background. Drains `_webhook_queue` one batch at a time.
Calls `service.process_webhook_leads(leads)`.

**Error handling**: if `process_webhook_leads` raises (e.g. Google Sheets 429),
the error is logged, `task_done()` is called, and the worker moves on to the
next batch. The failed batch is **not retried** — this is intentional to keep
the queue draining. Do NOT let the worker thread die; it must keep looping.

### 5c. `process_webhook_leads()` routing logic

For each `(lead_id, webhook_status_id)`:

1. **Dedup check**: if the same `(lead_id, status_id)` was processed within
   `WEBHOOK_DEDUP_TTL_SEC` (default 60s) → skip. Prevents AMO retry floods.

2. **Age filter**: if lead `updated_at < LEADS_CREATED_AFTER` → skip.

3. **Pipeline keyword filter**: if `PIPELINE_KEYWORD` is set and the lead's
   pipeline name doesn't contain it → skip.

4. **Trigger match** (`status_id in trigger_status_ids`):
   - Fetch full lead data from AMO (`/api/v4/leads/{id}?with=contacts,companies`).
   - Build the full row (`build_row()`).
   - Call `sheet.upsert_row(row, "Sheet1")` — inserts if new, updates if exists.
   - Record: `remember_sheet_status`, `remember_lead_tab`, `remember_lead_pipeline`,
     `remember_sheet_order_number` (preserving any existing Заказ №).
   - **`continue`** — do not fall through to terminal/status checks.

5. **Terminal match** (`status_id in terminal_status_id_to_name`):
   - Determines the display name (with override: Раздумье → "Отказ").
   - **"Успешно" is suppressed** — never written to sheet from a webhook.
   - Calls `sheet.update_status(lead_id, display_name, tab)`.
   - Calls `_set_expiry_for_status()` to start the cleanup countdown.

6. **Known lead, non-trigger, non-terminal**:
   - If lead has a known sheet status (`known_status` is set):
     - Looks up the display name for the new `status_id`.
     - Applies `AMO_STATUS_TO_SHEET_OVERRIDE` if applicable.
     - **"Успешно" is suppressed** here too.
     - Calls `sheet.update_status` + `remember_sheet_status`.
   - If lead has **no** known sheet status → **skipped entirely** (logged as
     "not trigger/terminal/known, skipped").

---

## 6. Sheet → AMO Flow (`sync_sheet_to_amo`)

Runs every `SYNC_POLL_SECONDS` (default 60s) inside the `worker()` thread.

**Scans: Sheet1 only.** No other tabs.

For every row in Sheet1:

### 6a. Order number trigger (Заказ № filled by admin)

Fires when ALL of the following are true:
1. Lead is tracked in `sheet_order_number_by_lead` (or is self-healed to `""`).
2. `known_order == ""` (order number was empty last time we saw it).
3. `order_number != ""` (it is now filled in the sheet).
4. `status_name == "В процессе"` (only fire at this stage).

Action:
- Fetch `pipeline_id` from state (or from AMO if missing).
- Resolve `status_id` for "У курера" (= "ЗАКАЗ ОТПРАВЛЕН") in that pipeline.
- PATCH AMO: set `status_id` + write `order_number` into field 987889.
- On success: `remember_sheet_order_number(lead_id, order_number)`.
- Record `remember_sheet_status(lead_id, "В процессе")` — the sheet row status
  is **not changed**; it must remain "В процессе".
- **`continue`** — skip the status-trigger section for this lead in this cycle.

> **The sheet row stays at "В процессе" after Заказ № is entered.**
> It only moves to "У курера" when an operator explicitly sets that in the sheet.

### 6b. Order number changed/cleared

Fires when `known_order != ""` and `known_order != order_number`.
PATCHes AMO field 987889 with the new value (or skips if clearing — empty
string would cause AMO 400 on numeric fields).

### 6c. Status trigger (admin changes Статус dropdown)

Fires when `status_name` is in `STATUS_MAP` (the configured dropdown options)
AND `status_name != known_sheet_status`.

- **"Успешно" is explicitly skipped** — display-only, never pushed to AMO.
- Special case: `status_name == "В процессе"` AND `order_number != ""`
  → target "ЗАКАЗ ОТПРАВЛЕН" (order was already issued) instead of
  "ЗАКАЗ БЕЗ НУМЕРАЦИИ". This handles operators manually resetting a row to
  "В процессе" when an order number already exists.
- Resolves `status_id` via `pipeline_status_display_to_id` → fallback to
  `pipeline_status_name_to_id` → fallback to `STATUS_MAP`.
- PATCHes AMO.
- Records `remember_sheet_status`.

End of cycle: `flush_state()` — one disk write for all changes.

---

## 7. Catch-Up (`catch_up_trigger_leads`)

Runs every ~10 minutes (every `600 // SYNC_POLL_SECONDS` worker cycles).

Purpose: self-heal leads that were in the trigger status when the service was
down (missed webhooks). AMO stops retrying after a few failures.

How:
- Queries AMO `/api/v4/leads?filter[statuses][N][pipeline_id]=P&...` for ALL
  trigger statuses across ALL pipelines.
- For each lead returned: if it has **no known sheet status** → writes a full
  row to Sheet1, same as a trigger webhook.
- Leads already tracked are left untouched.

---

## 8. Bootstrap (`bootstrap_sheet_state`)

Called once at startup after the worker loop starts.

Purpose: sync in-memory state with what is currently in Sheet1, so the first
`sync_sheet_to_amo()` cycle does not treat every visible row as a new change.

**Scans: Sheet1 only** (same tab restriction as sync loop).

For each row:
- Stores `remember_sheet_status`, `remember_sheet_order_number`,
  `remember_lead_tab`.
- Heals corrupted status cell text (e.g. Latin lookalikes in Cyrillic names).
- If `BOOTSTRAP_RECOVERY=true` (env): also queues leads whose Заказ № was
  filled in the sheet but whose order# was never pushed to AMO (power-cycle
  recovery). This runs in a **background thread** so the worker loop starts
  immediately.
- If `BOOTSTRAP_RECOVERY=false`: skips recovery entirely (default for prod to
  avoid accidental mass-replay).

---

## 9. Sheet Month Rotation (`check_and_rotate_sheet`)

Runs on every worker cycle (first thing, before sync).

- Reads `state["active_sheet_month"]` (stored as `"MM.YYYY"`).
- If current month matches → ensure Sheet1 exists, return.
- If month changed:
  1. Call `sheet.rotate_to_archive(old_month)` — renames Sheet1 → `MM.YYYY`.
  2. A fresh Sheet1 is auto-created by `_get_or_create_month_sheet`.
  3. Updates `lead_tab_by_lead` for all leads that were on Sheet1 → now point
     to the archive tab name.
  4. Saves state.

---

## 10. State File (`.sync_state.json`)

Persisted to disk on every flush. Contains:

| Key | Type | Meaning |
|---|---|---|
| `sheet_status_by_lead` | `{lead_id: str}` | Last status written to sheet |
| `sheet_order_number_by_lead` | `{lead_id: str}` | Last known order# (`""` = not yet filled) |
| `lead_tab_by_lead` | `{lead_id: str}` | Which sheet tab the lead row lives on |
| `lead_pipeline_by_lead` | `{lead_id: int}` | AMO pipeline ID |
| `lead_expiry` | `{lead_id: float}` | Unix ts after which lead is forgotten |
| `active_sheet_month` | str | Current active month, e.g. `"05.2026"` |

**Critical rule**: the KPI scheduler uses a **separate** `.kpi_sched_state.json`
file. It must NEVER write to `.sync_state.json` — doing so overwrites all lead
tracking data and causes a mass-replay storm.

---

## 11. Lead Lifecycle (End-to-End Example)

```
Manager moves lead in AMO → "ЗАКАЗ БЕЗ НУМЕРАЦИИ"
    │
    ▼
AMO sends webhook POST /webhook/amocrm
    │
    ▼ _webhook_worker → process_webhook_leads()
    │   status_id matches trigger_status_ids
    │   → sheet.upsert_row(full_row, "Sheet1")   ← row appears in Sheet1
    │   → remember_sheet_status("В процессе")
    │   → remember_sheet_order_number("")
    │
    ▼
Admin opens Sheet1, fills "Заказ №" cell with e.g. "42700"
    │
    ▼ sync_sheet_to_amo() 60s cycle
    │   known_order="" + order_number="42700" + status="В процессе"
    │   → AMO PATCH: status_id=<ЗАКАЗ ОТПРАВЛЕН>, field 987889="42700"
    │   → remember_sheet_order_number("42700")
    │   → remember_sheet_status("В процессе")    ← sheet row STAYS "В процессе"
    │   (NO sheet.update_status call here)
    │
    ▼
AMO sends return webhook for "ЗАКАЗ ОТПРАВЛЕН" (display "У курера")
    │
    ▼ process_webhook_leads()
    │   known_status="В процессе" — lead is tracked, non-trigger, non-terminal
    │   new display = "У курера"
    │   AMO_STATUS_TO_SHEET_OVERRIDE["У курера"] = "В процессе"
    │   sheet_display = "В процессе" — same as current → no-op write
    │   (sheet row remains "В процессе" ✓)
    │
    ▼
Operator sets sheet row "Статус" → "У курера"
    │
    ▼ sync_sheet_to_amo() 60s cycle
    │   status_name="У курера", known="В процессе" → changed
    │   SHEET_STATUS_TO_AMO_DISPLAY["У курера"] = "Успешно"
    │   → AMO PATCH: status_id=<Успешно реализовано>
    │   → remember_sheet_status("У курера")
    │
    ▼
AMO sends webhook for "Успешно реализовано"
    │
    ▼ process_webhook_leads()
    │   terminal_name = "Успешно реализовано" → display "Успешно"
    │   "Успешно" is suppressed — NOT written to sheet ✓
    │   _set_expiry_for_status() starts cleanup countdown
    │
    ▼ expire_finished_leads() (worker cycle)
    │   expiry timestamp reached
    │   → forget_lead() — removed from all state dicts
```

### Alternate ending: Operator sets "Отказ"

```
Operator sets sheet row "Статус" → "Отказ"
    │
    ▼ sync_sheet_to_amo() 60s cycle
    │   status_name="Отказ", known="В процессе" → changed
    │   SHEET_STATUS_TO_AMO_DISPLAY["Отказ"] = "Отказ"
    │   → AMO PATCH: status_id=<ОТКАЗ pipeline step>
    │   → remember_sheet_status("Отказ")
    │
    ▼
AMO sends webhook for "ОТКАЗ"
    │
    ▼ process_webhook_leads()
    │   terminal_name = "Отказ" → sheet_display = "Отказ"
    │   → sheet.update_status("Отказ") — no visible change ✓
```

### Alternate: Operator resets row to "В процессе" after Заказ № exists

```
Operator sets sheet row "Статус" back → "В процессе" (order# "42700" already in cell)
    │
    ▼ sync_sheet_to_amo() 60s cycle
    │   status_name="В процессе", order_number="42700"
    │   Special case: В процессе + order# → amo_lookup = "У курера" (ЗАКАЗ ОТПРАВЛЕН)
    │   → AMO PATCH: status_id=<ЗАКАЗ ОТПРАВЛЕН>
    │   → remember_sheet_status("В процессе")
```

---

## 12. What Must NEVER Happen (Common Regression Traps)

| Symptom | Root cause to check |
|---|---|
| Hundreds of AMO PATCHes per minute for old leads | `sync_sheet_to_amo()` scanning old month tabs — must only scan Sheet1 |
| Google Sheets 429 quota errors | Too many Sheets writes/min. Usually caused by mass-replay storm above |
| Webhook worker crashes → real webhooks dropped | 429 inside `process_webhook_leads` kills the batch. Worker must keep running. |
| `known_order` reset to `""` for all leads after 23:00 | KPI scheduler writing to `.sync_state.json` instead of `.kpi_sched_state.json` |
| Lead moved to trigger status but not written to Sheet1 | Webhook arrived while worker was crashed (see 429). `catch_up_trigger_leads` fixes it within 10 min. |
| Заказ № filled but AMO not PATCHed | Lead not in `sheet_order_number_by_lead` with `""` — check bootstrap and state. |
| Sheet row flips to "У курера" when Заказ № is entered | The order# trigger block is calling `sheet.update_status` — it must NOT; sheet stays "В процессе". |
| Sheet row flips to "У курера" via AMO return webhook | `AMO_STATUS_TO_SHEET_OVERRIDE["У курера"]` is missing or wrong — must map to `"В процессе"`. |
| "Отказ" from sheet not reaching AMO | Lookup key wrong — must be `"Отказ"` (display name), not `"ОТКАЗ"` (raw AMO name). |
| Status "Успешно" written to sheet from AMO webhook | The "Успешно" suppression was removed — it must always be present in both the terminal block and the non-terminal block of `process_webhook_leads`. |
| Sheet "Успешно" triggers an AMO PATCH | `sync_sheet_to_amo` must `continue` immediately when `status_name == "Успешно"`. |
| Same lead inserted 5–10× on the same date | Race condition: `iter_lead_statuses` wiped recently-inserted rows from the index. See §15. |
| New rows written starting from column B (Компания column is empty/shifted) | `append_rows` called without `table_range="A1"`. See §15. |

---

## 15. Known Bugs Fixed (Do Not Reintroduce)

### BUG-1 — Duplicate row inserts (race condition in `iter_lead_statuses`)

**Symptom:** Same lead appears 5–10 times on the same date, each at a different
row number (e.g. lead 41933085 inserted at rows 1739, 1820, 1888, 1950, … on
2026-05-11).

**Root cause:**
Two threads share `self._row_index` (the `lead_id → row_number` in-memory map):
- **Webhook worker** (`upsert_row`): inserts a row → stores `lead_id → row_N`
  in `self._row_index` under `self.lock`.
- **Sync thread** (`iter_lead_statuses`): reads the sheet via `get_all_values()`
  to refresh the index. But Google Sheets has a short cache/propagation lag — a
  row that was just appended with `append_rows()` may not appear in an
  immediately subsequent `get_all_values()` response. The old code then
  **completely replaced** `self._row_index[tab_name]` with the stale data,
  wiping the just-inserted lead from the index.

When AMO retried the webhook (after the 60 s dedup TTL), `upsert_row` found no
entry in the index → inserted the lead again. This repeated every ~11–22 minutes
as long as AMO kept retrying.

**Fix applied** (`sync_service.py` — `iter_lead_statuses`):
Instead of replacing the index, **merge under `self.lock`**: start from the
old in-memory index, overwrite with the authoritative sheet values, so that
recently-inserted rows not yet visible in the Sheets API cache are preserved.
```python
with self.lock:
    old_idx = self._row_index.get(tab_name, {})
    merged = {**old_idx, **new_idx}   # new_idx wins for shared keys
    self._row_index[tab_name] = merged
    self._row_count[tab_name] = last_data_row
```

**Do NOT change** `iter_lead_statuses` to replace the index outright without
the merge — that reintroduces this race condition.

---

### BUG-2 — New rows written starting from column B (data shifted right)

**Symptom:** Newly inserted sheet rows have "Компания" (col A) empty and the
data starting from column B, making the ID column appear in column A visually.

**Root cause:**
`gspread`'s `append_rows()` calls the Google Sheets `values.append` API. When
`table_range` is not specified, the API auto-detects the "logical table" range
by scanning the sheet from the top. Because many rows have an empty "Компания"
column (AMO leads with no linked company), the API detected the table as
starting at column B — and inserted new rows starting from column B too,
shifting every cell one column to the right.

**Fix applied** (`sync_service.py` — `upsert_row`):
Pass `table_range="A1"` explicitly:
```python
result = ws.append_rows(
    [row_data],
    value_input_option="USER_ENTERED",
    insert_data_option="INSERT_ROWS",
    table_range="A1",   # ← anchor at A1 so inserts always start at col A
)
```

**Do NOT remove** `table_range="A1"` from the `append_rows` call. Without it
the column-shift bug returns any time a batch of leads without a company is
inserted.

---

## 13. Key Configuration (`.env`)

| Variable | Purpose |
|---|---|
| `GOOGLE_WORKSHEET_NAME` | Active tab name, default `"Sheet1"` |
| `TRIGGER_STATUS_ID` | AMO status ID for "ЗАКАЗ БЕЗ НУМЕРАЦИИ" |
| `TRIGGER_STATUS_NAME` | AMO raw name, default `"NOMERATSIYALANMAGAN ZAKAZ"` |
| `TRIGGER_STATUS_NAMES` | Extra trigger names (comma-separated, multi-pipeline) |
| `DROPDOWN_STATUS_MAP_JSON` | JSON map of Sheet dropdown names → AMO status IDs |
| `BOOTSTRAP_RECOVERY` | `false` in prod — skip mass order# replay on restart |
| `SYNC_POLL_SECONDS` | Worker cycle interval, default `60` |
| `LEADS_CREATED_AFTER` | Ignore leads older than this date (Unix ts or DD.MM.YYYY) |
| `PIPELINE_KEYWORD` | Only process pipelines whose name contains this string |
| `DISPLAY_TZ_OFFSET` | UTC offset for sheet timestamps (5 = Tashkent) |

---

## 14. Log Files

| File | What it contains |
|---|---|
| `logs/app.log` | Everything — full combined output |
| `logs/leads.log` | Per-lead lifecycle: writes, status changes, expiry, order# tracking |
| `logs/webhooks.log` | Webhook batch summaries and per-lead routing decisions |
| `logs/amo_api.log` | Every AMO HTTP call: method, URL, status, duration |

All files rotate at 10 MB, keep 10 backups.
