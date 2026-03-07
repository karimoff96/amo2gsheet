# Sync Logic Reference

**Read this before touching `SHEET_STATUS_TO_AMO_DISPLAY`, `STATUS_MAP`,
`sync_sheet_to_amo`, or `process_webhook_leads`.**

---

## Sheet dropdown values and what they mean

| Sheet cell value | Meaning | What the script does |
|---|---|---|
| `В процессе` | Order placed, waiting for Заказ № | Update AMO status to В процессе if changed |
| `У курера` | Order is with the courier — **closes the deal** | PATCH AMO → **Успешно реализовано** (won status) |
| `Успешно` | Staff bookkeeping label only | **IGNORED — never pushed to AMO** |
| `Отказ` | Customer refused | PATCH AMO → Отказ (reject step) |

> **Critical rule:**  `Успешно` on the sheet is written BY the webhook when AMO
> sends `Успешно реализовано`.  If we pushed it back to AMO we'd create a loop.
> It is explicitly skipped in `sync_sheet_to_amo`.

---

## Direction 1 — AMO → Google Sheet (webhook)

Triggered by `process_webhook_leads` on every incoming AMO webhook.

### Trigger status (ЗАКАЗ БЕЗ НУМЕРАЦИИ and variants)
- **Create / overwrite** the row in Sheet1 with full lead data.
- Status cell → `"В процессе"`.
- Start tracking: `known_status`, `known_order`, `lead_tab`.

### Terminal / tracked lead status change
AMO status name → sheet display name (via `STATUS_DISPLAY_MAP`):

| AMO status (raw) | Sheet cell written |
|---|---|
| `Успешно реализовано` | `Успешно` |
| `ЗАКАЗ ОТПРАВЛЕН` / `Заказ отправлен` | `У курера` |
| `ОТКАЗ` / `Отказ` | `Отказ` |
| `Раздумье` | `Отказ` ← override via `AMO_STATUS_TO_SHEET_OVERRIDE` |

**Suppression rules (webhook → sheet update is blocked when):**

| Incoming AMO status → sheet value | Current sheet status | Reason |
|---|---|---|
| `ЗАКАЗ ОТПРАВЛЕН` → `У курера` | `В процессе` | Order-fill webhook must not overwrite В процессе before admin advances it |
| `Успешно реализовано` → `Успешно` | `У курера` | Admin set У курера → script PATCHed AMO → AMO echoes back Успешно реализовано; sheet must stay У курера |

---

## Direction 2 — Google Sheet → AMO (poll every 10 s)

Handled by `sync_sheet_to_amo`.

**Processing rules in order:**

1. Skip if `status_name` not in `cfg.STATUS_MAP` (not a tracked dropdown value).
2. **Skip if `status_name == "Успешно"`** — display-only, never push to AMO.
3. Skip if `known == status_name` — no change, nothing to do.
4. Translate via `SHEET_STATUS_TO_AMO_DISPLAY` → `amo_lookup`:

| Sheet value | `amo_lookup` | Resolves to AMO status |
|---|---|---|
| `У курера` | `"Успешно"` | `Успешно реализовано` (won) |
| `Отказ` | `"Отказ"` | Pipeline's reject step |
| `В процессе` | `"В процессе"` | В процессе stage |

5. Look up `status_id` via `pipeline_status_display_to_id[pipeline_id][amo_lookup]`.
6. PATCH AMO.  On success: `remember_sheet_status(lead_id, status_name)`.

---

## Order-number trigger (also in `sync_sheet_to_amo`)

When admin fills the `Заказ №` cell for a lead at `"В процессе"`:
- PATCH AMO → `Заказ отправлен` (display `"У курера"`).
- AMO fires a webhook back with `ЗАКАЗ ОТПРАВЛЕН` — this is **suppressed** by
  the rule above (sheet is still `В процессе`).

---

## Key constants — do not change without reading this file

| Constant | Purpose |
|---|---|
| `SHEET_STATUS_TO_AMO_DISPLAY` | Sheet value → AMO display name for PATCH lookup |
| `AMO_STATUS_TO_SHEET_OVERRIDE` | AMO display name → sheet override (e.g. Раздумье→Отказ) |
| `STATUS_DISPLAY_MAP` | Raw AMO status name → normalised sheet display name |
| `ORDER_NUM_FILLED_AMO_STATUS_DISPLAY` | AMO display name for order-fill PATCH (`"У курера"`) |
| `cfg.STATUS_MAP` | Which sheet dropdown values are eligible for Sheet→AMO push |
| `_EXPIRY_SECONDS` | Empty `{}` intentionally — leads are never auto-forgotten |
