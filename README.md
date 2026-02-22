# amoCRM ↔ Google Sheets Sync

A lightweight Python service that keeps amoCRM leads and a Google Sheet in sync:

- **AMO → Sheet**: When a lead reaches the trigger status (`NOMERATSIYALANMAGAN ZAKAZ` / "Заказ без нумерации"), it is written as a new row in Google Sheets with the display status **"В процессе"**.
- **Sheet → AMO**: A background worker polls the sheet every N seconds. When a user changes the status dropdown in the sheet, the lead is moved to the corresponding pipeline stage in AMO.

---

## Project Structure

```
amo2gsheet/
├── sync_service.py     # Main FastAPI service — webhook receiver + sheet sync worker
├── import_xlsx.py      # One-off bulk importer: push leads from an Excel file into AMO
├── inspect_amo.py      # Utility: print all pipelines/statuses and custom fields from AMO
├── requirements.txt    # Python dependencies
├── .env                # ← YOUR config (never commit this)
├── .env.example        # Template — copy to .env and fill in values
├── gsheet.json         # ← Google service-account key (never commit this)
├── .amo_tokens.json    # Auto-generated after first OAuth exchange (never commit this)
└── .sync_state.json    # Auto-generated — tracks last-known sheet status per lead
```

---

## Quick Start

### 1. Prerequisites

- Python 3.10+
- An **amoCRM** account with an OAuth integration created at `amocrm.ru/developers/`
- A **Google Cloud** service account with Google Sheets API enabled; download its JSON key as `gsheet.json`
- The service account email must be **shared** on the target Google Sheet (Editor)

### 2. Install dependencies

```bash
python -m venv venv
# Windows:
venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate

pip install -r requirements.txt
```

### 3. Configure

```bash
cp .env.example .env
```

Edit `.env` — all settings are documented inline (see **Configuration Reference** below).

### 4. OAuth authentication (first run only)

1. Start the service: `python sync_service.py`
2. Open the AMO OAuth consent URL in a browser (the service prints it on startup if tokens are missing).
3. After approving, paste the full redirect URL into `AMO_AUTH_CODE` in `.env`, then restart.
   OR call the exchange endpoint directly:
   ```bash
   curl -X POST http://localhost:8000/oauth/exchange \
     -H "Content-Type: application/json" \
     -d '{"redirect_url": "https://yandex.uz/?code=...&state=setup"}'
   ```
   Tokens are saved automatically to `.amo_tokens.json`.

### 5. Expose the service with ngrok (for webhooks)

```bash
ngrok http 8000
```

Copy the `https://xxxx.ngrok.io` URL and register it as a webhook in AMO for the **lead status changed** event (point to `/webhook/amocrm`).

### 6. Run

```bash
python sync_service.py
```

---

## Configuration Reference

All settings live in `.env`. Copy `.env.example` as your starting point.

| Variable | Required | Default | Description |
|---|---|---|---|
| `AMO_SUBDOMAIN` | ✅ | — | Your AMO subdomain (e.g. `mycompany`) |
| `AMO_CLIENT_ID` | ✅ | — | OAuth integration client ID |
| `AMO_CLIENT_SECRET` | ✅ | — | OAuth integration client secret |
| `AMO_REDIRECT_URI` | ✅ | — | Redirect URI registered in the integration |
| `AMO_AUTH_CODE` | First run | — | Full redirect URL from OAuth consent (one-time) |
| `AMO_TOKEN_STORE` | | `.amo_tokens.json` | Where access/refresh tokens are stored |
| `GOOGLE_SERVICE_ACCOUNT_FILE` | ✅ | `gsheet.json` | Path to the service account JSON key |
| `GOOGLE_SHEET_ID` | ✅ | — | Google Sheet ID (from its URL) |
| `GOOGLE_WORKSHEET_NAME` | | `Sheet1` | Tab name inside the spreadsheet |
| `TRIGGER_STATUS_NAME` | ✅ | `NOMERATSIYALANMAGAN ZAKAZ` | Exact AMO status name that triggers row insertion |
| `TRIGGER_STATUS_ID` | | `0` | Hard-coded fallback trigger ID (0 = auto-detect) |
| `PIPELINE_ID` | | `0` | Hard-coded fallback pipeline ID (0 = auto-detect) |
| `DROPDOWN_STATUS_MAP_JSON` | | `{}` | JSON map of sheet dropdown values to AMO status IDs (0 = auto) |
| `LEADS_CREATED_AFTER` | | `0` | Unix timestamp — ignore leads created before this date. **Set this on prod to skip historical data.** |
| `AMO_REQUEST_DELAY_SEC` | | `0.2` | Minimum seconds between AMO API calls (throttle). Raise to `0.5`–`1.0` if you see 429s. |
| `STAFF_CACHE_TTL_SEC` | | `300` | Seconds to cache the Staff sheet lookup |
| `SYNC_POLL_SECONDS` | | `10` | Seconds between Sheet → AMO sync polls |
| `HOST` | | `0.0.0.0` | Bind host for the FastAPI server |
| `PORT` | | `8000` | Bind port |
| `IMPORT_BATCH_SIZE` | | `50` | Leads per batch for `import_xlsx.py` |
| `IMPORT_DELAY_SEC` | | `0.5` | Delay between batches for `import_xlsx.py` |

---

## Status Mapping

### AMO → Google Sheet (read-only in code)

These are defined in `STATUS_DISPLAY_MAP` inside `sync_service.py`:

| AMO status name | Shown in sheet as |
|---|---|
| `NOMERATSIYALANMAGAN ZAKAZ` / `Заказ без нумерации` | **В процессе** |
| `ЗАКАЗ ОТПРАВЛЕН` / `Заказ отправлен` | **У курера** |
| `OTKAZ` / `ОТКАЗ` / `Отказ` | **Отказ** |
| `Успешно` / `Успешно реализовано` | **Успешно** |

### Google Sheet → AMO (dropdown-driven)

When a user changes the status dropdown in the sheet, the background worker moves the lead in AMO:

| Chosen in sheet | AMO stage it moves to |
|---|---|
| **У курера** | `Заказ отправлен` |
| **Успешно** | `Заказ отправлен` (same physical step) |
| **Отказ** | `Отказ` / `ОТКАЗ` / `OTKAZ` (pipeline-dependent) |

### Pipeline display names (read-only in code)

Long AMO pipeline names are translated into short display names in `PIPELINE_DISPLAY_MAP` inside `sync_service.py`. Update this map when adding new pipelines on prod.

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Health check |
| `POST` | `/webhook/amocrm` | amoCRM webhook receiver |
| `POST` | `/oauth/exchange` | Exchange OAuth code for tokens |
| `GET` | `/structure` | List all AMO pipelines and statuses |
| `GET` | `/leads/custom_fields` | List all AMO lead custom fields |
| `GET` | `/leads/{id}` | Inspect a single lead (all fields) |

---

## Utility Scripts

### `inspect_amo.py`

Prints all pipeline statuses and lead custom fields. Useful when setting up a new AMO account to find correct status names/IDs.

```bash
python inspect_amo.py
```

### `import_xlsx.py`

Bulk-imports leads from an Excel file (`data.xlsx`) into AMO.

```bash
python import_xlsx.py
```

---

## Production Checklist

- [ ] Set `LEADS_CREATED_AFTER` to a Unix timestamp matching your go-live date so historical leads are skipped.
- [ ] Register the ngrok (or real server) URL as an AMO webhook for **lead status changed** events.
- [ ] Update `PIPELINE_DISPLAY_MAP` and `STATUS_DISPLAY_MAP` in `sync_service.py` to match your production AMO pipeline/status names (run `inspect_amo.py` to find them).
- [ ] Set `AMO_REQUEST_DELAY_SEC=0.5` or higher if the production AMO account has many concurrent users.
- [ ] Set `SYNC_POLL_SECONDS=30` on prod to reduce Sheets API quota consumption.
- [ ] Keep `.env` and `gsheet.json` out of version control (both are in `.gitignore`).
- [ ] Consider running the service behind a proper process manager (e.g. `systemd`, `supervisor`, or a Docker container) for automatic restarts.

---

## How It Works

```
AMO lead moves to trigger status
        │
        ▼
amoCRM fires webhook → POST /webhook/amocrm
        │
        ▼
Service re-fetches full lead from AMO API  ← avoids race-condition with payload
        │
        ├─ Lead too old? (created_at < LEADS_CREATED_AFTER) → skip
        │
        ├─ Status is trigger → upsert row in Google Sheet ("В процессе")
        │
        └─ Status is terminal → update "статус" cell in existing row

Background worker (every SYNC_POLL_SECONDS):
        │
        ▼
Read all rows from Google Sheet
        │
        ▼
For each row whose "статус" changed since last poll:
        │
        └─ PATCH /api/v4/leads/{id}  →  move lead to correct AMO stage
```
