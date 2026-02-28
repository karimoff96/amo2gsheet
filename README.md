# amoCRM ↔ Google Sheets Sync

A lightweight Python service that keeps amoCRM leads and a Google Sheet in sync.

- **AMO → Sheet**: When a lead reaches the trigger status (`ЗАКАЗ БЕЗ НУМЕРАЦИИ`), it is written as a new row with status **"В процессе"**.
- **Sheet → AMO**: A background worker polls the sheet every N seconds. When a user fills in **Заказ №**, the order number is written to AMO and the lead moves to `Заказ отправлен`. When the status dropdown changes, the lead moves to the matching pipeline stage.
- **Multi-pipeline**: Supports multiple parallel sales pipelines. Pipelines are discovered automatically by keyword — no hard-coded IDs needed.

---

## Project Structure

```
amo2gsheet/
├── sync_service.py       # Main FastAPI service — webhook receiver + sheet sync worker
├── dashboard_router.py   # Staff KPI dashboard endpoint
├── env_loader.py         # DEV / PROD environment switching
├── setup_sheet.py        # One-time Google Sheet initialiser (headers, dropdowns, Staff tab)
├── inspect_amo.py        # Utility: print all pipelines/statuses and custom fields from AMO
├── import_xlsx.py        # Bulk importer: push leads from an Excel file into AMO
├── prod_check.py         # Pre-deploy readiness checker (no writes, exit 0 if clean)
├── deploy/
│   ├── deploy.sh            # Full server setup script (run once on fresh VPS)
│   ├── update.sh            # Pull latest code + restart service
│   ├── amo2gsheet.service   # systemd unit file
│   ├── cloudflared.service  # systemd unit for Cloudflare Tunnel
│   └── amo2gsheet.logrotate # logrotate config (daily, 14-day retention)
├── requirements.txt
├── .env                  # ← YOUR config (never commit)
├── .env.example          # Template — copy to .env and fill in
├── prod_gsheet.json      # ← Google service-account key, PROD (never commit)
├── dev_gsheet.json       # ← Google service-account key, DEV (never commit)
├── .amo_tokens_prod.json # Auto-generated after OAuth (never commit)
├── .amo_tokens_dev.json  # Auto-generated after OAuth (never commit)
└── .sync_state.json      # Auto-generated — tracks sheet status per lead
```

---

## Quick Start (local / dev)

### 1. Prerequisites

- Python 3.10+
- An **amoCRM** account with an OAuth integration (`amocrm.ru/developers/`)
- A **Google Cloud** service account with Sheets API enabled; save the JSON key as `dev_gsheet.json`
- Share the target Google Sheet with the service account email (Editor role)

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
# Set ENVIRONMENT=dev, then fill in DEV_* credentials
```

### 4. OAuth authentication (first run only)

Start the service and follow the printed auth URL:

```bash
python -m uvicorn sync_service:app --host 0.0.0.0 --port 8000
```

After approving in the browser, exchange the code:

```bash
curl -X POST http://localhost:8000/oauth/exchange \
  -H "Content-Type: application/json" \
  -d '{"redirect_url": "https://yandex.uz/?code=...&state=setup"}'
```

Tokens are saved automatically to `.amo_tokens_dev.json`.

### 5. Expose locally with ngrok (for webhook testing)

```bash
ngrok http 8000
```

Register `https://xxxx.ngrok-free.app/webhook/amocrm` as an AMO webhook — **Lead status changed** only.

### 6. Run

```bash
python -m uvicorn sync_service:app --host 0.0.0.0 --port 8000
```

---

## Production Deployment

See [SETUP_GUIDE.md](SETUP_GUIDE.md) for the full step-by-step guide.

**TL;DR** — on a fresh Ubuntu/Debian VPS as root:

```bash
scp -r amo2gsheet root@YOUR_SERVER_IP:/root/amo2gsheet
ssh root@YOUR_SERVER_IP
cd /root/amo2gsheet
bash deploy/deploy.sh
```

The deploy script installs Python, Cloudflare Tunnel, configures systemd with auto-restart, and sets up log rotation.

---

## Environment Switching (DEV / PROD)

Set `ENVIRONMENT=dev` or `ENVIRONMENT=prod` in `.env`. `env_loader.py` copies the matching `DEV_*` or `PROD_*` prefixed vars into the bare names the service reads.

```ini
ENVIRONMENT=prod

PROD_AMO_SUBDOMAIN=mycompany
PROD_GOOGLE_SHEET_ID=1abc...
PROD_GOOGLE_SERVICE_ACCOUNT_FILE=prod_gsheet.json
```

---

## Configuration Reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `ENVIRONMENT` | ✅ | `dev` | `dev` or `prod` — selects which credential set to use |
| `AMO_SUBDOMAIN` | ✅ | — | AMO subdomain (e.g. `mycompany`) |
| `AMO_CLIENT_ID` | ✅ | — | OAuth integration client ID |
| `AMO_CLIENT_SECRET` | ✅ | — | OAuth integration client secret |
| `AMO_REDIRECT_URI` | ✅ | — | Redirect URI registered in the integration |
| `AMO_TOKEN_STORE` | | `.amo_tokens.json` | Token file path |
| `GOOGLE_SERVICE_ACCOUNT_FILE` | ✅ | `gsheet.json` | Path to service account JSON key |
| `GOOGLE_SHEET_ID` | ✅ | — | Google Spreadsheet ID |
| `GOOGLE_WORKSHEET_NAME` | | `Sheet1` | Active tab name |
| `TRIGGER_STATUS_NAME` | ✅ | — | Exact AMO status name that triggers row insertion |
| `TRIGGER_STATUS_NAMES` | | — | Comma-separated extra trigger names (e.g. typo variants across pipelines) |
| `PIPELINE_KEYWORD` | | `` | Only process pipelines whose name contains this string (e.g. `sotuv`) |
| `PIPELINE_ID` | | `0` | Hard-coded fallback pipeline ID (0 = auto-detect per lead) |
| `TRIGGER_STATUS_ID` | | `0` | Hard-coded fallback trigger ID (0 = auto-detect) |
| `DROPDOWN_STATUS_MAP_JSON` | | `{}` | JSON map of sheet dropdown values → AMO status IDs (0 = auto) |
| `LEADS_CREATED_AFTER` | | `0` | Skip leads **last updated** before this date. Format: `DD.MM.YYYY HH:MM:SS` (UTC) or Unix timestamp. |
| `AMO_REQUEST_DELAY_SEC` | | `0.2` | Min seconds between AMO API calls. Raise to `0.5`–`1.0` on prod. |
| `STAFF_CACHE_TTL_SEC` | | `300` | Seconds to cache the Staff sheet lookup |
| `WEBHOOK_DEDUP_TTL_SEC` | | `60` | Ignore duplicate (lead, status) webhooks within this window |
| `SYNC_POLL_SECONDS` | | `10` | Seconds between Sheet → AMO sync polls |
| `SHEET_ROTATION_INTERVAL` | | `monthly` | `monthly` or `hourly` — when to archive the active tab |
| `INITIAL_SYNC_DATE_FROM` | | — | Bulk-sync AMO leads created from this date (`YYYY-MM-DD`) on startup |
| `INITIAL_SYNC_DATE_TO` | | — | Bulk-sync AMO leads created up to this date (`YYYY-MM-DD`) on startup |
| `HOST` | | `0.0.0.0` | Bind host |
| `PORT` | | `8000` | Bind port |
| `IMPORT_BATCH_SIZE` | | `50` | Leads per batch for `import_xlsx.py` |
| `IMPORT_DELAY_SEC` | | `0.5` | Delay between batches for `import_xlsx.py` |

---

## Status Mapping

### AMO → Google Sheet

Defined in `STATUS_DISPLAY_MAP` inside `sync_service.py`:

| AMO status name | Shown in sheet as |
|---|---|
| `ЗАКАЗ БЕЗ НУМЕРАЦИИ` / `ЗАЗАЗ БЕЗ НУМЕРАЦИИ` | **В процессе** |
| `NOMERATSIYALANMAGAN ZAKAZ` / `Заказ без нумерации` | **В процессе** |
| `ЗАКАЗ ОТПРАВЛЕН` / `Заказ отправлен` | **У курера** |
| `OTKAZ` / `ОТКАЗ` / `Отказ` | **Отказ** |
| `Успешно` / `Успешно реализовано` | **Успешно** |

### Google Sheet → AMO (dropdown-driven)

| Chosen in sheet | AMO action |
|---|---|
| **Заказ №** filled by admin | Order number written to AMO custom field + lead moved to `Заказ отправлен` |
| **У курера** | Lead moved to `Успешно реализовано` (won step) |
| **Успешно** | Lead moved to `Успешно реализовано` (won step) |
| **Отказ** | Lead moved to pipeline's reject step |

## Staff Sheet Format

The **Staff** tab maps employee codes to display names for the **Ответственный** column.
Required column layout (with a header row):

| № | Код сотрудника | Сотрудник | Отдел |
|---|---|---|---|
| 1 | 0134 | BAHODIR HUSANOV | A |
| 2 | 0144 | ELYORBEK ISOQULOV | A |

Codes are matched both with and without leading zeros (`0134` = `134`).

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

| Script | Purpose |
|---|---|
| `inspect_amo.py` | Print all pipelines, statuses, custom fields, sample leads |
| `setup_sheet.py` | Initialise Google Sheet (headers, dropdown, Staff tab) |
| `prod_check.py` | Pre-deploy audit — checks all config, AMO, Sheet connectivity |
| `import_xlsx.py` | Bulk-import leads from an Excel file into AMO |

---

## Production Checklist

- [ ] Set `ENVIRONMENT=prod` in `.env`
- [ ] Fill all `PROD_*` credentials in `.env`
- [ ] Set `LEADS_CREATED_AFTER` to your go-live date (leads last updated before this are skipped)
- [ ] Set `PIPELINE_KEYWORD` to filter only your relevant pipelines (e.g. `sotuv`)
- [ ] Register the Cloudflare Tunnel URL as an AMO webhook — **Lead status changed only**
- [ ] Run `python prod_check.py` — must exit with code 0
- [ ] Run `python setup_sheet.py` to initialise the Google Sheet
- [ ] Upload `prod_gsheet.json` to the server (it is gitignored)
- [ ] Run `bash deploy/deploy.sh` on the server
- [ ] Verify: `curl https://your-tunnel-url/health`

---

## How It Works

```
AMO lead moves to trigger status
        │
        ▼
amoCRM fires webhook → POST /webhook/amocrm
        │
        ▼
Service re-fetches full lead from AMO API  ← avoids race conditions
        │
        ├─ updated_at < LEADS_CREATED_AFTER? → skip (too old)
        ├─ Pipeline doesn't match PIPELINE_KEYWORD? → skip
        ├─ Duplicate webhook within WEBHOOK_DEDUP_TTL_SEC? → skip
        │
        ▼
Build row from lead data:
  • Contact name + phone (from embedded contacts)
  • Company name
  • Custom fields (Продукт, Дата заказа, Регион, Код сотрудника, …)
  • Ответственный ← looked up from Staff sheet by Код сотрудника
        │
        ▼
Upsert row in Google Sheet (update if lead_id exists, append if new)

──────── Background worker (every SYNC_POLL_SECONDS) ────────

Read all rows from Sheet
        │
        ├─ Заказ № newly filled?
        │       → PATCH AMO lead: set custom field + move to Заказ отправлен
        │
        └─ Status changed by admin?
                → PATCH AMO lead: move to matching pipeline stage
```
