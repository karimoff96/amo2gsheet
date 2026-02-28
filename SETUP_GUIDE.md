# Complete Setup Guide — amo2gsheet

This guide walks through everything from zero to a running production service.
Follow the steps in order.

---

## Table of Contents

1. [Create a Google Cloud service account](#1-create-a-google-cloud-service-account)
2. [Create the Google Sheet](#2-create-the-google-sheet)
3. [Create an amoCRM OAuth integration](#3-create-an-amocrm-oauth-integration)
4. [Upload the project to the server](#4-upload-the-project-to-the-server)
5. [Configure .env](#5-configure-env)
6. [Authenticate with amoCRM (OAuth)](#6-authenticate-with-amocrm-oauth)
7. [Inspect the AMO account (pipeline & status IDs)](#7-inspect-the-amo-account)
8. [Update sync_service.py maps for your pipelines](#8-update-sync_servicepy-maps)
9. [Initialise the Google Sheet](#9-initialise-the-google-sheet)
10. [Start the service (production)](#10-start-the-service)
11. [Register the webhook in amoCRM](#11-register-the-webhook-in-amocrm)
12. [Test everything end-to-end](#12-test-everything-end-to-end)
13. [Ongoing maintenance](#13-ongoing-maintenance)

---

## 1. Create a Google Cloud service account

A **service account** is a bot Google account the service uses to write to your sheet.

1. Go to [console.cloud.google.com](https://console.cloud.google.com).
2. Create a new project (or select an existing one).
3. In the left menu → **APIs & Services → Library**.
4. Search for **"Google Sheets API"** → click it → click **Enable**.
5. In the left menu → **APIs & Services → Credentials**.
6. Click **Create Credentials → Service account**.
   - Name: `amo2gsheet` (or anything you prefer)
   - Click **Create and Continue** → **Done**
7. Click on the newly created service account → tab **Keys** → **Add key → Create new key → JSON**.
8. A `.json` file downloads — this is your `gsheet.json`. Keep it secret.

> **Note the service account email address** — it looks like  
> `amo2gsheet@your-project.iam.gserviceaccount.com`  
> You will need it in the next step.

---

## 2. Create the Google Sheet

1. Go to [sheets.google.com](https://sheets.google.com) and create a new spreadsheet.
2. Click **Share** (top right) → paste the **service account email** from step 1 → role: **Editor** → Send.
3. Copy the **Spreadsheet ID** from the URL:
   ```
   https://docs.google.com/spreadsheets/d/  <<<THIS_PART>>>  /edit
   ```
   Save it — you will need it for `GOOGLE_SHEET_ID` in `.env`.

4. (Optional) Create a second tab named **Staff** for the employee code → name mapping.
   The `setup_sheet.py` script will create it automatically if it doesn't exist.

> **Staff sheet column layout** (must be exactly in this order):
> `№ | Код сотрудника | Сотрудник | Отдел`  
> Codes like `0134` are matched with or without leading zeros.

---

## 3. Create an amoCRM OAuth integration

amoCRM requires an OAuth 2.0 integration to allow external access.

1. Log in to your amoCRM account.
2. Go to **Settings → Integrations** (bottom of left sidebar).
3. Click **Create integration**.
4. Fill in:
   - **Name**: `Google Sheets Sync` (or any name)
   - **Redirect URI**: `https://yandex.uz/`  ← use exactly this (or any URL you can open after consent)
   - **Scopes**: check **Leads** (read + write) and **Contacts** (read)
5. Click **Save**. You will see:
   - **Integration ID** → this is `AMO_CLIENT_ID`
   - **Secret key** → this is `AMO_CLIENT_SECRET`
6. Leave the **Authorization code** empty for now (you will get it in step 6).

---

## 4. Upload the project to the server

### Option A — SCP from local machine (Windows PowerShell)

```powershell
# Replace 1.2.3.4 with your server IP
scp -r C:\Users\Doniyorbek\Desktop\Projects\amo2gsheet root@1.2.3.4:/root/amo2gsheet

# Upload the service account key separately (it is in .gitignore)
scp C:\path\to\gsheet.json root@1.2.3.4:/root/amo2gsheet/gsheet.json
```

### Option B — Git

```bash
# On the server
git clone https://github.com/yourrepo/amo2gsheet.git /root/amo2gsheet
# Then manually upload gsheet.json via scp (it is gitignored)
```

### Install dependencies

```bash
cd /root/amo2gsheet
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
deactivate
```

---

## 5. Configure .env

```bash
cd /root/amo2gsheet
cp .env.example .env
nano .env
```

Minimum required values to fill in:

```ini
AMO_SUBDOMAIN=yourprodsubdomain       # e.g.  mycompany  (not the full URL)
AMO_CLIENT_ID=<from step 3>
AMO_CLIENT_SECRET=<from step 3>
AMO_REDIRECT_URI=https://yandex.uz/  # must match exactly what you set in AMO

GOOGLE_SERVICE_ACCOUNT_FILE=gsheet.json
GOOGLE_SHEET_ID=<from step 2>
GOOGLE_WORKSHEET_NAME=Sheet1          # or whatever your main tab is named
```

Leave these for now — you will fill them after step 7:

```ini
TRIGGER_STATUS_NAME=              # fill after step 7
TRIGGER_STATUS_NAMES=             # extra trigger names if different pipelines use different names
PIPELINE_KEYWORD=sotuv            # only process pipelines whose name contains this (leave empty = all)
LEADS_CREATED_AFTER=0             # fill after step 7 — leads last updated before this are skipped
```

Production tuning (adjust as needed):

```ini
AMO_REQUEST_DELAY_SEC=0.5         # slow down on prod with many users
SYNC_POLL_SECONDS=30              # poll sheet every 30s (saves Sheets quota)
WEBHOOK_DEDUP_TTL_SEC=120         # ignore duplicate webhooks for 2 minutes
STAFF_CACHE_TTL_SEC=600           # cache Staff sheet for 10 minutes
```

---

## 6. Authenticate with amoCRM (OAuth)

This is a one-time step. Tokens are saved to `.amo_tokens.json` and refreshed automatically afterwards.

**Step 6a — Start the service with an empty token store**

```bash
cd /root/amo2gsheet
source venv/bin/activate
python sync_service.py
```

The service will print an auth URL in the logs:

```
[INFO] Open auth URL: https://www.amocrm.ru/oauth?client_id=...
```

**Step 6b — Open that URL**

Paste it in your browser. Log in as the amoCRM account owner and click **Allow**.

You will be redirected to a URL like:
```
https://yandex.uz/?code=def502...&state=setup&referer=...
```

Copy the **entire URL**.

**Step 6c — Exchange the code**

From your local machine or directly on the server:

```bash
curl -X POST http://localhost:8000/oauth/exchange \
  -H "Content-Type: application/json" \
  -d '{"redirect_url": "https://yandex.uz/?code=def502...&state=setup..."}'
```

You should get:
```json
{"status": "ok", "token_saved": true}
```

Tokens are now stored in `.amo_tokens.json`. **Stop the service** (`Ctrl+C`) and continue.

---

## 7. Inspect the AMO account

Run the inspector script to find pipeline names, status names, and IDs:

```bash
source venv/bin/activate

# Basic (pipelines, statuses, custom fields, 1 sample lead)
python inspect_amo.py

# See all users + 3 sample leads
python inspect_amo.py --users --leads 3
```

The script prints:

- All pipeline IDs and their exact status names
- All lead custom fields with their IDs and allowed values
- Sample leads so you can verify data is accessible
- A pre-filled `.env` snippet with a timestamp for `LEADS_CREATED_AFTER`
- A list of `STATUS_DISPLAY_MAP` and `PIPELINE_DISPLAY_MAP` entries to add to `sync_service.py`

**Write down:**
- The **exact status name** that should trigger row insertion (e.g. `NOMERATSIYALANMAGAN ZAKAZ`)
- The **exact names** of all your pipelines

---

## 8. Update sync_service.py maps

Open `sync_service.py` and update the two dictionaries at the top of the file using the names from step 7.

### PIPELINE_DISPLAY_MAP

Maps the long AMO pipeline name to a short display name written to the Sheet:

```python
PIPELINE_DISPLAY_MAP: Dict[str, str] = {
    "Nilufar - Sotuv Bioflex":   "Нилуфар",
    "My Prod Pipeline Name":     "Prod Short",
    # add all pipelines from inspect_amo.py output
}
```

### STATUS_DISPLAY_MAP

Maps raw AMO status names to human-readable names written to the Sheet:

```python
STATUS_DISPLAY_MAP: Dict[str, str] = {
    "NOMERATSIYALANMAGAN ZAKAZ": "В процессе",
    "MY PROD TRIGGER STATUS":    "В процессе",   # ← your production trigger
    "ЗАКАЗ ОТПРАВЛЕН":           "У курера",
    # ... add any others from inspect_amo.py output
}
```

> **Rule**: Every status name that AMO might send you via webhook should have an entry here.  
> Unknown names fall through unchanged, which is harmless but looks less clean.

Then update `.env`:

```ini
TRIGGER_STATUS_NAME=MY PROD TRIGGER STATUS    # exact value from PIPELINES & STATUSES
TRIGGER_STATUS_NAMES=TYPO VARIANT NAME        # comma-separated, if different pipelines use different names
PIPELINE_KEYWORD=sotuv                        # only process matching pipelines
LEADS_CREATED_AFTER=27.02.2026 00:00:00       # leads last updated before this are skipped
```

> **Note**: `LEADS_CREATED_AFTER` filters by `updated_at`, not `created_at`. A lead created months ago but moved to the trigger status after this date **will** be processed.

---

## 9. Initialise the Google Sheet

Run `setup_sheet.py` once to create headers, freeze the first row, and add the status dropdown:

```bash
source venv/bin/activate

# Full setup (creates headers, dropdown, Staff sheet)
python setup_sheet.py

# Import staff from a CSV file (two columns: code, name — no header required)
python setup_sheet.py --staff staff.csv

# Verify only — no changes made
python setup_sheet.py --check
```

### Staff sheet format

The **Staff** tab must have exactly these 4 columns with a header row:

```
№ | Код сотрудника | Сотрудник | Отдел
1   0134             BAHODIR HUSANOV    A
2   0144             ELYORBEK ISOQULOV  A
```

Employee codes from AMO's `Код ID` field are looked up here to populate the **Ответственный** column. Codes are matched with or without leading zeros.

---

## 10. Start the service

### Option A — foreground (for quick testing only)

```bash
cd /root/amo2gsheet
source venv/bin/activate
python -m uvicorn sync_service:app --host 0.0.0.0 --port 8000
```

### Option B — production (recommended): use the deploy script

The `deploy/` folder contains ready-made systemd + Cloudflare Tunnel configs with full fault tolerance.

```bash
bash deploy/deploy.sh
```

This single command:
- Installs Cloudflare Tunnel (`cloudflared`)
- Creates a Python virtualenv and installs dependencies
- Installs `deploy/amo2gsheet.service` → systemd with `Restart=always`
- Installs `deploy/cloudflared.service` → tunnel auto-restarts too
- Installs log rotation (daily, 14-day retention)
- Enables both services on boot and starts them

**Fault tolerance built in:**

| Feature | Detail |
|---|---|
| Auto-restart on crash | `Restart=always`, `RestartSec=5` |
| Boot auto-start | `systemctl enable amo2gsheet` |
| Starts after network | `After=network-online.target` |
| Crash loop protection | Max 5 restarts per 60 s, then alert |
| Graceful shutdown | 10 s to flush before SIGKILL |
| Log rotation | Daily, compressed, 14 days retained |

### Get a permanent public HTTPS URL (required for webhooks)

amoCRM webhooks require HTTPS. Use a **Named Cloudflare Tunnel** — it is free, permanent, and survives server reboots:

```bash
# 1. Log in to Cloudflare (opens browser)
cloudflared tunnel login

# 2. Create a named tunnel
cloudflared tunnel create amo2gsheet

# 3. Route a hostname to it (requires a domain on your Cloudflare account)
cloudflared tunnel route dns amo2gsheet webhook.yourdomain.com

# 4. Copy the tunnel token shown after step 2, then set it in
nano deploy/cloudflared.service
# Replace YOUR_TUNNEL_TOKEN_HERE with the actual token

# 5. Install and start the cloudflared service
cp deploy/cloudflared.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now cloudflared
```

Your permanent webhook URL will be: `https://webhook.yourdomain.com/webhook/amocrm`

> **No domain?** Use a temporary tunnel for testing: `cloudflared tunnel --url http://localhost:8000`  
> This gives a random `xxxx.trycloudflare.com` URL that changes on each restart.

---

## 11. Register the webhook in amoCRM

1. Log in to amoCRM → **Settings → Integrations → Webhooks**.
2. Click **Add webhook**.
3. Fill in:
   - **URL**: `https://webhook.yourdomain.com/webhook/amocrm`
   - **Events**: ✅ **Lead status changed** only — uncheck everything else to avoid a flood of unnecessary POST requests
4. Click **Save**.

Verify connectivity:
```bash
curl https://webhook.yourdomain.com/health
# Expected: {"status":"ok"}
```

---

## 12. Test everything end-to-end

1. Open your amoCRM account.
2. Move any test lead to the trigger status (e.g. `NOMERATSIYALANMAGAN ZAKAZ`).
3. Within a few seconds, open the Google Sheet — a new row should appear with status **"В процессе"**.
4. Fill in the **Заказ №** field on that row — within `SYNC_POLL_SECONDS` the order number should appear in AMO and the lead should move to **Заказ отправлен**.
5. Change the **Статус** dropdown to **"У курера"** — the lead should move to the won step in AMO.

If a row does not appear, check the logs:
```bash
sudo journalctl -u amo2gsheet -f
```

Common issues:

| Symptom | Likely cause |
|---|---|
| No row in sheet after status change | Webhook URL not registered, or `TRIGGER_STATUS_NAME` doesn't match exactly |
| `skipped_duplicate: N` in every response | AMO retrying — expected and harmless |
| `skipped_status_mismatch: N` | Webhook received for a non-trigger status — also expected |
| `No status ID mapping for lead X` | Status name not in `STATUS_DISPLAY_MAP` or `PIPELINE_DISPLAY_MAP` |
| `Token refresh failed` | amoCRM client secret wrong, or redirect URI mismatch |

---

## 13. Ongoing maintenance

### Update the code

```bash
# From your local machine — copy changed files then restart:
scp sync_service.py root@SERVER_IP:/root/amo2gsheet/
ssh root@SERVER_IP 'systemctl restart amo2gsheet'

# Or, if using git:
ssh root@SERVER_IP 'cd /root/amo2gsheet && bash deploy/update.sh'
```

### Update .env settings

```bash
nano /root/amo2gsheet/.env
systemctl restart amo2gsheet
```

### Add new pipelines

New pipelines matching `PIPELINE_KEYWORD` are picked up **automatically** on service restart — no code changes needed. To customise display names, add them to `PIPELINE_DISPLAY_MAP_JSON` in `.env`:

```ini
PIPELINE_DISPLAY_MAP_JSON={"Sardor - Sotuv Bioflex": "Сардор"}
```

If a new pipeline uses a different trigger status name, add it to `TRIGGER_STATUS_NAMES`.

### Add new staff members

Edit the **Staff** tab directly in Google Sheets — changes are picked up within `STAFF_CACHE_TTL_SEC` (default 5 min) without restarting.

### View live logs

```bash
journalctl -u amo2gsheet -f                  # live stream
tail -f /var/log/amo2gsheet/app.log          # log file
journalctl -u amo2gsheet --since '1 hour ago' # last hour
```

### Useful commands quick reference

| Task | Command |
|---|---|
| Start | `systemctl start amo2gsheet` |
| Stop | `systemctl stop amo2gsheet` |
| Restart | `systemctl restart amo2gsheet` |
| Status | `systemctl status amo2gsheet` |
| Live logs | `journalctl -u amo2gsheet -f` |
| Update code | `bash deploy/update.sh` |
| Inspect AMO | `python inspect_amo.py --users --leads 3` |
| Re-init sheet | `python setup_sheet.py --check` |
| Pre-deploy check | `python prod_check.py` |

---

## File reference

| File | Purpose |
|---|---|
| `sync_service.py` | Main FastAPI service — webhook handler + sheet sync worker |
| `dashboard_router.py` | Staff KPI dashboard endpoint |
| `env_loader.py` | DEV / PROD environment switching |
| `inspect_amo.py` | AMO account inspector — run once per new environment |
| `setup_sheet.py` | Google Sheet initialiser — run once per new environment |
| `prod_check.py` | Pre-deploy readiness checker (no writes) |
| `import_xlsx.py` | Bulk lead importer from Excel |
| `deploy/deploy.sh` | Full server setup script |
| `deploy/update.sh` | Pull latest code + restart |
| `deploy/amo2gsheet.service` | systemd unit — auto-restart, boot-start |
| `deploy/cloudflared.service` | systemd unit for Cloudflare Tunnel |
| `deploy/amo2gsheet.logrotate` | Log rotation config |
| `.env` | All configuration and secrets |
| `.env.example` | Template — copy to `.env` and fill in |
| `prod_gsheet.json` | Google service account key, PROD (never commit) |
| `dev_gsheet.json` | Google service account key, DEV (never commit) |
| `.amo_tokens_prod.json` | AMO tokens, PROD (auto-generated, never commit) |
| `.amo_tokens_dev.json` | AMO tokens, DEV (auto-generated, never commit) |
| `.sync_state.json` | Tracks last-known sheet status per lead (auto-generated) |
