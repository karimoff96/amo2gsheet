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
10. [Start the service](#10-start-the-service)
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
LEADS_CREATED_AFTER=0             # fill after step 7
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
LEADS_CREATED_AFTER=1740787200                # timestamp from inspect_amo.py hints
```

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

### staff.csv format

```csv
101,Нилуфар Каримова
102,Мунира Хасанова
103,Акобир Рашидов
```

The **Staff** sheet maps employee codes (from AMO's "Код сотрудника" field) to display names in the "Ответственный" column of the main sheet.

---

## 10. Start the service

### Option A — foreground (for testing)

```bash
cd /root/amo2gsheet
source venv/bin/activate
python sync_service.py
```

### Option B — systemd (production)

```bash
sudo nano /etc/systemd/system/amo2gsheet.service
```

```ini
[Unit]
Description=amoCRM <-> Google Sheets Sync
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/amo2gsheet
ExecStart=/root/amo2gsheet/venv/bin/python sync_service.py
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable amo2gsheet
sudo systemctl start amo2gsheet

# Check status
sudo systemctl status amo2gsheet

# Live logs
sudo journalctl -u amo2gsheet -f
```

### Get a public HTTPS URL (required for webhooks)

amoCRM webhooks require HTTPS. The easiest no-domain solution is Cloudflare Tunnel:

```bash
# Install
curl -L https://pkg.cloudflare.com/cloudflare-main.gpg \
  | sudo tee /usr/share/keyrings/cloudflare-main.gpg > /dev/null
echo "deb [signed-by=/usr/share/keyrings/cloudflare-main.gpg] \
  https://pkg.cloudflare.com/cloudflared any main" \
  | sudo tee /etc/apt/sources.list.d/cloudflared.list
sudo apt update && sudo apt install -y cloudflared

# Start a temporary tunnel (gives you https://xxxx.trycloudflare.com)
cloudflared tunnel --url http://localhost:8000
```

Save the `https://xxxx.trycloudflare.com` URL — you will register it in the next step.

> For a **permanent URL** without a domain:  
> Create a free Cloudflare account and set up a [Named Tunnel](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/get-started/).

---

## 11. Register the webhook in amoCRM

1. Log in to amoCRM → **Settings → Integrations → Webhooks**.
2. Click **Add webhook**.
3. Fill in:
   - **URL**: `https://xxxx.trycloudflare.com/webhook/amocrm`
   - **Events**: ✅ **Lead status changed** (and optionally Lead added, Lead updated)
4. Click **Save**.

To verify it's working:
```bash
curl https://xxxx.trycloudflare.com/health
# Expected: {"status":"ok"}
```

---

## 12. Test everything end-to-end

1. Open your amoCRM account.
2. Move any test lead to the trigger status (e.g. `NOMERATSIYALANMAGAN ZAKAZ`).
3. Within a few seconds, open the Google Sheet — a new row should appear with status **"В процессе"**.
4. Change the "статус" dropdown on that row to **"У курера"**.
5. Wait up to `SYNC_POLL_SECONDS` (default 30s) — the lead should move to **Заказ отправлен** in AMO.

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
cd /root/amo2gsheet
# Download updated files from local machine:
scp sync_service.py root@1.2.3.4:/root/amo2gsheet/
sudo systemctl restart amo2gsheet
```

### Update .env settings

```bash
nano /root/amo2gsheet/.env
sudo systemctl restart amo2gsheet
```

### Add new pipelines

1. Run `python inspect_amo.py` to get the new pipeline/status names.
2. Add entries to `PIPELINE_DISPLAY_MAP` and `STATUS_DISPLAY_MAP` in `sync_service.py`.
3. Restart the service.

### Add new staff members

```bash
# Interactive
python setup_sheet.py

# Or edit the Staff tab directly in Google Sheets
```

### View live logs

```bash
sudo journalctl -u amo2gsheet -f
```

### Useful commands quick reference

| Task | Command |
|---|---|
| Start | `sudo systemctl start amo2gsheet` |
| Stop | `sudo systemctl stop amo2gsheet` |
| Restart | `sudo systemctl restart amo2gsheet` |
| Status | `sudo systemctl status amo2gsheet` |
| Live logs | `sudo journalctl -u amo2gsheet -f` |
| Inspect AMO | `python inspect_amo.py --users --leads 3` |
| Re-init sheet | `python setup_sheet.py --check` |
| Import staff | `python setup_sheet.py --staff staff.csv` |

---

## File reference

| File | Purpose |
|---|---|
| `sync_service.py` | Main FastAPI service — webhook handler + sheet sync worker |
| `inspect_amo.py` | AMO account inspector — run once per new environment |
| `setup_sheet.py` | Google Sheet initialiser — run once per new environment |
| `import_xlsx.py` | Bulk lead importer from Excel |
| `.env` | All configuration and secrets |
| `.env.example` | Template — copy to `.env` and fill in |
| `gsheet.json` | Google service account key (never commit) |
| `.amo_tokens.json` | AMO access/refresh tokens (auto-generated, never commit) |
| `.sync_state.json` | Tracks last-known sheet status per lead (auto-generated) |
