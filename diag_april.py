"""
diag_april.py — Month-rollover diagnostic for amo2gsheet
Run on server: cd /home/amo2gsheet && source venv/bin/activate && python3 diag_april.py

Checks specific to the April 2026 month-rotation failure:
  1.  Service running & recent errors
  2.  State file: active_sheet_month, lead count, tab pointers
  3.  Sheet rotation log lines (did rotate_to_archive fire?)
  4.  Google Sheets: what tabs actually exist now?
  5.  New Sheet1: does it exist and is it writable?
  6.  LEADS_CREATED_AFTER vs first April lead timestamps
  7.  Catch-up: AMO leads currently in trigger status
  8.  Webhook: last received + last written
  9.  Self-test: write one cell to Sheet1 and read it back
"""

import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

APP_DIR = Path("/home/amo2gsheet")
if not APP_DIR.exists():
    APP_DIR = Path("/root/amo2gsheet")

sys.path.insert(0, str(APP_DIR))
from env_loader import load_env
load_env()

SEP = "─" * 72
def section(t): print(f"\n{SEP}\n  {t}\n{SEP}")
def ok(m):   print(f"  [OK]   {m}")
def warn(m): print(f"  [WARN] {m}")
def err(m):  print(f"  [ERR]  {m}")
def info(m): print(f"  [INFO] {m}")

# ── shared helpers ────────────────────────────────────────────────────────────
LOG_DIR   = Path(os.getenv("LOG_DIR", str(APP_DIR / "logs")))
APP_LOG   = Path("/var/log/amo2gsheet/app.log")
STATE_FILE = APP_DIR / ".sync_state.json"

def tail_log(path: Path, n: int = 2000) -> list[str]:
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8", errors="replace").splitlines()[-n:]

def grep_log(path: Path, pattern: str, n: int = 2000) -> list[str]:
    lines = tail_log(path, n)
    rx = re.compile(pattern, re.I)
    return [l for l in lines if rx.search(l)]

# ── 1. Service status ─────────────────────────────────────────────────────────
section("1. SYSTEMD SERVICE STATUS")
result = subprocess.run(
    "systemctl status amo2gsheet --no-pager -l",
    shell=True, capture_output=True, text=True
)
out = (result.stdout + result.stderr)
print(out[:2000])
if "running" in out:
    ok("Service is RUNNING")
elif "failed" in out:
    err("Service is FAILED — check journalctl -u amo2gsheet -n 50")
else:
    warn("Service status unclear")

section("1b. LAST 5 ERRORS/WARNINGS IN APP LOG")
for line in grep_log(APP_LOG, r"\[ERROR\]|\[WARN\]|Exception|Traceback")[-10:]:
    print(f"  {line[:200]}")

# ── 2. State file ─────────────────────────────────────────────────────────────
section("2. SYNC STATE FILE")
if not STATE_FILE.exists():
    err(f"State file not found: {STATE_FILE}")
else:
    state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    active_month = state.get("active_sheet_month", "(not set)")
    sheet_statuses = state.get("sheet_status_by_lead", {})
    lead_tabs = state.get("lead_tab_by_lead", {})
    order_nums = state.get("sheet_order_number_by_lead", {})

    info(f"active_sheet_month  = {active_month!r}")
    info(f"Leads tracked       = {len(sheet_statuses)}")
    info(f"Lead tab pointers   = {len(lead_tabs)}")

    # Expected: "04.2026"
    tz = timezone(timedelta(hours=float(os.getenv("DISPLAY_TZ_OFFSET", "5"))))
    expected_month = datetime.now(tz).strftime("%m.%Y")
    if active_month == expected_month:
        ok(f"active_sheet_month matches current month ({expected_month})")
    else:
        err(
            f"active_sheet_month='{active_month}' but current month='{expected_month}' "
            f"— rotation may have FAILED or not triggered yet"
        )

    # Tab distribution
    tab_counts: dict[str, int] = {}
    for tab in lead_tabs.values():
        tab_counts[tab] = tab_counts.get(tab, 0) + 1
    info(f"Tab pointer distribution: {dict(sorted(tab_counts.items()))}")

    # Leads written in April (created in AMO after Apr 1 00:00 UTC)
    apr1_ts = int(datetime(2026, 4, 1, tzinfo=timezone.utc).timestamp())
    leads_created_after = int(os.getenv("LEADS_CREATED_AFTER", "0") or 0)
    info(f"LEADS_CREATED_AFTER timestamp = {leads_created_after} "
         f"({datetime.fromtimestamp(leads_created_after, tz=timezone.utc).strftime('%d.%m.%Y %H:%M') if leads_created_after else 'all leads'})")

# ── 3. Rotation log lines ──────────────────────────────────────────────────────
section("3. SHEET ROTATION LOG LINES (last 200 lines of app.log)")
rotate_lines = grep_log(APP_LOG, r"rotat|archiv|month changed|active_sheet_month|Sheet1", n=200)
if rotate_lines:
    for l in rotate_lines[-20:]:
        print(f"  {l[:200]}")
else:
    warn("No rotation-related log lines found in last 200 lines")

section("3b. BOOTSTRAP / STARTUP LOG LINES")
for line in grep_log(APP_LOG, r"bootstrap|startup|Resolved trigger|rotate|month", n=500)[-15:]:
    print(f"  {line[:200]}")

# ── 4. Google Sheets: existing tabs ───────────────────────────────────────────
section("4. GOOGLE SHEETS — EXISTING TABS")
try:
    import gspread
    creds_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "")
    if creds_file and not creds_file.startswith("/"):
        creds_file = str(APP_DIR / creds_file)
    sheet_id = os.getenv("GOOGLE_SHEET_ID", "")
    worksheet_name = os.getenv("GOOGLE_WORKSHEET_NAME", "Sheet1")
    info(f"Creds file     = {creds_file}")
    info(f"SHEET_ID       = {sheet_id}")
    info(f"WORKSHEET_NAME = {worksheet_name}")

    if not Path(creds_file).exists():
        err(f"Creds file not found: {creds_file}")
    elif not sheet_id:
        err("GOOGLE_SHEET_ID not set")
    else:
        gc = gspread.service_account(filename=creds_file)
        sh = gc.open_by_key(sheet_id)
        tabs = [ws.title for ws in sh.worksheets()]
        ok(f"Spreadsheet accessible — {len(tabs)} tab(s): {tabs}")

        # Check Sheet1 exists
        if worksheet_name in tabs:
            ok(f"'{worksheet_name}' tab EXISTS")
            ws = sh.worksheet(worksheet_name)
            row_count = len([r for r in ws.get_all_values() if any(r)])
            info(f"'{worksheet_name}' non-empty rows: {row_count}")
        else:
            err(
                f"'{worksheet_name}' tab MISSING — rotation created archive tab "
                f"but failed to create the new Sheet1"
            )

        # Check archive tab for March
        if "03.2026" in tabs:
            ok("Archive tab '03.2026' exists (March data preserved)")
        else:
            warn("Archive tab '03.2026' not found — rotation may not have run")

except Exception as exc:
    err(f"Google Sheets check failed: {exc}")

# ── 5. Sheet1 write test ──────────────────────────────────────────────────────
section("5. SHEET1 WRITE TEST")
try:
    gc2 = gspread.service_account(filename=creds_file)
    sh2 = gc2.open_by_key(sheet_id)
    if worksheet_name in [ws.title for ws in sh2.worksheets()]:
        ws2 = sh2.worksheet(worksheet_name)
        test_val = f"DIAG_TEST_{int(time.time())}"
        ws2.update_cell(1, 1, test_val)
        read_back = ws2.cell(1, 1).value
        if read_back == test_val:
            ok(f"Write test PASSED (cell A1 = '{test_val}')")
            # Restore original value
            ws2.update_cell(1, 1, "ID")
        else:
            err(f"Write test FAILED: wrote '{test_val}' but read back '{read_back}'")
    else:
        warn(f"Skipping write test — '{worksheet_name}' tab does not exist")
except Exception as exc:
    err(f"Write test exception: {exc}")

# ── 6. AMO API — leads currently in trigger status ────────────────────────────
section("6. AMO API — LEADS IN TRIGGER STATUS RIGHT NOW")
try:
    import requests as req_lib

    subdomain = os.getenv("AMO_SUBDOMAIN", "").strip()
    domain = f"https://{subdomain}.amocrm.ru"
    token_file = os.getenv("AMO_TOKEN_STORE", "data/tokens.json")
    if not token_file.startswith("/"):
        token_file = str(APP_DIR / token_file)
    tokens = json.loads(Path(token_file).read_text())
    hdrs = {"Authorization": f"Bearer {tokens['access_token']}"}

    # Known trigger status pairs (pipeline_id, status_id) = ЗАКАЗ БЕЗ НУМЕРАЦИИ
    # Re-fetch pipeline structure to get current pairs
    r = req_lib.get(f"{domain}/api/v4/leads/pipelines?with=statuses&limit=250", headers=hdrs, timeout=15)
    trigger_name = os.getenv("TRIGGER_STATUS_NAME", "ЗАКАЗ БЕЗ НУМЕРАЦИИ")
    extra_names  = [n.strip() for n in os.getenv("TRIGGER_STATUS_NAMES", "").split(",") if n.strip()]
    all_trigger_names = [trigger_name] + extra_names

    pairs = []
    for p in r.json()["_embedded"]["pipelines"]:
        for s in p["_embedded"]["statuses"]:
            if s["name"] in all_trigger_names:
                pairs.append((p["id"], s["id"]))
                info(f"Trigger: pipeline={p['id']} ({p['name']!r}) status={s['id']} ({s['name']!r})")

    if not pairs:
        err("No trigger status pairs resolved from AMO — check TRIGGER_STATUS_NAME in .env")
    else:
        qs = "&".join(
            f"filter[statuses][{i}][pipeline_id]={pid}&filter[statuses][{i}][status_id]={sid}"
            for i, (pid, sid) in enumerate(pairs)
        )
        r2 = req_lib.get(f"{domain}/api/v4/leads?{qs}&limit=250", headers=hdrs, timeout=15)
        if r2.status_code == 204:
            ok("No leads currently in trigger status (AMO returned 204)")
        elif r2.status_code == 200:
            leads_in_trigger = (r2.json().get("_embedded") or {}).get("leads") or []
            info(f"Leads currently in trigger status: {len(leads_in_trigger)}")
            state_data = json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else {}
            known = state_data.get("sheet_status_by_lead", {})
            for l in leads_in_trigger[:20]:
                lid = str(l["id"])
                tracked = known.get(lid, "(not tracked)")
                flag = " ← NOT IN SHEET" if tracked == "(not tracked)" else ""
                info(f"  lead={lid}  status_id={l.get('status_id')}  tracked='{tracked}'{flag}")
        else:
            err(f"AMO leads query returned HTTP {r2.status_code}: {r2.text[:200]}")

except Exception as exc:
    err(f"AMO trigger check failed: {exc}")

# ── 7. Webhook activity ───────────────────────────────────────────────────────
section("7. WEBHOOK ACTIVITY (last 24h)")
now_ts = time.time()
day_ago = now_ts - 86400
arrived  = grep_log(APP_LOG, r"WEBHOOK ARRIVED", n=5000)
written  = grep_log(APP_LOG, r"WEBHOOK TRIGGER|CATCH-UP lead=", n=5000)
batch    = grep_log(APP_LOG, r"WEBHOOK BATCH done", n=5000)

info(f"WEBHOOK ARRIVED lines (all time, last 5000): {len(arrived)}")
info(f"WEBHOOK TRIGGER / CATCH-UP writes (all time, last 5000): {len(written)}")
info(f"WEBHOOK BATCH done lines: {len(batch)}")
if arrived:
    ok(f"Last arrival : {arrived[-1][:160]}")
else:
    err("No WEBHOOK ARRIVED lines — webhooks not reaching the service")
if written:
    ok(f"Last write   : {written[-1][:160]}")
else:
    err("No leads written since last restart — possible rotation or Sheet1 issue")
if batch:
    last_batch = batch[-1]
    # Extract written count
    m = re.search(r"written=(\d+)", last_batch)
    written_count = m.group(1) if m else "?"
    info(f"Last batch   : {last_batch[:160]}")
    if written_count == "0":
        warn("Last batch wrote 0 leads — all webhooks are being skipped")

section("7b. RECENT skip_mismatch CAUSES")
# Show last 5 skipped with their status_id to spot unknown pipelines
skip_lines = grep_log(APP_LOG, r"not trigger.terminal.known, skipped", n=1000)
info(f"Total skip_mismatch lines (last 1000): {len(skip_lines)}")
# Count by status_id
sid_counts: dict[str, int] = {}
for l in skip_lines:
    m = re.search(r"status_id=(\d+)", l)
    if m:
        s = m.group(1)
        sid_counts[s] = sid_counts.get(s, 0) + 1
if sid_counts:
    info(f"Skipped status_id counts: {dict(sorted(sid_counts.items(), key=lambda x:-x[1])[:10])}")

# ── 8. Catch-up history ───────────────────────────────────────────────────────
section("8. CATCH-UP HISTORY")
cu_lines = grep_log(APP_LOG, r"CATCH-UP", n=5000)
info(f"Total CATCH-UP lines (last 5000 log lines): {len(cu_lines)}")
for l in cu_lines[-10:]:
    print(f"  {l[:200]}")

# ── Done ──────────────────────────────────────────────────────────────────────
section("DONE")
print()
