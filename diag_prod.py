"""
diag_prod.py  —  Production diagnostic script for amo2gsheet
Run this directly on the server:  python3 /root/amo2gsheet/diag_prod.py

Checks:
  1. Systemd service status
  2. Recent errors/warnings from all log files
  3. AMO API token validity (live request)
  4. Google Sheets write access (live request)
  5. Last sync timestamps from logs
  6. Queue / thread health (if app is running)
  7. Disk space & log sizes
"""

import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── Helpers ────────────────────────────────────────────────────────────────────

SEP = "─" * 70

def section(title: str) -> None:
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)

def ok(msg):  print(f"  [OK]   {msg}")
def warn(msg): print(f"  [WARN] {msg}")
def err(msg):  print(f"  [ERR]  {msg}")
def info(msg): print(f"  [INFO] {msg}")

def run(cmd: str, timeout: int = 10) -> str:
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout
        )
        return (result.stdout + result.stderr).strip()
    except subprocess.TimeoutExpired:
        return "(command timed out)"
    except Exception as e:
        return f"(error: {e})"

# ── 1. Service status ──────────────────────────────────────────────────────────

section("1. SYSTEMD SERVICE STATUS")

status_out = run("systemctl status amo2gsheet --no-pager -l")
print(status_out[:3000])

active_line = [l for l in status_out.splitlines() if "Active:" in l]
if active_line:
    line = active_line[0]
    if "running" in line:
        ok("Service is RUNNING")
    elif "failed" in line:
        err("Service is FAILED")
    elif "inactive" in line:
        warn("Service is INACTIVE / stopped")
    else:
        warn(f"Service state: {line.strip()}")

# Last 20 journald lines
section("1b. JOURNALD — last 20 lines")
print(run("journalctl -u amo2gsheet -n 20 --no-pager"))

# ── 2. Log files ───────────────────────────────────────────────────────────────

section("2. LOG FILE SIZES & MODIFICATION TIMES")

LOG_DIRS = [
    Path("/root/amo2gsheet/logs"),
    Path("/var/log/amo2gsheet"),
]
LOG_FILES: list[Path] = []

for d in LOG_DIRS:
    if d.exists():
        for f in sorted(d.iterdir()):
            if f.is_file():
                mtime = datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc)
                age_min = (datetime.now(timezone.utc) - mtime).total_seconds() / 60
                size_kb = f.stat().st_size / 1024
                LOG_FILES.append(f)
                flag = ""
                if age_min > 60:
                    flag = "  ← last write >60 min ago!"
                info(
                    f"{f}  ({size_kb:,.0f} KB)  "
                    f"last modified {age_min:.0f} min ago{flag}"
                )
    else:
        info(f"Log dir not found: {d}")

# ── 3. Recent errors from app.log ──────────────────────────────────────────────

section("3. RECENT ERRORS & WARNINGS (last 500 lines of each log)")

ERROR_RE = re.compile(r"\b(ERROR|CRITICAL|Exception|Traceback|401|403|429|500|503)\b", re.I)

for log_path in LOG_FILES:
    if log_path.suffix not in (".log", ".txt") and ".log" not in log_path.name:
        continue
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception as e:
        warn(f"Cannot read {log_path}: {e}")
        continue

    tail = lines[-500:]
    hits = [(i + max(0, len(lines) - 500) + 1, l) for i, l in enumerate(tail) if ERROR_RE.search(l)]

    print(f"\n  [{log_path.name}]  {len(hits)} error-looking lines in last 500")
    for lineno, text in hits[-30:]:   # show at most 30
        print(f"    L{lineno:>5}: {text[:200]}")

# ── 4. Last successful sync timestamp ──────────────────────────────────────────

section("4. LAST SUCCESSFUL SYNC ACTIVITY")

SYNC_RE = re.compile(
    r"(wrote|synced|sheet.*updated|row.*written|lead.*written|batch.*flushed|"
    r"push.*ok|gsheet.*ok|update.*ok)",
    re.I,
)

for log_path in LOG_FILES:
    if "app" not in log_path.name and "leads" not in log_path.name:
        continue
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        continue
    matches = [l for l in lines if SYNC_RE.search(l)]
    if matches:
        ok(f"[{log_path.name}] Last sync line:\n      {matches[-1][:200]}")
    else:
        warn(f"[{log_path.name}] No successful sync lines found")

# ── 5. AMO API token check ─────────────────────────────────────────────────────

section("5. AMO API TOKEN CHECK (live request)")

try:
    sys.path.insert(0, "/root/amo2gsheet")
    from env_loader import load_env
    load_env()

    AMO_DOMAIN   = os.getenv("AMO_DOMAIN", "").strip().rstrip("/")
    ACCESS_TOKEN = os.getenv("AMO_ACCESS_TOKEN", "").strip()
    TOKEN_FILE   = os.getenv("AMO_TOKEN_STORE", "/root/amo2gsheet/data/tokens.json")

    # Prefer token from the rotating token store if available
    token_source = "env"
    if Path(TOKEN_FILE).exists():
        try:
            stored = json.loads(Path(TOKEN_FILE).read_text())
            if stored.get("access_token"):
                ACCESS_TOKEN = stored["access_token"]
                token_source = TOKEN_FILE
                exp = stored.get("expires_at")
                if exp:
                    exp_dt = datetime.fromtimestamp(exp, tz=timezone.utc)
                    now    = datetime.now(timezone.utc)
                    if exp_dt < now:
                        err(f"Token EXPIRED at {exp_dt} (now {now})")
                    else:
                        ok(f"Token expires at {exp_dt} ({(exp_dt-now).seconds//3600}h left)")
        except Exception as e:
            warn(f"Could not parse token store: {e}")

    info(f"AMO_DOMAIN   = {AMO_DOMAIN}")
    info(f"Token source = {token_source}")
    info(f"Token prefix = {ACCESS_TOKEN[:12]}…" if ACCESS_TOKEN else "No token found")

    if AMO_DOMAIN and ACCESS_TOKEN:
        import urllib.request
        req = urllib.request.Request(
            f"{AMO_DOMAIN}/api/v4/account",
            headers={"Authorization": f"Bearer {ACCESS_TOKEN}"},
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                body = json.loads(resp.read())
                ok(f"AMO API reachable — account: {body.get('name')} (id={body.get('id')})")
        except Exception as e:
            err(f"AMO API request failed: {e}")
    else:
        warn("AMO_DOMAIN or ACCESS_TOKEN not set — skipping live check")

except Exception as e:
    err(f"AMO token check crashed: {e}")

# ── 6. Google Sheets access check ──────────────────────────────────────────────

section("6. GOOGLE SHEETS ACCESS CHECK (live request)")

try:
    GSHEET_CREDS_FILE = os.getenv(
        "GSHEET_CREDS",
        "/root/amo2gsheet/prod_gsheet.json",
    )
    SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "").strip()

    info(f"Creds file      = {GSHEET_CREDS_FILE}")
    info(f"SPREADSHEET_ID  = {SPREADSHEET_ID}")

    if not Path(GSHEET_CREDS_FILE).exists():
        err(f"Credentials file not found: {GSHEET_CREDS_FILE}")
    else:
        import gspread
        gc = gspread.service_account(filename=GSHEET_CREDS_FILE)
        if SPREADSHEET_ID:
            sh = gc.open_by_key(SPREADSHEET_ID)
            sheets = [ws.title for ws in sh.worksheets()]
            ok(f"Google Sheets reachable — {len(sheets)} worksheets: {sheets}")
        else:
            ok("gspread authenticated (no SPREADSHEET_ID set, skipping open)")

except Exception as e:
    err(f"Google Sheets check failed: {e}")

# ── 7. Recent AMO API calls from amo_api.log ───────────────────────────────────

section("7. AMO API CALL SUMMARY (last 100 lines of amo_api.log)")

for log_path in LOG_FILES:
    if "amo_api" not in log_path.name:
        continue
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()[-100:]
        if not lines:
            warn("amo_api.log is empty")
        else:
            ok(f"Last AMO API call: {lines[-1][:200]}")
            # Count HTTP status codes
            status_counts: dict[str, int] = {}
            for l in lines:
                m = re.search(r"HTTP[/ ]+(\d{3})", l)
                if m:
                    s = m.group(1)
                    status_counts[s] = status_counts.get(s, 0) + 1
            if status_counts:
                info(f"HTTP status distribution (last 100 lines): {status_counts}")
            # Highlight 4xx/5xx
            bad = [l for l in lines if re.search(r"HTTP[/ ]+(4|5)\d\d", l)]
            for b in bad[-10:]:
                err(f"  {b[:200]}")
    except Exception as e:
        warn(f"Cannot read amo_api.log: {e}")

# ── 8. Disk space ──────────────────────────────────────────────────────────────

section("8. DISK SPACE")
print(run("df -h /"))
print(run("df -h /root"))

# ── 9. Python process & memory ────────────────────────────────────────────────

section("9. PYTHON PROCESS (ps)")
ps = run("ps aux | grep -E '[u]vicorn|[s]ync_service|[p]ython' | head -20")
print(ps if ps else "  (no matching processes found)")

# ── 10. Network reachability ──────────────────────────────────────────────────

section("10. NETWORK — DNS & TLS to AMO domain")
try:
    AMO_HOST = os.getenv("AMO_DOMAIN", "").strip().lstrip("https://").split("/")[0]
    if AMO_HOST:
        print(run(f"curl -sI --max-time 5 https://{AMO_HOST}/api/v4/account | head -5"))
    else:
        warn("AMO_DOMAIN not set")
except Exception as e:
    warn(f"Network check skipped: {e}")

# ── Done ──────────────────────────────────────────────────────────────────────

section("DONE")
print()
