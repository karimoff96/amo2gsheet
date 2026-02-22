"""
inspect_amo.py — AMO account inspector.

Run this script after completing OAuth to get all IDs and names you need
to configure your .env file for a new production environment.

Usage:
    python inspect_amo.py
    python inspect_amo.py --leads 5      # show 5 sample leads instead of 1
    python inspect_amo.py --users        # also print all AMO users
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

# ── Config from .env ──────────────────────────────────────────────────────────
SUBDOMAIN  = os.getenv("AMO_SUBDOMAIN", "").strip()
CLIENT_ID  = os.getenv("AMO_CLIENT_ID", "").strip()
CLIENT_SEC = os.getenv("AMO_CLIENT_SECRET", "").strip()
REDIRECT   = os.getenv("AMO_REDIRECT_URI", "").strip()
TOKEN_PATH = Path(os.getenv("AMO_TOKEN_STORE", ".amo_tokens.json"))

if not SUBDOMAIN:
    sys.exit("[ERROR] AMO_SUBDOMAIN is not set in .env")

BASE = f"https://{SUBDOMAIN}.amocrm.ru"
SEP  = "=" * 64


# ── Token helpers ─────────────────────────────────────────────────────────────
def _load_tokens() -> dict:
    if TOKEN_PATH.exists():
        return json.loads(TOKEN_PATH.read_text(encoding="utf-8"))
    return {
        "access_token":  os.getenv("AMO_ACCESS_TOKEN", ""),
        "refresh_token": os.getenv("AMO_REFRESH_TOKEN", ""),
    }


def _save_tokens(access: str, refresh: str) -> None:
    TOKEN_PATH.write_text(
        json.dumps({"access_token": access, "refresh_token": refresh}, indent=2),
        encoding="utf-8",
    )


def _refresh(refresh_token: str) -> str:
    if not refresh_token:
        sys.exit(
            "[ERROR] No refresh token found.\n"
            "Complete OAuth first via POST /oauth/exchange on the running service,\n"
            "or set AMO_AUTH_CODE in .env and restart sync_service.py."
        )
    r = requests.post(
        f"{BASE}/oauth2/access_token",
        json={
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SEC,
            "grant_type":    "refresh_token",
            "refresh_token": refresh_token,
            "redirect_uri":  REDIRECT,
        },
        timeout=30,
    )
    if r.status_code != 200:
        sys.exit(f"[ERROR] Token refresh failed: {r.status_code} {r.text}")
    data = r.json()
    _save_tokens(data["access_token"], data["refresh_token"])
    print("[INFO] Token refreshed and saved.\n")
    return data["access_token"]


def get_token() -> str:
    tokens = _load_tokens()
    access  = tokens.get("access_token", "")
    refresh = tokens.get("refresh_token", "")
    if access:
        r = requests.get(
            f"{BASE}/api/v4/account",
            headers={"Authorization": f"Bearer {access}"},
            timeout=15,
        )
        if r.status_code == 200:
            return access
    return _refresh(refresh)


def amo_get(token: str, endpoint: str) -> dict:
    r = requests.get(
        f"{BASE}{endpoint}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    if r.status_code >= 400:
        print(f"[WARN] GET {endpoint} returned {r.status_code}: {r.text[:200]}")
        return {}
    return r.json()


def ts(unix) -> str:
    if not unix:
        return "—"
    return datetime.fromtimestamp(int(unix), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def section(title: str) -> None:
    print(f"\n{SEP}\n  {title}\n{SEP}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Inspect an amoCRM account.")
    parser.add_argument("--leads", type=int, default=1, metavar="N",
                        help="Number of sample leads to show (default 1)")
    parser.add_argument("--users", action="store_true",
                        help="Print all AMO user accounts")
    args = parser.parse_args()

    print(f"\nConnecting to: {BASE}")
    token = get_token()
    print(f"[OK] Authenticated  (subdomain: {SUBDOMAIN})\n")

    # ── 1. Pipelines & Statuses ───────────────────────────────────────────────
    section("PIPELINES & STATUSES")
    data = amo_get(token, "/api/v4/leads/pipelines?with=statuses&limit=250")
    pipelines = data.get("_embedded", {}).get("pipelines", [])

    if not pipelines:
        print("  [WARN] No pipelines found.")
    for p in pipelines:
        print(f"\n  Pipeline  {p['id']:<12}  {p['name']}")
        for s in p.get("_embedded", {}).get("statuses", []):
            print(f"    └ Status  {s['id']:<10}  {s['name']}")

    # ── 2. Lead Custom Fields ─────────────────────────────────────────────────
    section("LEAD CUSTOM FIELDS")
    d2 = amo_get(token, "/api/v4/leads/custom_fields?limit=250")
    fields = d2.get("_embedded", {}).get("custom_fields", [])
    print(f"  {'ID':<12} {'TYPE':<20} NAME  [allowed values]")
    print(f"  {'-'*12} {'-'*20} {'---'}")
    for cf in fields:
        ftype = cf.get("type") or cf.get("field_type", "?")
        fname = cf.get("name") or cf.get("field_name", "?")
        enums = cf.get("enums") or []
        opts  = ""
        if enums:
            sample = ", ".join(str(e.get("value", "")) for e in enums[:8])
            if len(enums) > 8:
                sample += f"  ... (+{len(enums)-8} more)"
            opts = f"   [{sample}]"
        print(f"  {cf['id']:<12} {ftype:<20} {fname}{opts}")

    # ── 3. Users ──────────────────────────────────────────────────────────────
    if args.users:
        section("AMO USERS")
        ud = amo_get(token, "/api/v4/users?limit=250")
        users = ud.get("_embedded", {}).get("users", [])
        print(f"  {'ID':<12} {'NAME':<30} EMAIL")
        print(f"  {'-'*12} {'-'*30} {'---'}")
        for u in users:
            print(f"  {u['id']:<12} {u.get('name',''):<30} {u.get('email','')}")
    else:
        print("\n  (Run with --users to also list all AMO user accounts)")

    # ── 4. Sample Leads ───────────────────────────────────────────────────────
    section(f"SAMPLE LEADS  (showing {args.leads}  |  use --leads N for more)")
    ld = amo_get(token, f"/api/v4/leads?with=contacts,companies,tags&limit={max(1, args.leads)}")
    leads = ld.get("_embedded", {}).get("leads", [])
    if not leads:
        print("  No leads found in this account.")
    for lead in leads:
        print(f"\n  ── Lead {lead.get('id')} {'─'*40}")
        print(f"  name          : {lead.get('name')}")
        print(f"  price         : {lead.get('price')}")
        print(f"  pipeline_id   : {lead.get('pipeline_id')}")
        print(f"  status_id     : {lead.get('status_id')}")
        print(f"  responsible   : {lead.get('responsible_user_id')}")
        print(f"  created_at    : {ts(lead.get('created_at', 0))}  "
              f"(unix: {lead.get('created_at')})")
        print(f"  updated_at    : {ts(lead.get('updated_at', 0))}")
        cfvs = lead.get("custom_fields_values") or []
        if cfvs:
            print("  custom fields :")
            for cf in cfvs:
                vals = [v.get("value") for v in (cf.get("values") or [])]
                print(f"    [{cf.get('field_id')}] {cf.get('field_name')}: {vals}")
        tags = (lead.get("_embedded") or {}).get("tags") or []
        if tags:
            print(f"  tags          : {', '.join(t.get('name', '') for t in tags)}")

    # ── 5. .env / code configuration hints ───────────────────────────────────
    section(".env  AND  sync_service.py  CONFIGURATION HINTS")
    now_ts = int(datetime.now(tz=timezone.utc).timestamp())

    print(f"""
  ┌─ .env settings ──────────────────────────────────────────────┐

  # Set this to the EXACT name of the status that should trigger
  # a lead row to be written to Google Sheets.
  # (Find it in the PIPELINES & STATUSES section above)
  TRIGGER_STATUS_NAME=<paste status name here>

  # Ignore leads created before your go-live date.
  # Useful timestamps:
  #   Now  ({datetime.now().strftime('%Y-%m-%d %H:%M')}):  {now_ts}
  #   2026-03-01 00:00 UTC:           1740787200
  #   2026-01-01 00:00 UTC:           1735689600
  LEADS_CREATED_AFTER={now_ts}

  └──────────────────────────────────────────────────────────────┘
""")

    print("  ┌─ sync_service.py  mappings to add / update ──────────────────┐\n")
    print("  PIPELINE_DISPLAY_MAP  (raw AMO name  →  short display name):\n")
    for p in pipelines:
        print(f'    "{p["name"]}": "<short name>",')

    print("\n  STATUS_DISPLAY_MAP  (raw AMO status name  →  display name in sheet):\n")
    skip = {"Успешно реализовано", "Закрыто и не реализовано", "Неразобранное"}
    for p in pipelines:
        for s in p.get("_embedded", {}).get("statuses", []):
            if s["name"] not in skip:
                print(f'    "{s["name"]}": "<display name>",')
    print("\n  └──────────────────────────────────────────────────────────────┘")

    print(f"\n{SEP}")
    print(f"  Done. Tokens saved to: {TOKEN_PATH}")
    print(SEP + "\n")


if __name__ == "__main__":
    main()

