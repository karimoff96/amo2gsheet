#!/bin/bash
# Wrapper script called by systemd.
# Uses python-dotenv to export .env variables — handles Cyrillic, JSON values,
# inline comments, and spaces in values without any bash parse errors.
# exec's uvicorn so PID stays the same and systemd tracks it correctly.

APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# Export all .env variables via Python (safe for any value type).
set -a
while IFS= read -r _kv; do
    [[ -z "$_kv" ]] && continue
    export "$_kv"
done < <("$APP_DIR/venv/bin/python" -c "
import sys, os
from dotenv import dotenv_values
for k, v in dotenv_values('$APP_DIR/.env').items():
    if v is None: continue
    v = v.replace(\"'\", \"'\\\"'\\\"'\")
    print(f\"{k}='{v}'\")
")
set +a

exec "$APP_DIR/venv/bin/python" -m uvicorn sync_service:app \
    --host 0.0.0.0 \
    --port 8000 \
    --no-access-log
