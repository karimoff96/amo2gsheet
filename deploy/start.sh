#!/bin/bash
# Wrapper script called by systemd.
# Sources .env properly (handles comment lines, blank lines, quoted values)
# then exec's uvicorn — PID stays the same so systemd tracks it correctly.

APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# Export all variables from .env, skipping comments and blank lines.
# Strip Windows CRLF line endings first to avoid \r being appended to values.
set -a
# shellcheck source=/dev/null
source <(sed 's/\r//' "$APP_DIR/.env")
set +a

exec "$APP_DIR/venv/bin/python" -m uvicorn sync_service:app \
    --host 0.0.0.0 \
    --port 8000 \
    --no-access-log
