#!/bin/bash
# Wrapper script called by systemd.
# env_loader.py (called at import time inside sync_service.py) reads .env
# directly via python-dotenv, so bash does not need to export anything.
# exec keeps the PID stable so systemd tracks it correctly.

APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"

exec "$APP_DIR/venv/bin/python" -m uvicorn sync_service:app \
    --host 0.0.0.0 \
    --port 8000 \
    --no-access-log
