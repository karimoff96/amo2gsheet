#!/usr/bin/env bash
# ============================================================
#  update.sh — Pull latest code and restart the service
#  Run this every time you push a code change.
#  Usage: bash deploy/update.sh
# ============================================================
set -euo pipefail

PROJECT_DIR="/root/amo2gsheet"
VENV="$PROJECT_DIR/venv"

echo "==> Stopping service..."
systemctl stop amo2gsheet

echo "==> Pulling latest code..."
cd "$PROJECT_DIR"
git pull

echo "==> Updating dependencies..."
"$VENV/bin/pip" install -r requirements.txt -q

echo "==> Starting service..."
systemctl start amo2gsheet
sleep 3
systemctl status amo2gsheet --no-pager

echo ""
echo "Live logs: journalctl -u amo2gsheet -f"
