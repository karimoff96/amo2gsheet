#!/usr/bin/env bash
# ============================================================
#  update.sh — Pull latest code and restart the service
#  Run this every time you push a code change.
#  Usage: bash deploy/update.sh
# ============================================================
set -euo pipefail

PROJECT_DIR="/home/amo2gsheet"
VENV="$PROJECT_DIR/venv"

echo "==> Pulling latest code..."
cd "$PROJECT_DIR"
git pull

echo "==> Updating dependencies..."
"$VENV/bin/pip" install -r requirements.txt -q

# Give AMO a moment to finish any in-flight webhook deliveries before we
# drop the port.  AMO retries within ~5s; a 3s pause keeps the window tiny.
echo "==> Draining in-flight webhooks (3s)..."
sleep 3

echo "==> Restarting service..."
systemctl restart amo2gsheet
sleep 5
systemctl status amo2gsheet --no-pager

echo ""
echo "Live logs: journalctl -u amo2gsheet -f"
