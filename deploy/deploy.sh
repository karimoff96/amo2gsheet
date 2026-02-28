#!/usr/bin/env bash
# ============================================================
#  deploy.sh — Full server setup for amo2gsheet
#  Run once on a fresh Ubuntu/Debian VPS as root.
#  Usage: bash deploy.sh
# ============================================================
set -euo pipefail

PROJECT_DIR="/root/amo2gsheet"
VENV="$PROJECT_DIR/venv"
SERVICE_NAME="amo2gsheet"
LOG_DIR="/var/log/amo2gsheet"

echo "==> [1/7] Installing system packages..."
apt-get update -qq
apt-get install -y -qq python3 python3-venv python3-pip curl git

echo "==> [2/7] Installing Cloudflare Tunnel..."
if ! command -v cloudflared &>/dev/null; then
    curl -fsSL https://pkg.cloudflare.com/cloudflare-main.gpg \
        | tee /usr/share/keyrings/cloudflare-main.gpg > /dev/null
    echo "deb [signed-by=/usr/share/keyrings/cloudflare-main.gpg] \
        https://pkg.cloudflare.com/cloudflared any main" \
        | tee /etc/apt/sources.list.d/cloudflared.list
    apt-get update -qq
    apt-get install -y -qq cloudflared
    echo "    cloudflared installed: $(cloudflared --version)"
else
    echo "    cloudflared already installed: $(cloudflared --version)"
fi

echo "==> [3/7] Setting up Python virtualenv..."
cd "$PROJECT_DIR"
if [ ! -d "$VENV" ]; then
    python3 -m venv "$VENV"
fi
"$VENV/bin/pip" install --upgrade pip -q
"$VENV/bin/pip" install -r requirements.txt -q
echo "    Dependencies installed."

echo "==> [4/7] Setting up log directory..."
mkdir -p "$LOG_DIR"
echo "    Log dir: $LOG_DIR"

echo "==> [5/7] Installing systemd services..."
cp deploy/amo2gsheet.service /etc/systemd/system/amo2gsheet.service

# Only install cloudflared service if token is configured in it
if grep -q "YOUR_TUNNEL_TOKEN_HERE" deploy/cloudflared.service; then
    echo "    [WARN] Cloudflare tunnel token not set in deploy/cloudflared.service — skipping tunnel service."
    echo "    Set your token then run: cp deploy/cloudflared.service /etc/systemd/system/ && systemctl enable --now cloudflared"
else
    cp deploy/cloudflared.service /etc/systemd/system/cloudflared.service
    systemctl daemon-reload
    systemctl enable cloudflared
    systemctl restart cloudflared
    echo "    Cloudflare tunnel service enabled and started."
fi

echo "==> [6/7] Installing logrotate config..."
cp deploy/amo2gsheet.logrotate /etc/logrotate.d/amo2gsheet
echo "    Logrotate config installed."

echo "==> [7/7] Enabling and starting amo2gsheet service..."
systemctl daemon-reload
systemctl enable "$SERVICE_NAME"
systemctl restart "$SERVICE_NAME"
sleep 3
systemctl status "$SERVICE_NAME" --no-pager

echo ""
echo "======================================================"
echo "  Deployment complete!"
echo "  Live logs:   journalctl -u amo2gsheet -f"
echo "  Status:      systemctl status amo2gsheet"
echo "  Restart:     systemctl restart amo2gsheet"
echo "  Health:      curl http://localhost:8000/health"
echo "======================================================"
