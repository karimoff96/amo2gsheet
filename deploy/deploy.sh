#!/usr/bin/env bash
# ============================================================
#  deploy.sh — Full server setup for amo2gsheet
#  Run once on a fresh Ubuntu/Debian VPS as root.
#  Usage: bash deploy.sh
# ============================================================
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
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

echo "==> [3/7] Verifying required config files..."
MISSING=0
if [ ! -f "$PROJECT_DIR/.env" ]; then
    echo "    [ERROR] .env not found at $PROJECT_DIR/.env"
    echo "    Upload it: scp .env root@SERVER_IP:$PROJECT_DIR/.env"
    MISSING=1
fi
GSHEET_FOUND=0
for f in gsheet.json dev_gsheet.json prod_gsheet.json; do
    if [ -f "$PROJECT_DIR/$f" ]; then GSHEET_FOUND=1; break; fi
done
if [ "$GSHEET_FOUND" -eq 0 ]; then
    echo "    [ERROR] No Google service account JSON found (gsheet.json / prod_gsheet.json)"
    echo "    Upload it: scp prod_gsheet.json root@SERVER_IP:$PROJECT_DIR/prod_gsheet.json"
    MISSING=1
fi
if [ "$MISSING" -eq 1 ]; then
    echo ""
    echo "Fix the above errors and re-run deploy.sh."
    exit 1
fi
echo "    .env and gsheet key found."

echo "==> [4/7] Setting up Python virtualenv..."
cd "$PROJECT_DIR"
if [ ! -d "$VENV" ]; then
    python3 -m venv "$VENV"
fi
"$VENV/bin/pip" install --upgrade pip -q
"$VENV/bin/pip" install -r requirements.txt -q
echo "    Dependencies installed."

echo "==> [5/7] Setting up log directory and firewall..."
apt-get install -y -qq ufw
ufw allow 8000/tcp > /dev/null 2>&1 || true
ufw --force enable > /dev/null 2>&1 || true
echo "    Port 8000 opened in ufw."
mkdir -p "$LOG_DIR"
echo "    Log dir: $LOG_DIR"

echo "==> [6/7] Installing systemd services..."
# Substitute the actual project path into the service file before installing
sed "s|/root/amo2gsheet|$PROJECT_DIR|g" deploy/amo2gsheet.service \
    > /etc/systemd/system/amo2gsheet.service
# Make the start wrapper executable
chmod +x "$PROJECT_DIR/deploy/start.sh"

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

echo "==> [7/7] Installing logrotate config..."
cp deploy/amo2gsheet.logrotate /etc/logrotate.d/amo2gsheet
echo "    Logrotate config installed."

echo "==> Starting amo2gsheet service..."
systemctl daemon-reload
systemctl enable "$SERVICE_NAME"
systemctl restart "$SERVICE_NAME"
sleep 3
if systemctl is-active --quiet "$SERVICE_NAME"; then
    echo "    Service is running."
else
    echo "    [ERROR] Service failed to start. Check logs:"
    journalctl -u "$SERVICE_NAME" -n 30 --no-pager
    exit 1
fi

PUBLIC_IP=$(curl -s --max-time 5 ifconfig.me 2>/dev/null || echo "UNKNOWN")

echo ""
echo "======================================================"
echo "  Deployment complete!"
echo "  Public IP:   $PUBLIC_IP"
echo "  Webhook URL: http://$PUBLIC_IP:8000/webhook/amocrm"
echo "  Health:      curl http://localhost:8000/health"
echo "  Dashboard:   http://$PUBLIC_IP:8000/dashboard"
echo ""
echo "  Live logs:   journalctl -u amo2gsheet -f"
echo "  Status:      systemctl status amo2gsheet"
echo "  Restart:     systemctl restart amo2gsheet"
echo ""
echo "  First run — complete OAuth if tokens are missing:"
echo "    journalctl -u amo2gsheet -n 30  # find the OAuth URL"
echo "    curl -X POST http://localhost:8000/oauth/exchange \\"
echo "      -H 'Content-Type: application/json' \\"
echo "      -d '{\"redirect_url\": \"https://yoursite.uz/?code=XXXX&state=setup\"}'"
echo "    systemctl restart amo2gsheet"
echo "======================================================"
