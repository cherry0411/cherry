#!/usr/bin/env bash
# Cherry one-command setup for fresh Ubuntu/Debian server
# Run: bash setup.sh
set -e

echo "=== Cherry Setup ==="

# 1. Install Docker
if ! command -v docker &>/dev/null; then
    echo "Installing Docker..."
    curl -fsSL https://get.docker.com | bash
fi

# 2. Start services
echo "Starting services..."
docker compose up -d --build

# 3. Daily backup cron (3 AM)
BACKUP_CRON="0 3 * * * /opt/cherry/scripts/backup.sh"
if ! crontab -l 2>/dev/null | grep -q "backup.sh"; then
    (crontab -l 2>/dev/null; echo "$BACKUP_CRON") | crontab -
    echo "Daily backup scheduled (3 AM)"
fi

# 4. Create git repo for version tracking
if [ ! -d .git ]; then
    git init
    git add -A
    git commit -m "Initial commit" 2>/dev/null || true
fi

echo ""
echo "=== Done ==="
echo "Frontend : http://$(curl -s ifconfig.me 2>/dev/null || echo 'YOUR_IP')"
echo ""
echo "Health   : ./scripts/healthcheck.sh"
echo "Backup   : ./scripts/backup.sh"
echo "Restore  : ./scripts/restore.sh <file>"
echo "Mock data: docker compose exec -T postgres psql -U cherry -d cherry < backend/mock_data.sql"
echo "Update   : git pull && docker compose up -d --build"
echo ""
echo "GitHub Actions CI/CD:"
echo "  Set 3 secrets in repo Settings → Secrets:"
echo "    SERVER_HOST = your-server-ip"
echo "    SERVER_USER = root"
echo "    SERVER_SSH_KEY = (cat ~/.ssh/id_ed25519)"
