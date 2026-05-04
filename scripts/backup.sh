#!/usr/bin/env bash
# Database backup — runs on server via cron: 0 3 * * * /opt/cherry/scripts/backup.sh
set -e
cd /opt/cherry
BACKUP_DIR="./backups"
mkdir -p "$BACKUP_DIR"
FILE="$BACKUP_DIR/cherry_$(date +%Y%m%d_%H%M%S).sql.gz"
docker compose exec -T postgres pg_dump -U cherry -d cherry | gzip > "$FILE"
find "$BACKUP_DIR" -name "*.sql.gz" -mtime +7 -delete
echo "Backup: $FILE ($(du -h "$FILE" | cut -f1))"
