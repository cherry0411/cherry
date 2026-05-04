#!/usr/bin/env bash
# Restore from backup: ./scripts/restore.sh backups/cherry_20260504_030000.sql.gz
set -e
if [ -z "$1" ]; then echo "Usage: ./scripts/restore.sh <backup-file>"; exit 1; fi
cd /opt/cherry
gunzip -c "$1" | docker compose exec -T postgres psql -U cherry -d cherry
echo "Restored from $1"
