#!/usr/bin/env bash
# Health check — run on server: ./scripts/healthcheck.sh
set -e
echo "=== Cherry Health $(date) ==="

# PostgreSQL
if docker compose exec -T postgres pg_isready -U cherry -d cherry &>/dev/null; then
    echo "  postgres : OK"
else
    echo "  postgres : DOWN"
fi

# API
if curl -sf http://localhost:5070/health &>/dev/null; then
    echo "  api      : OK"
else
    echo "  api      : DOWN"
fi

# Frontend (nginx)
if curl -sf http://localhost:80 &>/dev/null; then
    echo "  frontend : OK"
else
    echo "  frontend : DOWN"
fi

# Crawler (check if process running)
if docker compose ps crawler | grep -q "Up"; then
    echo "  crawler  : OK"
else
    echo "  crawler  : DOWN"
fi

# Disk
DISK=$(df -h / | tail -1 | awk '{print $5}')
echo "  disk     : $DISK"

# DB row count
ROWS=$(docker compose exec -T postgres psql -U cherry -d cherry -t -c "SELECT reltuples::bigint FROM pg_class WHERE relname='torrents'" 2>/dev/null | tr -d ' ')
echo "  torrents : ${ROWS:-?}"
