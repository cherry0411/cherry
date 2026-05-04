#!/usr/bin/env bash
# Cherry deploy script — run on the server after git pull
set -e

cd "$(dirname "$0")"

case "${1:-all}" in
  all)
    docker compose up -d --build
    ;;

  api|backend)
    docker compose build api
    docker compose up -d --no-deps api
    ;;

  frontend|fe)
    # Frontend is static files mounted as a volume — just reload nginx
    docker compose exec frontend nginx -s reload 2>/dev/null \
      || docker compose up -d --no-deps --force-recreate frontend
    ;;

  crawler|go)
    docker compose build crawler
    docker compose up -d --no-deps crawler
    ;;

  *)
    echo "Usage: ./deploy.sh [all|backend|frontend|crawler]"
    exit 1
    ;;
esac

echo "Done."
