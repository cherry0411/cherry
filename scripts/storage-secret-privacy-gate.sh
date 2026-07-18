#!/usr/bin/env bash
set -euo pipefail

# Validate a secrets.tar listing from stdin. Recovery needs the storage .env
# and off-host backup policy only. Crawler actor masters, raw heat keys, rolling
# actor state, and arbitrary /etc files are forbidden even inside crypt remote.

seen_env=0
seen_backup=0
while IFS= read -r entry; do
  entry="${entry%$'\r'}"
  entry="${entry#./}"
  case "${entry}" in
    .env) seen_env=1 ;;
    cherry-backup.env) seen_backup=1 ;;
    "") ;;
    *)
      echo "privacy gate failed: forbidden secret archive entry: ${entry}" >&2
      exit 1
      ;;
  esac
done
if [[ "${seen_env}" -ne 1 || "${seen_backup}" -ne 1 ]]; then
  echo "privacy gate failed: recovery secret allowlist is incomplete" >&2
  exit 1
fi
