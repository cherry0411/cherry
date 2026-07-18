#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=storage-backup-common.sh
source /usr/local/sbin/storage-backup-common.sh
load_cherry_backup_config
check_cherry_backup_remote

DATA_ROOT="${CHERRY_DATA_ROOT:-/srv/cherry}"
POSTGRES_IMAGE='postgres:17.10-alpine@sha256:742f40ea20b9ff2ff31db5458d127452988a2164df9e17441e191f3b72252193'
exec 9>/run/lock/cherry-storage-backup-verify.lock
flock -n 9 || exit 0

latest_id="$("${CHERRY_RCLONE[@]}" cat "${CHERRY_BACKUP_REMOTE_ROOT}/base/LATEST")"
[[ "${latest_id}" =~ ^[A-Za-z0-9._-]+$ ]] || { echo "invalid remote LATEST" >&2; exit 1; }
drill="${DATA_ROOT}/backups/drill/${latest_id}-$$"
trap 'rm -rf -- "${drill}"' EXIT
install -d -m 0700 "${drill}/download" "${drill}/postgres"

"${CHERRY_RCLONE[@]}" copy "${CHERRY_BACKUP_REMOTE_ROOT}/base/${latest_id}" "${drill}/download" \
  --transfers 1 --checkers 2 --contimeout 15s --timeout 10m
(
  cd "${drill}/download"
  sha256sum -c SHA256SUMS
  tar --zstd -tf heat.tar.zst >/dev/null
  tar --zstd -tf secrets.tar.zst >/dev/null
)
tar --zstd -C "${drill}/postgres" -xpf "${drill}/download/postgres.tar.zst"
docker run --rm --network none --entrypoint pg_verifybackup \
  -v "${drill}/postgres:/restore:ro" "${POSTGRES_IMAGE}" /restore

latest_wal="$("${CHERRY_RCLONE[@]}" lsf "${CHERRY_BACKUP_REMOTE_ROOT}/wal" \
  --files-only --include '*.gz' | grep -E '^[A-Za-z0-9._-]+\.gz$' | sort | tail -n 1)"
[[ -n "${latest_wal}" ]] || { echo "no archived WAL found for restore drill" >&2; exit 1; }
install -d -m 0700 "${drill}/wal"
"${CHERRY_RCLONE[@]}" copyto "${CHERRY_BACKUP_REMOTE_ROOT}/wal/${latest_wal}" \
  "${drill}/wal/${latest_wal}"
"${CHERRY_RCLONE[@]}" copyto "${CHERRY_BACKUP_REMOTE_ROOT}/wal/${latest_wal}.sha256" \
  "${drill}/wal/${latest_wal}.sha256"
(cd "${drill}/wal" && sha256sum -c "${latest_wal}.sha256" && gzip -t "${latest_wal}")

report="${drill}/restore-drill-${latest_id}-$(date -u +%Y%m%dT%H%M%SZ).txt"
cat >"${report}" <<EOF
format=cherry-storage-restore-drill-v1
backup_id=${latest_id}
verified_at=$(date -u +%FT%TZ)
host_id=${CHERRY_BACKUP_HOST_ID}
result=sha256+archive-read+pg_verifybackup+wal-sidecar-ok
EOF
"${CHERRY_RCLONE[@]}" copyto "${report}" \
  "${CHERRY_BACKUP_REMOTE_ROOT}/drills/$(basename "${report}")" --immutable
echo "restore drill passed: ${latest_id}"
