#!/usr/bin/env bash
set -euo pipefail

# Bootstrap the dedicated Cherry storage/search host. The script intentionally
# exposes only SSH. API traffic from crawler hosts is carried by persistent SSH
# local forwards configured after the storage host's SSH key is known.

REPO_DIR="${REPO_DIR:-/opt/cherry}"
DATA_ROOT="${CHERRY_DATA_ROOT:-/srv/cherry}"
ENV_FILE="${REPO_DIR}/deploy/storage/.env"
SECRET_DIR="/etc/cherry-secrets"

if [[ "${EUID}" -ne 0 ]]; then
  echo "must run as root" >&2
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends docker.io docker-compose-v2 openssl ufw ca-certificates curl gzip jq rclone tar util-linux zstd
systemctl enable --now docker

# A no-login account is reserved for crawler local forwards. Public keys are
# added later with permitopen="127.0.0.1:5070" after each crawler generates a
# dedicated tunnel key.
if ! id cherry-tunnel >/dev/null 2>&1; then
  useradd --create-home --shell /usr/sbin/nologin cherry-tunnel
fi
install -d -o cherry-tunnel -g cherry-tunnel -m 0700 /home/cherry-tunnel/.ssh
touch /home/cherry-tunnel/.ssh/authorized_keys
chown cherry-tunnel:cherry-tunnel /home/cherry-tunnel/.ssh/authorized_keys
chmod 0600 /home/cherry-tunnel/.ssh/authorized_keys

cat >/etc/ssh/sshd_config.d/60-cherry-tunnel.conf <<'EOF'
Match User cherry-tunnel
    AuthenticationMethods publickey
    PasswordAuthentication no
    KbdInteractiveAuthentication no
    AllowTcpForwarding local
    PermitOpen 127.0.0.1:5070
    GatewayPorts no
    X11Forwarding no
    AllowAgentForwarding no
    PermitTTY no
EOF
sshd -t
systemctl reload ssh

install -d -m 0750 "${DATA_ROOT}" "${DATA_ROOT}/postgres" "${DATA_ROOT}/meili" \
  "${DATA_ROOT}/api" "${DATA_ROOT}/api/heat" "${DATA_ROOT}/backups" \
  "${DATA_ROOT}/backups/wal" "${DATA_ROOT}/backups/staging" \
  "${DATA_ROOT}/backups/failed" "${DATA_ROOT}/restore-wal"
install -d -o root -g root -m 0700 "${SECRET_DIR}"

# Actor pseudonymization and CHHT transport authentication are independent.
# Both regions receive the same actor master so cross-region duplicates merge,
# but each receives only its own transport HMAC so it cannot impersonate the
# other region's batches or daily completion proof.
if [[ ! -e "${SECRET_DIR}/heat-actor-master" ]]; then
  umask 077
  openssl rand 32 >"${SECRET_DIR}/heat-actor-master"
fi
for transport in heat-hmac-sg heat-hmac-jp; do
  if [[ ! -e "${SECRET_DIR}/${transport}" ]]; then
    umask 077
    openssl rand 32 >"${SECRET_DIR}/${transport}"
  fi
done
chmod 0600 "${SECRET_DIR}/heat-actor-master" \
  "${SECRET_DIR}/heat-hmac-sg" "${SECRET_DIR}/heat-hmac-jp"
if [[ "$(wc -c <"${SECRET_DIR}/heat-actor-master")" -lt 32 ||
      "$(wc -c <"${SECRET_DIR}/heat-hmac-sg")" -lt 32 ||
      "$(wc -c <"${SECRET_DIR}/heat-hmac-jp")" -lt 32 ]]; then
  echo "heat secret files must each contain at least 32 raw bytes" >&2
  exit 1
fi

cat >/etc/sysctl.d/99-cherry-storage.conf <<'EOF'
# Preserve page cache for PostgreSQL/Meili and avoid swap-driven latency.
vm.swappiness = 1
vm.dirty_background_ratio = 5
vm.dirty_ratio = 15
fs.file-max = 524288
EOF
sysctl --system >/dev/null

# The provider firewall is not an application security boundary. Keep every
# storage/search port closed until an explicit HTTPS or private-overlay design
# is deployed and measured.
ufw default deny incoming
ufw default allow outgoing
ufw allow OpenSSH
ufw --force enable

if [[ ! -d "${REPO_DIR}/.git" ]]; then
  echo "repository must already be cloned at ${REPO_DIR}" >&2
  exit 1
fi

if [[ ! -e "${ENV_FILE}" ]]; then
  umask 077
  pg_password="$(openssl rand -base64 36 | tr -d '\n')"
  api_key="$(openssl rand -hex 32)"
  meili_key="$(openssl rand -hex 32)"
  heat_sg_secret_base64="$(openssl base64 -A <"${SECRET_DIR}/heat-hmac-sg")"
  heat_jp_secret_base64="$(openssl base64 -A <"${SECRET_DIR}/heat-hmac-jp")"
  cat >"${ENV_FILE}" <<EOF
CHERRY_DATA_ROOT=${DATA_ROOT}
CHERRY_API_PORT=5070
POSTGRES_DB=cherry
POSTGRES_USER=cherry
POSTGRES_PASSWORD=${pg_password}
CHERRY_API_KEY=${api_key}
MEILI_MASTER_KEY=${meili_key}
CHERRY_HEAT_CRAWLER_0_SECRET_BASE64=${heat_sg_secret_base64}
CHERRY_HEAT_CRAWLER_1_SECRET_BASE64=${heat_jp_secret_base64}
CHERRY_HEAT_COVERAGE_START_DAY=$(date -u +%F)
CHERRY_HEAT_CRAWLER_0=sg-crawler-01
CHERRY_HEAT_CRAWLER_1=jp-crawler-01
CHERRY_HEAT_LATE_GRACE_MINUTES=30
MEILI_OUTBOX_BATCH_SIZE=500
MEILI_OUTBOX_LEASE_SECONDS=300
MEILI_TASK_POLL_MS=250
MEILI_TASK_TIMEOUT_SECONDS=120
MEILI_OUTBOX_IDLE_MS=2000
CHERRY_DEDUP_CAPACITY=30000000
CHERRY_HEAT_COMMIT_BATCH_REQUESTS=8
CHERRY_HEAT_PROJECTION_BATCH_SIZE=500
EOF
  chmod 0600 "${ENV_FILE}"
fi

for required in CHERRY_HEAT_CRAWLER_0_SECRET_BASE64 CHERRY_HEAT_CRAWLER_1_SECRET_BASE64; do
  if ! grep -q "^${required}=" "${ENV_FILE}"; then
    echo "existing ${ENV_FILE} predates per-crawler heat keys; add ${required} and redistribute the matching raw transport secret" >&2
    exit 1
  fi
done
configured_sg="$(sed -n 's/^CHERRY_HEAT_CRAWLER_0_SECRET_BASE64=//p' "${ENV_FILE}" | tail -n 1)"
configured_jp="$(sed -n 's/^CHERRY_HEAT_CRAWLER_1_SECRET_BASE64=//p' "${ENV_FILE}" | tail -n 1)"
if [[ "${configured_sg}" != "$(openssl base64 -A <"${SECRET_DIR}/heat-hmac-sg")" ||
      "${configured_jp}" != "$(openssl base64 -A <"${SECRET_DIR}/heat-hmac-jp")" ]]; then
  echo "heat transport key files do not match ${ENV_FILE}; refuse an ambiguous deployment" >&2
  exit 1
fi

cd "${REPO_DIR}"
docker compose --env-file "${ENV_FILE}" -f deploy/storage/compose.yml config --quiet
docker compose --env-file "${ENV_FILE}" -f deploy/storage/compose.yml build api
docker compose --env-file "${ENV_FILE}" -f deploy/storage/compose.yml up -d
docker compose --env-file "${ENV_FILE}" -f deploy/storage/compose.yml exec -T -u root postgres \
  chown -R postgres:postgres /var/lib/postgresql/wal-archive

# An off-host rclone crypt destination is a durability gate, not an optional
# follow-up. The first run deliberately stops here with the API stopped until
# the operator configures the provider-specific rclone remote.
BACKUP_ENV=/etc/cherry-backup.env
RCLONE_CONFIG=/etc/rclone/cherry.conf
install -d -o root -g root -m 0700 /etc/rclone /var/lib/cherry-backup
if [[ ! -e "${BACKUP_ENV}" ]]; then
  install -o root -g root -m 0600 deploy/storage/backup.env.example "${BACKUP_ENV}"
fi
install -o root -g root -m 0755 scripts/storage-backup-common.sh /usr/local/sbin/storage-backup-common.sh
install -o root -g root -m 0755 scripts/backup-storage.sh /usr/local/sbin/cherry-storage-backup
install -o root -g root -m 0755 scripts/upload-storage-wal.sh /usr/local/sbin/cherry-storage-wal-upload
install -o root -g root -m 0755 scripts/verify-storage-backup.sh /usr/local/sbin/cherry-storage-backup-verify
install -o root -g root -m 0644 deploy/storage/cherry-storage-backup.service /etc/systemd/system/
install -o root -g root -m 0644 deploy/storage/cherry-storage-backup.timer /etc/systemd/system/
install -o root -g root -m 0644 deploy/storage/cherry-storage-wal-upload.service /etc/systemd/system/
install -o root -g root -m 0644 deploy/storage/cherry-storage-wal-upload.timer /etc/systemd/system/
install -o root -g root -m 0644 deploy/storage/cherry-storage-backup-verify.service /etc/systemd/system/
install -o root -g root -m 0644 deploy/storage/cherry-storage-backup-verify.timer /etc/systemd/system/
systemctl daemon-reload

if ! /usr/local/sbin/cherry-storage-backup --gate; then
  docker compose --env-file "${ENV_FILE}" -f deploy/storage/compose.yml stop -t 30 api || true
  echo "off-host backup gate failed; API was stopped to prevent authoritative ingest" >&2
  echo "configure ${BACKUP_ENV} and ${RCLONE_CONFIG}, then rerun this script" >&2
  exit 1
fi
/usr/local/sbin/cherry-storage-wal-upload
systemctl enable --now cherry-storage-backup.timer \
  cherry-storage-wal-upload.timer cherry-storage-backup-verify.timer

echo "storage stack started; API remains bound to 127.0.0.1:5070"
echo "secrets: ${ENV_FILE} (0600)"
echo "crawler-only raw heat secrets: ${SECRET_DIR} (root/0600)"
echo "data: ${DATA_ROOT}"
echo "off-host backup gate: verified; timers enabled"
echo "next: install the shared actor master plus only the matching SG/JP transport key on each crawler"
