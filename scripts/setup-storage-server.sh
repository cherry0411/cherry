#!/usr/bin/env bash
set -euo pipefail

# Bootstrap the dedicated Cherry storage/search host. The script intentionally
# exposes only SSH. API traffic from crawler hosts is carried by persistent SSH
# local forwards configured after the storage host's SSH key is known.

REPO_DIR="${REPO_DIR:-/opt/cherry}"
DATA_ROOT="${CHERRY_DATA_ROOT:-/srv/cherry}"
ENV_FILE="${REPO_DIR}/deploy/storage/.env"

if [[ "${EUID}" -ne 0 ]]; then
  echo "must run as root" >&2
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends docker.io docker-compose-v2 openssl ufw ca-certificates curl
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
  "${DATA_ROOT}/api" "${DATA_ROOT}/backups"

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
  cat >"${ENV_FILE}" <<EOF
CHERRY_DATA_ROOT=${DATA_ROOT}
CHERRY_API_PORT=5070
POSTGRES_DB=cherry
POSTGRES_USER=cherry
POSTGRES_PASSWORD=${pg_password}
CHERRY_API_KEY=${api_key}
MEILI_MASTER_KEY=${meili_key}
MEILI_OUTBOX_BATCH_SIZE=500
MEILI_OUTBOX_LEASE_SECONDS=300
MEILI_TASK_POLL_MS=250
MEILI_TASK_TIMEOUT_SECONDS=120
MEILI_OUTBOX_IDLE_MS=2000
CHERRY_DEDUP_CAPACITY=100000000
EOF
  chmod 0600 "${ENV_FILE}"
fi

cd "${REPO_DIR}"
docker compose --env-file "${ENV_FILE}" -f deploy/storage/compose.yml config --quiet
docker compose --env-file "${ENV_FILE}" -f deploy/storage/compose.yml build api
docker compose --env-file "${ENV_FILE}" -f deploy/storage/compose.yml up -d

echo "storage stack started; API remains bound to 127.0.0.1:5070"
echo "secrets: ${ENV_FILE} (0600)"
echo "data: ${DATA_ROOT}"
echo "next: install each crawler tunnel key with permitopen=127.0.0.1:5070"
