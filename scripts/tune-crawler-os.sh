#!/usr/bin/env bash
set -euo pipefail

# Linux host tuning for a high-throughput DHT metadata crawler.
# Run as root on the crawler host:
#   sudo bash scripts/tune-crawler-os.sh
#
# The script writes persistent sysctl, limits, and systemd defaults, applies
# sysctl immediately, and performs best-effort NIC queue/RPS tuning.

SYSCTL_FILE="/etc/sysctl.d/99-cherry-crawler.conf"
LIMITS_FILE="/etc/security/limits.d/99-cherry-crawler.conf"
SYSTEMD_SYSTEM_DIR="/etc/systemd/system.conf.d"
SYSTEMD_USER_DIR="/etc/systemd/user.conf.d"
SYSTEMD_SYSTEM_FILE="${SYSTEMD_SYSTEM_DIR}/99-cherry-crawler.conf"
SYSTEMD_USER_FILE="${SYSTEMD_USER_DIR}/99-cherry-crawler.conf"

NOFILE_LIMIT="${NOFILE_LIMIT:-1048576}"
NPROC_LIMIT="${NPROC_LIMIT:-262144}"
TXQUEUELEN="${TXQUEUELEN:-10000}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "must run as root" >&2
  exit 1
fi

write_sysctl() {
  cat > "${SYSCTL_FILE}" <<'EOF'
# Cherry crawler high-throughput network tuning.

# Large UDP/TCP socket buffers for bursty DHT traffic.
net.core.rmem_max = 134217728
net.core.wmem_max = 134217728
net.core.rmem_default = 16777216
net.core.wmem_default = 16777216
net.core.optmem_max = 16777216

# Larger packet backlog between NIC and userspace.
net.core.netdev_max_backlog = 250000
net.core.somaxconn = 65535

# UDP memory pressure thresholds, in pages. Keep high enough for DHT bursts.
net.ipv4.udp_mem = 262144 524288 1048576
net.ipv4.udp_rmem_min = 8192
net.ipv4.udp_wmem_min = 8192

# Outbound TCP metadata fetches create many short-lived connections.
net.ipv4.ip_local_port_range = 10000 65535
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_fin_timeout = 10
net.ipv4.tcp_syn_retries = 3
net.ipv4.tcp_synack_retries = 3
net.ipv4.tcp_orphan_retries = 1
net.ipv4.tcp_max_syn_backlog = 65535
net.ipv4.tcp_max_tw_buckets = 2000000

# Avoid slow path memory growth under large numbers of sockets.
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216
net.ipv4.tcp_moderate_rcvbuf = 1

# Keep reverse-path filtering loose; DHT traffic can be asymmetric on some VPS/NAT setups.
net.ipv4.conf.all.rp_filter = 0
net.ipv4.conf.default.rp_filter = 0

# File/socket scale.
fs.file-max = 2097152
fs.nr_open = 2097152
EOF

  if [[ -e /proc/sys/net/netfilter/nf_conntrack_max ]]; then
    cat >> "${SYSCTL_FILE}" <<'EOF'

# If conntrack is enabled on the host, raise the ceiling to avoid drops.
net.netfilter.nf_conntrack_max = 1048576
EOF
  fi
}

write_limits() {
  cat > "${LIMITS_FILE}" <<EOF
* soft nofile ${NOFILE_LIMIT}
* hard nofile ${NOFILE_LIMIT}
* soft nproc ${NPROC_LIMIT}
* hard nproc ${NPROC_LIMIT}
root soft nofile ${NOFILE_LIMIT}
root hard nofile ${NOFILE_LIMIT}
root soft nproc ${NPROC_LIMIT}
root hard nproc ${NPROC_LIMIT}
EOF

  mkdir -p "${SYSTEMD_SYSTEM_DIR}" "${SYSTEMD_USER_DIR}"
  cat > "${SYSTEMD_SYSTEM_FILE}" <<EOF
[Manager]
DefaultLimitNOFILE=${NOFILE_LIMIT}
DefaultLimitNPROC=${NPROC_LIMIT}
EOF
  cat > "${SYSTEMD_USER_FILE}" <<EOF
[Manager]
DefaultLimitNOFILE=${NOFILE_LIMIT}
DefaultLimitNPROC=${NPROC_LIMIT}
EOF
}

tune_nics() {
  local cpu_count
  cpu_count="$(nproc)"
  local mask
  if (( cpu_count >= 32 )); then
    mask="ffffffff"
  else
    mask="$(printf '%x' "$(( (1 << cpu_count) - 1 ))")"
  fi

  for dev_path in /sys/class/net/*; do
    local dev
    dev="$(basename "${dev_path}")"
    [[ "${dev}" == "lo" ]] && continue
    [[ ! -e "${dev_path}/operstate" ]] && continue

    ip link set dev "${dev}" txqueuelen "${TXQUEUELEN}" 2>/dev/null || true

    for rps in "${dev_path}"/queues/rx-*/rps_cpus; do
      [[ -e "${rps}" ]] || continue
      echo "${mask}" > "${rps}" 2>/dev/null || true
    done

    for rps_flow in "${dev_path}"/queues/rx-*/rps_flow_cnt; do
      [[ -e "${rps_flow}" ]] || continue
      echo 4096 > "${rps_flow}" 2>/dev/null || true
    done

    if command -v ethtool >/dev/null 2>&1; then
      ethtool -G "${dev}" rx 4096 tx 4096 >/dev/null 2>&1 || true
      ethtool -K "${dev}" gro on gso on tso on >/dev/null 2>&1 || true
    fi
  done

  if [[ -e /proc/sys/net/core/rps_sock_flow_entries ]]; then
    echo 32768 > /proc/sys/net/core/rps_sock_flow_entries || true
  fi
}

main() {
  write_sysctl
  write_limits

  sysctl --system
  tune_nics

  if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reexec || true
  fi

  cat <<EOF

Cherry crawler OS tuning applied.

Persistent files:
  ${SYSCTL_FILE}
  ${LIMITS_FILE}
  ${SYSTEMD_SYSTEM_FILE}
  ${SYSTEMD_USER_FILE}

Recommended runtime checks:
  ulimit -n
  sysctl net.core.rmem_max net.core.netdev_max_backlog net.ipv4.ip_local_port_range
  ss -s
  cat /proc/net/sockstat

For Docker Compose, keep service ulimits at least:
  nofile soft/hard: ${NOFILE_LIMIT}

Reboot is recommended so PAM/systemd limits apply to all services.
EOF
}

main "$@"
