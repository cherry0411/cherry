#!/usr/bin/env bash
set -euo pipefail

# Resource-aware Linux tuning for Cherry's native crawler. Defaults target a
# 2 vCPU / 4 GiB host with 96 UDP listeners; every setting is bounded and the
# script is idempotent.
#
#   sudo bash scripts/tune-crawler-os.sh
#
# Optional overrides:
#   NOFILE_LIMIT=65536 DHT_PORT_RANGE=21000:21095 ENABLE_NOTRACK=auto

SYSCTL_FILE="/etc/sysctl.d/99-cherry-crawler.conf"
LIMITS_FILE="/etc/security/limits.d/99-cherry-crawler.conf"

NOFILE_LIMIT="${NOFILE_LIMIT:-65536}"
NPROC_LIMIT="${NPROC_LIMIT:-32768}"
DHT_PORT_RANGE="${DHT_PORT_RANGE:-21000:21095}"
ENABLE_NOTRACK="${ENABLE_NOTRACK:-auto}"
TXQUEUELEN="${TXQUEUELEN:-4096}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "must run as root" >&2
  exit 1
fi

sysctl_line_if_present() {
  local key="$1" value="$2"
  local path="/proc/sys/${key//./\/}"
  if [[ -e "${path}" ]]; then
    printf '%s = %s\n' "${key}" "${value}" >> "${SYSCTL_FILE}"
  fi
}

write_sysctl() {
  cat > "${SYSCTL_FILE}" <<'EOF'
# Cherry crawler tuning for 2 vCPU / 4 GiB.
# Keep per-socket buffers modest: 96 listeners multiply every socket limit.
net.core.rmem_max = 1048576
net.core.wmem_max = 524288
net.core.rmem_default = 262144
net.core.wmem_default = 262144
net.core.optmem_max = 131072
net.core.netdev_max_backlog = 16384

# Metadata downloads create many short-lived outbound TCP connections.
net.ipv4.ip_local_port_range = 10000 65535
net.ipv4.tcp_moderate_rcvbuf = 1

# Enough descriptor headroom without oversized global tables.
fs.file-max = 262144
fs.nr_open = 1048576

# Prefer dropping Go heap pages before swapping under transient pressure.
vm.swappiness = 10
EOF

  # Some kernels do not expose these knobs. Only persist keys present on this
  # host so sysctl --system remains clean across distributions.
  sysctl_line_if_present net.ipv4.udp_rmem_min 8192
  sysctl_line_if_present net.ipv4.udp_wmem_min 8192
}

write_limits() {
  cat > "${LIMITS_FILE}" <<EOF
* soft nofile ${NOFILE_LIMIT}
* hard nofile ${NOFILE_LIMIT}
* soft nproc ${NPROC_LIMIT}
* hard nproc ${NPROC_LIMIT}
root soft nofile ${NOFILE_LIMIT}
root hard nofile ${NOFILE_LIMIT}
EOF
}

configure_notrack() {
  local enabled="${ENABLE_NOTRACK}"
  if [[ "${enabled}" == "auto" ]]; then
    # Do not load conntrack just to bypass it. If the host is not tracking
    # connections already, there is no table pressure to optimize away.
    if [[ -e /proc/sys/net/netfilter/nf_conntrack_count || -e /proc/net/nf_conntrack ]]; then
      enabled=true
    else
      enabled=false
    fi
  fi
  [[ "${enabled}" == "true" ]] || return 0
  command -v iptables >/dev/null 2>&1 || return 0

  # Only local inbound DHT and locally sourced DHT replies/queries need bypass.
  # Avoid the four broad/redundant rules used by the legacy script.
  iptables -t raw -C PREROUTING -p udp --dport "${DHT_PORT_RANGE}" -j NOTRACK 2>/dev/null ||
    iptables -t raw -A PREROUTING -p udp --dport "${DHT_PORT_RANGE}" -j NOTRACK
  iptables -t raw -C OUTPUT -p udp --sport "${DHT_PORT_RANGE}" -j NOTRACK 2>/dev/null ||
    iptables -t raw -A OUTPUT -p udp --sport "${DHT_PORT_RANGE}" -j NOTRACK
}

tune_nics() {
  local cpu_count
  cpu_count="$(nproc)"

  for dev_path in /sys/class/net/*; do
    local dev rx_queues mask
    dev="$(basename "${dev_path}")"
    [[ "${dev}" == "lo" ]] && continue
    [[ -e "${dev_path}/operstate" ]] || continue

    ip link set dev "${dev}" txqueuelen "${TXQUEUELEN}" 2>/dev/null || true
    rx_queues="$(find "${dev_path}/queues" -maxdepth 1 -name 'rx-*' 2>/dev/null | wc -l)"

    # A multi-queue NIC already distributes work in hardware. RPS on top of at
    # least one RX queue per CPU adds cross-CPU cache traffic on small hosts.
    if (( rx_queues > 0 && rx_queues < cpu_count )); then
      if (( cpu_count >= 32 )); then
        mask=ffffffff
      else
        mask="$(printf '%x' "$(( (1 << cpu_count) - 1 ))")"
      fi
      for rps in "${dev_path}"/queues/rx-*/rps_cpus; do
        [[ -e "${rps}" ]] && echo "${mask}" > "${rps}" 2>/dev/null || true
      done
    fi
  done
}

main() {
  write_sysctl
  write_limits
  sysctl --system >/dev/null
  configure_notrack
  tune_nics

  echo "Cherry crawler OS tuning applied"
  echo "  sysctl: ${SYSCTL_FILE}"
  echo "  limits: ${LIMITS_FILE}"
  echo "  DHT ports: ${DHT_PORT_RANGE}"
  echo "Start the crawler from a new login/session, or set LimitNOFILE=${NOFILE_LIMIT} in its systemd unit."
}

main "$@"
