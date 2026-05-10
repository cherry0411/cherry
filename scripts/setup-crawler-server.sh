#!/usr/bin/env bash
# Cherry Crawler Server Setup
# Usage: bash setup-crawler-server.sh <MAIN_API_URL> [INSTANCE_PREFIX] [NODE_COUNT]
# Example: bash setup-crawler-server.sh http://172.17.14.158 c2 8
set -e

MAIN_API="${1:?Usage: $0 <MAIN_API_URL> [INSTANCE_PREFIX] [NODE_COUNT]}"
PREFIX="${2:-c2}"
NODES="${3:-8}"

echo "=== Cherry Crawler Server Setup ==="
echo "API:  $MAIN_API"
echo "Name: $PREFIX"
echo "Nodes: $NODES"
echo ""

# ---- 1. Install Go ----
if ! command -v go &>/dev/null; then
    echo "Installing Go..."
    GO_VER=$(curl -s https://go.dev/VERSION?m=text 2>/dev/null | head -1 || echo "go1.25.0")
    wget -q "https://go.dev/dl/${GO_VER}.linux-amd64.tar.gz" -O /tmp/go.tar.gz
    sudo tar -C /usr/local -xzf /tmp/go.tar.gz
    echo 'export PATH=$PATH:/usr/local/go/bin' | sudo tee /etc/profile.d/go.sh > /dev/null
    export PATH=$PATH:/usr/local/go/bin
    rm /tmp/go.tar.gz
    echo "Go installed: $(go version)"
fi

# ---- 2. Clone repo ----
WORKDIR="/opt/cherry-crawler"
if [ ! -d "$WORKDIR" ]; then
    echo "Cloning repo..."
    sudo mkdir -p "$WORKDIR"
    sudo chown "$USER:$USER" "$WORKDIR"
    git clone https://github.com/cherry0411/cherry.git "$WORKDIR"
else
    echo "Repo exists, updating..."
    cd "$WORKDIR" && git pull
fi

# ---- 3. Sysctl tuning ----
echo "Applying sysctl..."
sudo tee /etc/sysctl.d/99-cherry-crawler.conf > /dev/null <<'SYSCTL'
# Conntrack: prevent DHT UDP table overflow
net.netfilter.nf_conntrack_max = 524288
net.netfilter.nf_conntrack_udp_timeout = 30

# Socket buffers: 8MB recv, 4MB send
net.core.rmem_max = 8388608
net.core.wmem_max = 4194304
net.core.rmem_default = 262144
net.core.wmem_default = 262144

# File descriptors
fs.file-max = 655360
fs.nr_open = 655360

# UDP tuning
net.ipv4.udp_mem = 8388608 12582912 16777216
SYSCTL
sudo sysctl -p /etc/sysctl.d/99-cherry-crawler.conf

# ---- 4. File descriptor limits ----
echo "Setting file limits..."
sudo tee /etc/security/limits.d/99-cherry-crawler.conf > /dev/null <<'LIMIT'
* soft nofile 655360
* hard nofile 655360
root soft nofile 655360
root hard nofile 655360
LIMIT
ulimit -n 655360 2>/dev/null || true

# ---- 5. Firewall ----
echo "Opening UDP ports..."
if command -v ufw &>/dev/null; then
    sudo ufw allow 20003:$((20003 + NODES - 1))/udp
    echo "ufw: opened UDP 20003-$((20003 + NODES - 1))"
fi

# ---- 6. Start crawlers ----
echo ""
echo "=== Starting $NODES crawlers ==="
cd "$WORKDIR"
export MAIN_API_URL="$MAIN_API"
export INSTANCE_PREFIX="$PREFIX"
bash scripts/crawler-baremetal.sh start

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Health check:  curl $MAIN_API/health"
echo "Status:        cd $WORKDIR && bash scripts/crawler-baremetal.sh status"
echo "Logs:          cd $WORKDIR && bash scripts/crawler-baremetal.sh logs 1"
echo ""
echo "Cloud: open UDP 20003-$((20003 + NODES - 1)) in security group!"
