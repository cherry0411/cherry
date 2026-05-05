#!/usr/bin/env bash
# Bare-metal crawler launcher — discovery + metadata split
set -e

WORKDIR="/opt/cherry-crawler"
PID_DIR="$WORKDIR/pids"
LOG_DIR="$WORKDIR/logs"
GO_BIN=/usr/local/go/bin
mkdir -p "$PID_DIR" "$LOG_DIR"

NODES_DISCOVERY=4   # DHT sniffing only (lightweight)
NODES_METADATA=4    # DHT + metadata download (heavier)
START_PORT=20003
MAIN_API="${MAIN_API_URL:-http://CHANGE-ME}"
PREFIX="${INSTANCE_PREFIX:-bare}"

# ---- discovery node config ----
DISCOVERY_ENV=(
    "CHERRY_PICKER_ROLE=discovery"
    "CHERRY_PICKER_EVENT_QUEUE=4096"
    "CHERRY_PICKER_DEDUPE_PEER_TTL=10m"
    "CHERRY_PICKER_DEDUPE_METADATA_TTL=30m"
    "CHERRY_PICKER_DHT_MODE=crawl"
    "CHERRY_PICKER_EMIT_PEER_EVENTS=true"
    "CHERRY_PICKER_DHT_PACKET_WORKERS=128"
    "CHERRY_PICKER_DHT_PACKET_JOBS=1024"
    "CHERRY_PICKER_DHT_MAX_NODES=10000"
    "CHERRY_PICKER_DHT_REFRESH_NODES=256"
    "CHERRY_PICKER_EXPORTER=stdout"
)

# ---- metadata node config ----
METADATA_ENV=(
    "CHERRY_PICKER_ROLE=combined"
    "CHERRY_PICKER_EVENT_QUEUE=8192"
    "CHERRY_PICKER_DEDUPE_PEER_TTL=10m"
    "CHERRY_PICKER_DEDUPE_METADATA_TTL=30m"
    "CHERRY_PICKER_DHT_MODE=crawl"
    "CHERRY_PICKER_EMIT_PEER_EVENTS=false"
    "CHERRY_PICKER_DHT_PACKET_WORKERS=64"
    "CHERRY_PICKER_DHT_PACKET_JOBS=512"
    "CHERRY_PICKER_DHT_MAX_NODES=10000"
    "CHERRY_PICKER_DHT_REFRESH_NODES=128"
    "CHERRY_PICKER_METADATA_ENABLED=true"
    "CHERRY_PICKER_METADATA_BLACKLIST=65536"
    "CHERRY_PICKER_METADATA_REQUEST_QUEUE=4096"
    "CHERRY_PICKER_METADATA_WORKERS=64"
    "CHERRY_PICKER_EXPORTER=http"
    "CHERRY_PICKER_EXPORTER_URL=$MAIN_API/api/v1/torrents/batch"
    "CHERRY_PICKER_EXPORTER_BATCH=32"
    "CHERRY_PICKER_EXPORTER_FLUSH=3s"
    "CHERRY_PICKER_EXPORTER_TIMEOUT=60s"
    "CHERRY_PICKER_EXPORTER_HTTP_RETRIES=2"
    "CHERRY_PICKER_EXPORTER_RETRY_BACKOFF=5s"
)

build() {
    echo "Building crawler..."
    local src_dir
    src_dir="$(cd "$(dirname "$0")/../go/cherry-picker" && pwd)"
    cd "$src_dir"
    $GO_BIN build -buildvcs=false -o "$WORKDIR/cherry-picker" ./cmd/cherry-picker
    echo "Built: $WORKDIR/cherry-picker"
}

launch() {
    local role=$1 idx=$2 port=$3
    local id="${PREFIX}-${role}-${idx}"
    local pidfile="$PID_DIR/${role}-${idx}.pid"
    local logfile="$LOG_DIR/${role}-${idx}.log"
    local -n envs="${role}_ENV"

    if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
        echo "  ${role}-${idx} already running (PID $(cat "$pidfile"))"
        return
    fi

    nohup env "${envs[@]}" \
        CHERRY_PICKER_INSTANCE_ID="$id" \
        CHERRY_PICKER_LISTEN_ADDR=":$port" \
        "$WORKDIR/cherry-picker" >> "$logfile" 2>&1 &
    echo $! > "$pidfile"
    echo "  Started ${role}-${idx} ($id) UDP :$port (PID $!)"
}

stop_one() {
    local role=$1 idx=$2
    local pidfile="$PID_DIR/${role}-${idx}.pid"
    if [ -f "$pidfile" ]; then
        local pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" && echo "  Stopped ${role}-${idx} (PID $pid)"
        fi
        rm -f "$pidfile"
    fi
}

start() {
    build
    local port
    echo "=== Discovery nodes (DHT sniffing only) ==="
    for i in $(seq 1 $NODES_DISCOVERY); do
        port=$((START_PORT + i - 1))
        launch discovery "$i" "$port"
    done
    echo "=== Metadata nodes (DHT + download) ==="
    for i in $(seq 1 $NODES_METADATA); do
        port=$((START_PORT + NODES_DISCOVERY + i - 1))
        launch metadata "$i" "$port"
    done
}

stop() {
    echo "Stopping all..."
    for i in $(seq 1 $NODES_DISCOVERY); do stop_one discovery "$i"; done
    for i in $(seq 1 $NODES_METADATA); do stop_one metadata "$i"; done
}

status() {
    echo "=== Discovery ==="
    for i in $(seq 1 $NODES_DISCOVERY); do
        local pf="$PID_DIR/discovery-${i}.pid"
        if [ -f "$pf" ] && kill -0 "$(cat "$pf")" 2>/dev/null; then
            echo "  discovery-${i} : RUNNING (PID $(cat "$pf")) UDP :$((START_PORT + i - 1))"
        else echo "  discovery-${i} : STOPPED"; fi
    done
    echo "=== Metadata ==="
    for i in $(seq 1 $NODES_METADATA); do
        local pf="$PID_DIR/metadata-${i}.pid"
        local port=$((START_PORT + NODES_DISCOVERY + i - 1))
        if [ -f "$pf" ] && kill -0 "$(cat "$pf")" 2>/dev/null; then
            echo "  metadata-${i} : RUNNING (PID $(cat "$pf")) UDP :${port}"
        else echo "  metadata-${i} : STOPPED"; fi
    done
    echo ""
    echo "Discovery UDP: ${START_PORT}-$((START_PORT + NODES_DISCOVERY - 1))"
    echo "Metadata UDP:  $((START_PORT + NODES_DISCOVERY))-$((START_PORT + NODES_DISCOVERY + NODES_METADATA - 1))"
    echo ""
    echo ""
    echo "=== Recent logs ==="
    for i in $(seq 1 $NODES_DISCOVERY); do
        local lf="$LOG_DIR/discovery-${i}.log"
        if [ -f "$lf" ]; then
            echo "--- discovery-${i} ---"
            tail -8 "$lf"
        fi
    done
    for i in $(seq 1 $NODES_METADATA); do
        local lf="$LOG_DIR/metadata-${i}.log"
        if [ -f "$lf" ]; then
            echo "--- metadata-${i} ---"
            tail -8 "$lf"
        fi
    done
}

logs() { tail -f "$LOG_DIR/${1:-metadata-1}.log"; }

case "${1:-start}" in
    build)  build ;;
    start)  start ;;
    stop)   stop ;;
    restart) stop; start ;;
    status) status ;;
    logs)   logs "$2" ;;
    *)      echo "Usage: $0 {build|start|stop|restart|status|logs [name]}" ;;
esac
