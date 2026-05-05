#!/usr/bin/env bash
# Bare-metal crawler launcher with broker-based discovery/metadata cooperation
set -e

WORKDIR="/opt/cherry-crawler"
PID_DIR="$WORKDIR/pids"
LOG_DIR="$WORKDIR/logs"
GO_BIN=/usr/local/go/bin/go
mkdir -p "$PID_DIR" "$LOG_DIR"

NODES_DISCOVERY=4
NODES_METADATA=4
START_PORT=20003
MAIN_API="${MAIN_API_URL:-http://CHANGE-ME}"
PREFIX="${INSTANCE_PREFIX:-bare}"
BROKER_ADDR="127.0.0.1:9800"
BROKER_URL="http://${BROKER_ADDR}"

COMMON_ENV=(
    "CHERRY_PICKER_DEDUPE_PEER_TTL=10m"
    "CHERRY_PICKER_DEDUPE_METADATA_TTL=30m"
    "CHERRY_PICKER_DHT_MODE=crawl"
    "CHERRY_PICKER_DHT_PACKET_JOBS=1024"
    "CHERRY_PICKER_DHT_MAX_NODES=10000"
    "CHERRY_PICKER_EXPORTER_TIMEOUT=60s"
)

# Discovery: DHT sniffing only, push peers to broker
# Metadata: poll broker for peers, download and report to main API

build() {
    echo "Building..."
    local src_dir
    src_dir="$(cd "$(dirname "$0")/../go/cherry-picker" && pwd)"
    cd "$src_dir"
    $GO_BIN build -buildvcs=false -o "$WORKDIR/cherry-picker" ./cmd/cherry-picker
    $GO_BIN build -buildvcs=false -o "$WORKDIR/broker" ./cmd/broker
    echo "Built cherry-picker + broker"
}

launch_broker() {
    local pidfile="$PID_DIR/broker.pid"
    if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
        echo "Broker already running (PID $(cat "$pidfile"))"
        return
    fi
    BROKER_ADDR="$BROKER_ADDR" nohup "$WORKDIR/broker" >> "$LOG_DIR/broker.log" 2>&1 &
    echo $! > "$pidfile"
    echo "Broker started on $BROKER_ADDR (PID $!)"
}

launch() {
    local role=$1 idx=$2 port=$3
    local id="${PREFIX}-${role}-${idx}"
    local pidfile="$PID_DIR/${role}-${idx}.pid"
    local logfile="$LOG_DIR/${role}-${idx}.log"

    if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
        echo "  ${role}-${idx} already running (PID $(cat "$pidfile"))"
        return
    fi

    local -a envs=("${COMMON_ENV[@]}")
    envs+=("CHERRY_PICKER_ROLE=$role")
    envs+=("CHERRY_PICKER_INSTANCE_ID=$id")
    envs+=("CHERRY_PICKER_LISTEN_ADDR=:$port")
    envs+=("CHERRY_PICKER_BROKER_URL=$BROKER_URL")

    if [ "$role" = "discovery" ]; then
        envs+=("CHERRY_PICKER_EMIT_PEER_EVENTS=true")
        envs+=("CHERRY_PICKER_DHT_PACKET_WORKERS=128")
        envs+=("CHERRY_PICKER_DHT_REFRESH_NODES=256")
        envs+=("CHERRY_PICKER_EVENT_QUEUE=4096")
        envs+=("CHERRY_PICKER_EXPORTER=stdout")
    else
        envs+=("CHERRY_PICKER_EMIT_PEER_EVENTS=false")
        envs+=("CHERRY_PICKER_DHT_PACKET_WORKERS=32")
        envs+=("CHERRY_PICKER_DHT_REFRESH_NODES=64")
        envs+=("CHERRY_PICKER_EVENT_QUEUE=8192")
        envs+=("CHERRY_PICKER_METADATA_ENABLED=true")
        envs+=("CHERRY_PICKER_METADATA_BLACKLIST=65536")
        envs+=("CHERRY_PICKER_METADATA_REQUEST_QUEUE=4096")
        envs+=("CHERRY_PICKER_METADATA_WORKERS=64")
        envs+=("CHERRY_PICKER_EXPORTER=http")
        envs+=("CHERRY_PICKER_EXPORTER_URL=$MAIN_API/api/v1/torrents/batch")
        envs+=("CHERRY_PICKER_EXPORTER_BATCH=32")
        envs+=("CHERRY_PICKER_EXPORTER_FLUSH=3s")
    fi

    nohup env "${envs[@]}" "$WORKDIR/cherry-picker" >> "$logfile" 2>&1 &
    echo $! > "$pidfile"
    echo "  Started ${role}-${idx} ($id) on UDP :$port (PID $!)"
}

stop_one() {
    local name=$1
    local pidfile="$PID_DIR/${name}.pid"
    if [ -f "$pidfile" ]; then
        local pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" && echo "  Stopped $name (PID $pid)"
        fi
        rm -f "$pidfile"
    fi
}

start() {
    build
    launch_broker
    sleep 1
    local port
    echo "=== Discovery nodes ($NODES_DISCOVERY, UDP :${START_PORT}-$((START_PORT+NODES_DISCOVERY-1))) ==="
    for i in $(seq 1 $NODES_DISCOVERY); do
        port=$((START_PORT + i - 1))
        launch discovery "$i" "$port"
    done
    echo "=== Metadata nodes ($NODES_METADATA, UDP :$((START_PORT+NODES_DISCOVERY))-$((START_PORT+NODES_DISCOVERY+NODES_METADATA-1))) ==="
    for i in $(seq 1 $NODES_METADATA); do
        port=$((START_PORT + NODES_DISCOVERY + i - 1))
        launch metadata "$i" "$port"
    done
}

stop() {
    echo "Stopping..."
    stop_one broker
    for i in $(seq 1 $NODES_DISCOVERY); do stop_one "discovery-${i}"; done
    for i in $(seq 1 $NODES_METADATA); do stop_one "metadata-${i}"; done
}

status() {
    echo "=== Broker ==="
    local pf="$PID_DIR/broker.pid"
    if [ -f "$pf" ] && kill -0 "$(cat "$pf")" 2>/dev/null; then
        echo "  broker : RUNNING (PID $(cat "$pf")) — $BROKER_ADDR"
        curl -sf "$BROKER_URL/stats" 2>/dev/null | python3 -m json.tool 2>/dev/null || true
    else
        echo "  broker : STOPPED"
    fi
    echo ""
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
    echo "=== Recent logs ==="
    for i in $(seq 1 $NODES_DISCOVERY); do
        local lf="$LOG_DIR/discovery-${i}.log"
        [ -f "$lf" ] && echo "--- discovery-${i} ---" && tail -4 "$lf"
    done
    for i in $(seq 1 $NODES_METADATA); do
        local lf="$LOG_DIR/metadata-${i}.log"
        [ -f "$lf" ] && echo "--- metadata-${i} ---" && tail -4 "$lf"
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
