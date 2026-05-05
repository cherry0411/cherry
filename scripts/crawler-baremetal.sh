#!/usr/bin/env bash
# Bare-metal Go crawler — combined mode with API pre-check
set -e

WORKDIR="/opt/cherry-crawler"
PID_DIR="$WORKDIR/pids"
LOG_DIR="$WORKDIR/logs"
GO_BIN=/usr/local/go/bin/go
mkdir -p "$PID_DIR" "$LOG_DIR"

NODES=8
START_PORT=20003
MAIN_API="${MAIN_API_URL:-http://CHANGE-ME}"
PREFIX="${PREFIX:-go}"

build() {
    echo "Building..."
    local src_dir
    src_dir="$(cd "$(dirname "$0")/../go/cherry-picker" && pwd)"
    cd "$src_dir"
    $GO_BIN build -buildvcs=false -o "$WORKDIR/cherry-picker" ./cmd/cherry-picker
    echo "Built: $WORKDIR/cherry-picker"
}

launch() {
    local idx=$1 port=$2
    local id="${PREFIX}-${idx}"
    local pidfile="$PID_DIR/crawler-${idx}.pid"
    local logfile="$LOG_DIR/crawler-${idx}.log"

    if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
        echo "  crawler-${idx} already running (PID $(cat "$pidfile"))"
        return
    fi

    nohup env \
        CHERRY_PICKER_ROLE=combined \
        CHERRY_PICKER_INSTANCE_ID="$id" \
        CHERRY_PICKER_LISTEN_ADDR=":$port" \
        CHERRY_PICKER_EVENT_QUEUE=8192 \
        CHERRY_PICKER_DEDUPE_PEER_TTL=10m \
        CHERRY_PICKER_DEDUPE_METADATA_TTL=30m \
        CHERRY_PICKER_DHT_MODE=crawl \
        CHERRY_PICKER_EMIT_PEER_EVENTS=true \
        CHERRY_PICKER_DHT_PACKET_WORKERS=128 \
        CHERRY_PICKER_DHT_PACKET_JOBS=512 \
        CHERRY_PICKER_DHT_MAX_NODES=10000 \
        CHERRY_PICKER_DHT_REFRESH_NODES=256 \
        CHERRY_PICKER_METADATA_ENABLED=true \
        CHERRY_PICKER_METADATA_BLACKLIST=65536 \
        CHERRY_PICKER_METADATA_REQUEST_QUEUE=4096 \
        CHERRY_PICKER_METADATA_WORKERS=128 \
        CHERRY_PICKER_EXPORTER=http \
        CHERRY_PICKER_EXPORTER_URL="$MAIN_API/api/v1/torrents/batch" \
        CHERRY_PICKER_EXPORTER_BATCH=32 \
        CHERRY_PICKER_EXPORTER_FLUSH=3s \
        CHERRY_PICKER_EXPORTER_TIMEOUT=60s \
        CHERRY_PICKER_EXPORTER_HTTP_RETRIES=2 \
        CHERRY_PICKER_EXPORTER_RETRY_BACKOFF=5s \
        "$WORKDIR/cherry-picker" >> "$logfile" 2>&1 &
    echo $! > "$pidfile"
    echo "  Started crawler-${idx} ($id) UDP :$port (PID $!)"
}

stop_one() {
    local idx=$1
    local pidfile="$PID_DIR/crawler-${idx}.pid"
    if [ -f "$pidfile" ]; then
        local pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then kill "$pid" && echo "  Stopped crawler-${idx} (PID $pid)"; fi
        rm -f "$pidfile"
    fi
}

start() {
    build
    echo "=== Starting $NODES crawlers (UDP :${START_PORT}-$((START_PORT+NODES-1))) ==="
    for i in $(seq 1 $NODES); do
        launch "$i" $((START_PORT + i - 1))
    done
}

stop() {
    echo "Stopping..."
    for i in $(seq 1 $NODES); do stop_one "$i"; done
    # Also stop legacy broker if running
    pkill -f broker 2>/dev/null || true
}

status() {
    for i in $(seq 1 $NODES); do
        local pf="$PID_DIR/crawler-${i}.pid"
        if [ -f "$pf" ] && kill -0 "$(cat "$pf")" 2>/dev/null; then
            echo "crawler-${i} : RUNNING (PID $(cat "$pf")) UDP :$((START_PORT + i - 1))"
        else echo "crawler-${i} : STOPPED"; fi
    done
    echo ""
    for i in $(seq 1 $NODES); do
        local lf="$LOG_DIR/crawler-${i}.log"
        [ -f "$lf" ] && echo "--- crawler-${i} ---" && tail -3 "$lf"
    done
}

logs() { tail -f "$LOG_DIR/crawler-${1:-1}.log"; }

case "${1:-start}" in
    build)  build ;;
    start)  start ;;
    stop)   stop ;;
    restart) stop; start ;;
    status) status ;;
    logs)   logs "$2" ;;
    *)      echo "Usage: $0 {build|start|stop|restart|status|logs [N]}" ;;
esac
