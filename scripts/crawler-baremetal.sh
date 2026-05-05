#!/usr/bin/env bash
# Bare-metal crawler launcher — runs N crawler processes directly (no Docker)
# Usage: ./scripts/crawler-baremetal.sh [start|stop|status|build]
set -e

WORKDIR="/opt/cherry-crawler"
NODES=8
START_PORT=20003
MAIN_API="${MAIN_API_URL:-http://CHANGE-ME}"
PREFIX="${INSTANCE_PREFIX:-bare}"

export CHERRY_PICKER_EVENT_QUEUE="16384"
export CHERRY_PICKER_DEDUPE_PEER_TTL="15m"
export CHERRY_PICKER_DEDUPE_METADATA_TTL="1h"
export CHERRY_PICKER_DHT_MODE="crawl"
export CHERRY_PICKER_EMIT_PEER_EVENTS="true"
export CHERRY_PICKER_DHT_PACKET_WORKERS="512"
export CHERRY_PICKER_DHT_PACKET_JOBS="2048"
export CHERRY_PICKER_DHT_MAX_NODES="20000"
export CHERRY_PICKER_DHT_REFRESH_NODES="512"
export CHERRY_PICKER_METADATA_ENABLED="true"
export CHERRY_PICKER_METADATA_BLACKLIST="131072"
export CHERRY_PICKER_METADATA_REQUEST_QUEUE="32768"
export CHERRY_PICKER_METADATA_WORKERS="512"
export CHERRY_PICKER_EXPORTER="http"
export CHERRY_PICKER_EXPORTER_URL="$MAIN_API/api/v1/torrents/batch"
export CHERRY_PICKER_EXPORTER_BATCH="128"
export CHERRY_PICKER_EXPORTER_FLUSH="2s"
export CHERRY_PICKER_EXPORTER_TIMEOUT="5s"
export CHERRY_PICKER_EXPORTER_HTTP_RETRIES="3"
export CHERRY_PICKER_EXPORTER_RETRY_BACKOFF="1s"

PID_DIR="$WORKDIR/pids"
mkdir -p "$PID_DIR"

build() {
    echo "Building crawler..."
    cd "$(dirname "$0")/../go/cherry-picker"
    go build -o "$WORKDIR/cherry-picker" ./cmd/cherry-picker
    echo "Built: $WORKDIR/cherry-picker"
}

start_one() {
    local i=$1
    local port=$((START_PORT + i - 1))
    local id="${PREFIX}-${i}"
    local pidfile="$PID_DIR/crawler-${i}.pid"
    local logfile="$WORKDIR/logs/crawler-${i}.log"

    if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
        echo "crawler-$i already running (PID $(cat "$pidfile"))"
        return
    fi

    mkdir -p "$WORKDIR/logs"

    CHERRY_PICKER_ROLE="combined" \
    CHERRY_PICKER_INSTANCE_ID="$id" \
    CHERRY_PICKER_LISTEN_ADDR=":$port" \
    nohup "$WORKDIR/cherry-picker" >> "$logfile" 2>&1 &
    echo $! > "$pidfile"
    echo "Started crawler-$i ($id) on UDP :$port (PID $!)"
}

start() {
    build
    for i in $(seq 1 $NODES); do
        start_one "$i"
    done
}

stop() {
    for i in $(seq 1 $NODES); do
        local pidfile="$PID_DIR/crawler-${i}.pid"
        if [ -f "$pidfile" ]; then
            local pid=$(cat "$pidfile")
            if kill -0 "$pid" 2>/dev/null; then
                kill "$pid" && echo "Stopped crawler-$i (PID $pid)"
            fi
            rm -f "$pidfile"
        fi
    done
}

status() {
    for i in $(seq 1 $NODES); do
        local pidfile="$PID_DIR/crawler-${i}.pid"
        if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
            echo "crawler-$i : RUNNING (PID $(cat "$pidfile")) — UDP :$((START_PORT + i - 1))"
        else
            echo "crawler-$i : STOPPED"
        fi
    done
    echo ""
    echo "Recent log tail:"
    tail -3 "$WORKDIR/logs/crawler-1.log" 2>/dev/null || true
}

logs() {
    tail -f "$WORKDIR/logs/crawler-${1:-1}.log"
}

case "${1:-start}" in
    build)  build ;;
    start)  start ;;
    stop)   stop ;;
    restart) stop; start ;;
    status) status ;;
    logs)   logs "$2" ;;
    *)      echo "Usage: $0 {build|start|stop|restart|status|logs [N]}" ;;
esac
