#!/usr/bin/env bash
# Cherry JS Spider Manager
set -e

WORKDIR="$(cd "$(dirname "$0")" && pwd)"
PID_DIR="$WORKDIR/pids"
LOG_DIR="$WORKDIR/logs"
mkdir -p "$PID_DIR" "$LOG_DIR"

NODES=6
START_PORT=20003
MAIN_API="${SPIDER_API_URL:-http://CHANGE-ME}"
PREFIX="${SPIDER_INSTANCE_PREFIX:-js}"

start_one() {
    local i=$1 port=$2
    local id="${PREFIX}-${i}"
    local pidfile="$PID_DIR/spider-${i}.pid"
    local logfile="$LOG_DIR/spider-${i}.log"

    if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
        echo "  spider-${i} already running (PID $(cat "$pidfile"))"
        return
    fi

    SPIDER_PORT="$port" \
    SPIDER_ADDRESS="0.0.0.0" \
    SPIDER_API_URL="$MAIN_API" \
    SPIDER_INSTANCE_ID="$id" \
    SPIDER_MAX_CONN="${SPIDER_MAX_CONN:-600}" \
    SPIDER_MAX_NODES="${SPIDER_MAX_NODES:-5000}" \
    SPIDER_TIMEOUT="${SPIDER_TIMEOUT:-5000}" \
    SPIDER_DEDUP_SIZE="${SPIDER_DEDUP_SIZE:-200000}" \
    nohup node "$WORKDIR/cherry-spider.js" >> "$logfile" 2>&1 &
    echo $! > "$pidfile"
    echo "  Started spider-${i} ($id) UDP :$port (PID $!)"
}

stop_one() {
    local i=$1
    local pidfile="$PID_DIR/spider-${i}.pid"
    if [ -f "$pidfile" ]; then
        local pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" && echo "  Stopped spider-${i} (PID $pid)"
        fi
        rm -f "$pidfile"
    fi
}

start() {
    echo "=== Starting $NODES spider nodes ($(node -v)) ==="
    for i in $(seq 1 $NODES); do
        start_one "$i" $((START_PORT + i - 1))
    done
}

stop() {
    echo "Stopping..."
    for i in $(seq 1 $NODES); do stop_one "$i"; done
}

status() {
    for i in $(seq 1 $NODES); do
        local pidfile="$PID_DIR/spider-${i}.pid"
        if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
            echo "spider-${i} : RUNNING (PID $(cat "$pidfile")) UDP :$((START_PORT + i - 1))"
        else
            echo "spider-${i} : STOPPED"
        fi
    done
    echo ""
    echo "=== Recent logs ==="
    for i in $(seq 1 $NODES); do
        local lf="$LOG_DIR/spider-${i}.log"
        [ -f "$lf" ] && echo "--- spider-${i} ---" && tail -4 "$lf"
    done
}

logs() { tail -f "$LOG_DIR/spider-${1:-1}.log"; }

case "${1:-start}" in
    start)  start ;;
    stop)   stop ;;
    restart) stop; start ;;
    status) status ;;
    logs)   logs "$2" ;;
    *)      echo "Usage: $0 {start|stop|restart|status|logs [N]}" ;;
esac
