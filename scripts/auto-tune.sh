#!/usr/bin/env bash
# Auto-tune crawler workers based on CPU load and export errors
# Runs on the crawler server alongside the crawlers
# Usage: nohup bash scripts/auto-tune.sh &
set -e

WORKDIR="/opt/cherry-crawler"
LOG_DIR="$WORKDIR/logs"
STATE_FILE="$WORKDIR/.auto-tune-state"

# Tuning ranges for 2C4G
MIN_META=32
MAX_META=256
MIN_DHT=32
MAX_DHT=256
MIN_BATCH=32
MAX_BATCH=128

# Current values (read from env or use defaults)
CUR_META=${CHERRY_PICKER_METADATA_WORKERS:-64}
CUR_DHT=${CHERRY_PICKER_DHT_PACKET_WORKERS:-64}
CUR_BATCH=${CHERRY_PICKER_EXPORTER_BATCH:-64}

# State
HEALTHY_COUNT=0
ERROR_COUNT=0
CPU_AVG=0

save_state() {
    echo "meta=$CUR_META dht=$CUR_DHT batch=$CUR_BATCH healthy=$HEALTHY_COUNT err=$ERROR_COUNT cpu=$CPU_AVG" > "$STATE_FILE"
}

log() {
    echo "[auto-tune $(date +%H:%M:%S)] $@"
}

tune() {
    # Get CPU (1-min load avg as percentage of cores; cores=2)
    local cores=2
    local load=$(cat /proc/loadavg | awk '{print $1}')
    CPU_AVG=$(echo "scale=0; $load / $cores * 100" | bc)

    # Count recent errors in crawler logs (last 60s)
    local recent_errors=0
    for f in "$LOG_DIR"/*.log; do
        recent_errors=$((recent_errors + $(grep -c "export batch failed\|deadline exceeded\|413\|502\|503" "$f" 2>/dev/null | tail -1 || echo 0)))
    done

    # Count recent successful batches
    local recent_ok=0
    for f in "$LOG_DIR"/*.log; do
        recent_ok=$((recent_ok + $(grep -c "Batch processed" "$f" 2>/dev/null | tail -1 || echo 0)))
    done

    log "CPU=${CPU_AVG}% meta=${CUR_META} dht=${CUR_DHT} batch=${CUR_BATCH} errors=${recent_errors} ok=${recent_ok}"

    # Decision logic
    if [ "$recent_errors" -gt 5 ]; then
        # Too many errors: reduce batch and meta workers
        CUR_BATCH=$((CUR_BATCH * 2 / 3))
        CUR_META=$((CUR_META * 2 / 3))
        [ $CUR_BATCH -lt $MIN_BATCH ] && CUR_BATCH=$MIN_BATCH
        [ $CUR_META -lt $MIN_META ] && CUR_META=$MIN_META
        ERROR_COUNT=0
        HEALTHY_COUNT=0
        return 1  # need restart

    elif [ "$CPU_AVG" -gt 85 ]; then
        # CPU too high: reduce workers
        CUR_META=$((CUR_META * 3 / 4))
        CUR_DHT=$((CUR_DHT * 3 / 4))
        [ $CUR_META -lt $MIN_META ] && CUR_META=$MIN_META
        [ $CUR_DHT -lt $MIN_DHT ] && CUR_DHT=$MIN_DHT
        ERROR_COUNT=0
        HEALTHY_COUNT=0
        return 1  # need restart

    elif [ "$CPU_AVG" -lt 50 ] && [ "$recent_errors" -eq 0 ] && [ "$recent_ok" -gt 0 ]; then
        # Healthy: slowly increase
        HEALTHY_COUNT=$((HEALTHY_COUNT + 1))
        if [ $HEALTHY_COUNT -ge 6 ]; then  # 3 minutes stable
            CUR_META=$((CUR_META * 5 / 4))
            CUR_DHT=$((CUR_DHT * 5 / 4))
            [ $CUR_META -gt $MAX_META ] && CUR_META=$MAX_META
            [ $CUR_DHT -gt $MAX_DHT ] && CUR_DHT=$MAX_DHT
            HEALTHY_COUNT=0
            return 1  # need restart
        fi
    else
        HEALTHY_COUNT=0
    fi

    save_state
    return 0
}

restart_crawlers() {
    log "Restarting with meta=$CUR_META dht=$CUR_DHT batch=$CUR_BATCH"
    export CHERRY_PICKER_METADATA_WORKERS=$CUR_META
    export CHERRY_PICKER_DHT_PACKET_WORKERS=$CUR_DHT
    export CHERRY_PICKER_EXPORTER_BATCH=$CUR_BATCH
    bash "$(dirname "$0")/crawler-baremetal.sh" stop > /dev/null 2>&1
    sleep 2
    bash "$(dirname "$0")/crawler-baremetal.sh" start > /dev/null 2>&1
    save_state
}

# Main loop
log "Auto-tune started. meta=${MIN_META}-${MAX_META} dht=${MIN_DHT}-${MAX_DHT} batch=${MIN_BATCH}-${MAX_BATCH}"
restart_crawlers  # start with defaults

while true; do
    sleep 30
    tune && continue
    restart_crawlers
done
