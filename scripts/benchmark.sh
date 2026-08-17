#!/usr/bin/env bash
# benchmark.sh — Throughput and latency benchmark for the distributed KV store.
# Usage: ./scripts/benchmark.sh [leader_port] [num_requests]

set -euo pipefail

PORT="${1:-8001}"
NUM_REQUESTS="${2:-1000}"
BASE_URL="http://localhost:${PORT}"

GREEN='\033[0;32m'
CYAN='\033[0;36m'
NC='\033[0m'

log() { echo -e "${GREEN}[BENCH]${NC} $*"; }
header() { echo -e "\n${CYAN}═══════════════════════════════════════${NC}"; echo -e "${CYAN}  $*${NC}"; echo -e "${CYAN}═══════════════════════════════════════${NC}"; }

# Check if cluster is accessible
if ! curl -sf "${BASE_URL}/cluster/status" >/dev/null 2>&1; then
    echo "Error: Cannot reach cluster at ${BASE_URL}"
    echo "Make sure the cluster is running and this node is accessible."
    exit 1
fi

# Get cluster status
STATUS=$(curl -sf "${BASE_URL}/cluster/status")
log "Cluster status: ${STATUS}"

##############################
# Write Throughput Benchmark
##############################
header "Write Throughput (${NUM_REQUESTS} sequential PUTs)"

START=$(date +%s%N)
SUCCESS=0
FAIL=0

for i in $(seq 1 "$NUM_REQUESTS"); do
    if curl -sf -X PUT "${BASE_URL}/kv/bench-key-${i}" -d "value-${i}-$(date +%s%N)" >/dev/null 2>&1; then
        SUCCESS=$((SUCCESS + 1))
    else
        FAIL=$((FAIL + 1))
    fi
done

END=$(date +%s%N)
ELAPSED_MS=$(( (END - START) / 1000000 ))
ELAPSED_S=$(echo "scale=3; $ELAPSED_MS / 1000" | bc)
OPS_PER_SEC=$(echo "scale=1; $SUCCESS / $ELAPSED_S" | bc)

log "Completed: ${SUCCESS}/${NUM_REQUESTS} successful"
log "Failed:    ${FAIL}"
log "Duration:  ${ELAPSED_S}s"
log "Throughput: ${OPS_PER_SEC} writes/sec"

##############################
# Read Latency Benchmark
##############################
header "Read Latency (${NUM_REQUESTS} sequential GETs)"

LATENCIES=()
READ_SUCCESS=0

START=$(date +%s%N)

for i in $(seq 1 "$NUM_REQUESTS"); do
    REQ_START=$(date +%s%N)
    if curl -sf "${BASE_URL}/kv/bench-key-${i}" >/dev/null 2>&1; then
        REQ_END=$(date +%s%N)
        LATENCY_US=$(( (REQ_END - REQ_START) / 1000 ))
        LATENCIES+=("$LATENCY_US")
        READ_SUCCESS=$((READ_SUCCESS + 1))
    fi
done

END=$(date +%s%N)
ELAPSED_MS=$(( (END - START) / 1000000 ))
ELAPSED_S=$(echo "scale=3; $ELAPSED_MS / 1000" | bc)
READ_OPS=$(echo "scale=1; $READ_SUCCESS / $ELAPSED_S" | bc)

# Calculate percentiles
if [ ${#LATENCIES[@]} -gt 0 ]; then
    # Sort latencies
    IFS=$'\n' SORTED=($(sort -n <<<"${LATENCIES[*]}")); unset IFS

    TOTAL=${#SORTED[@]}
    P50_IDX=$(( TOTAL * 50 / 100 ))
    P95_IDX=$(( TOTAL * 95 / 100 ))
    P99_IDX=$(( TOTAL * 99 / 100 ))

    P50=${SORTED[$P50_IDX]}
    P95=${SORTED[$P95_IDX]}
    P99=${SORTED[$P99_IDX]}

    # Convert to ms
    P50_MS=$(echo "scale=2; $P50 / 1000" | bc)
    P95_MS=$(echo "scale=2; $P95 / 1000" | bc)
    P99_MS=$(echo "scale=2; $P99 / 1000" | bc)

    log "Completed: ${READ_SUCCESS}/${NUM_REQUESTS} successful"
    log "Duration:  ${ELAPSED_S}s"
    log "Throughput: ${READ_OPS} reads/sec"
    log "Latency p50: ${P50_MS}ms"
    log "Latency p95: ${P95_MS}ms"
    log "Latency p99: ${P99_MS}ms"
fi

##############################
# Mixed Workload (80% read, 20% write)
##############################
header "Mixed Workload (80/20 read/write, ${NUM_REQUESTS} ops)"

MIXED_SUCCESS=0
START=$(date +%s%N)

for i in $(seq 1 "$NUM_REQUESTS"); do
    ROLL=$((RANDOM % 100))
    if [ $ROLL -lt 80 ]; then
        # Read
        KEY_IDX=$((RANDOM % NUM_REQUESTS + 1))
        if curl -sf "${BASE_URL}/kv/bench-key-${KEY_IDX}" >/dev/null 2>&1; then
            MIXED_SUCCESS=$((MIXED_SUCCESS + 1))
        fi
    else
        # Write
        if curl -sf -X PUT "${BASE_URL}/kv/bench-mixed-${i}" -d "mixed-${i}" >/dev/null 2>&1; then
            MIXED_SUCCESS=$((MIXED_SUCCESS + 1))
        fi
    fi
done

END=$(date +%s%N)
ELAPSED_MS=$(( (END - START) / 1000000 ))
ELAPSED_S=$(echo "scale=3; $ELAPSED_MS / 1000" | bc)
MIXED_OPS=$(echo "scale=1; $MIXED_SUCCESS / $ELAPSED_S" | bc)

log "Completed: ${MIXED_SUCCESS}/${NUM_REQUESTS} successful"
log "Duration:  ${ELAPSED_S}s"
log "Throughput: ${MIXED_OPS} mixed ops/sec"

##############################
# Summary
##############################
header "BENCHMARK SUMMARY"
log "Write throughput: ${OPS_PER_SEC} ops/sec"
log "Read throughput:  ${READ_OPS} ops/sec"
log "Mixed throughput: ${MIXED_OPS} ops/sec"
if [ ${#LATENCIES[@]} -gt 0 ]; then
    log "Read latency p50: ${P50_MS}ms | p95: ${P95_MS}ms | p99: ${P99_MS}ms"
fi
