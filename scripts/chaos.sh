#!/usr/bin/env bash
# chaos.sh — Simple chaos testing script for the distributed KV store.
# Usage: ./scripts/chaos.sh [test_name]
#
# Tests:
#   leader_kill    — Kill the leader node, verify cluster elects a new leader
#   random_kill    — Randomly kill a node, verify cluster continues operating
#   partition      — Simulate a network partition by pausing a container
#   rapid_restart  — Rapidly stop and start a node
#   all            — Run all tests sequentially

set -euo pipefail

COMPOSE_FILE="deployments/docker-compose.yml"
NODES=("dkv-node1" "dkv-node2" "dkv-node3")
HTTP_PORTS=(8001 8002 8003)

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

wait_for_cluster() {
    log_info "Waiting for cluster to stabilize..."
    sleep 3
}

find_leader() {
    for i in "${!HTTP_PORTS[@]}"; do
        local port=${HTTP_PORTS[$i]}
        local status
        status=$(curl -s "http://localhost:${port}/cluster/status" 2>/dev/null || echo "{}")
        if echo "$status" | grep -q '"role":"Leader"'; then
            echo "$i"
            return
        fi
    done
    echo "-1"
}

write_key() {
    local port=$1 key=$2 value=$3
    curl -sf -X PUT "http://localhost:${port}/kv/${key}" -d "${value}" 2>/dev/null
}

read_key() {
    local port=$1 key=$2
    curl -sf "http://localhost:${port}/kv/${key}" 2>/dev/null
}

test_leader_kill() {
    log_info "=== TEST: Leader Kill ==="

    # Write a value through the leader
    local leader_idx
    leader_idx=$(find_leader)
    if [[ "$leader_idx" == "-1" ]]; then
        log_error "No leader found"
        return 1
    fi

    local leader_port=${HTTP_PORTS[$leader_idx]}
    local leader_container=${NODES[$leader_idx]}
    log_info "Leader is ${leader_container} on port ${leader_port}"

    # Write test data
    write_key "$leader_port" "chaos-test-1" "before-kill"
    log_info "Wrote key 'chaos-test-1' = 'before-kill'"

    # Kill the leader
    log_info "Killing leader: ${leader_container}"
    docker stop "$leader_container" >/dev/null 2>&1

    # Wait for new election
    log_info "Waiting for new leader election..."
    sleep 5

    # Find new leader
    local new_leader_idx
    new_leader_idx=$(find_leader)
    if [[ "$new_leader_idx" == "-1" ]]; then
        log_error "No new leader elected after killing ${leader_container}"
        docker start "$leader_container" >/dev/null 2>&1
        return 1
    fi

    local new_leader_port=${HTTP_PORTS[$new_leader_idx]}
    log_info "New leader on port ${new_leader_port}"

    # Verify data is still accessible
    local value
    value=$(read_key "$new_leader_port" "chaos-test-1")
    if [[ "$value" == "before-kill" ]]; then
        log_info "✅ Data preserved after leader kill"
    else
        log_error "❌ Data lost after leader kill (got: ${value})"
    fi

    # Write new data through new leader
    write_key "$new_leader_port" "chaos-test-2" "after-kill"
    log_info "Wrote key 'chaos-test-2' = 'after-kill' through new leader"

    # Restart killed node
    log_info "Restarting ${leader_container}..."
    docker start "$leader_container" >/dev/null 2>&1
    sleep 5

    log_info "=== Leader Kill Test Complete ==="
}

test_random_kill() {
    log_info "=== TEST: Random Node Kill ==="

    # Pick a random node
    local idx=$((RANDOM % 3))
    local container=${NODES[$idx]}
    log_info "Randomly selected: ${container}"

    # Write some data first
    local leader_idx
    leader_idx=$(find_leader)
    if [[ "$leader_idx" == "-1" ]]; then
        log_error "No leader found"
        return 1
    fi
    local leader_port=${HTTP_PORTS[$leader_idx]}

    for i in $(seq 1 10); do
        write_key "$leader_port" "random-test-${i}" "value-${i}" || true
    done
    log_info "Wrote 10 test keys"

    # Kill the random node
    log_info "Killing ${container}..."
    docker stop "$container" >/dev/null 2>&1
    sleep 3

    # Verify cluster still works (if we didn't kill the leader, or new leader elected)
    sleep 3
    leader_idx=$(find_leader)
    if [[ "$leader_idx" != "-1" ]]; then
        local port=${HTTP_PORTS[$leader_idx]}
        write_key "$port" "after-random-kill" "still-working"
        log_info "✅ Cluster still operational after killing ${container}"
    else
        log_warn "Cluster may need more time to elect a new leader"
    fi

    # Restart
    docker start "$container" >/dev/null 2>&1
    sleep 3
    log_info "=== Random Kill Test Complete ==="
}

test_partition() {
    log_info "=== TEST: Network Partition (Container Pause) ==="

    local idx=$((RANDOM % 3))
    local container=${NODES[$idx]}
    log_info "Partitioning: ${container}"

    docker pause "$container" >/dev/null 2>&1
    log_info "Container paused (simulating network partition)"
    sleep 5

    local leader_idx
    leader_idx=$(find_leader)
    if [[ "$leader_idx" != "-1" ]]; then
        local port=${HTTP_PORTS[$leader_idx]}
        write_key "$port" "partition-test" "during-partition"
        log_info "✅ Cluster operational during partition"
    else
        log_warn "No leader found during partition (expected if partitioned node was leader)"
    fi

    docker unpause "$container" >/dev/null 2>&1
    log_info "Container unpaused"
    sleep 5

    log_info "=== Partition Test Complete ==="
}

test_rapid_restart() {
    log_info "=== TEST: Rapid Restart ==="

    local idx=$((RANDOM % 3))
    local container=${NODES[$idx]}
    log_info "Rapid-restarting: ${container}"

    for i in $(seq 1 3); do
        log_info "  Cycle ${i}: stop..."
        docker stop "$container" >/dev/null 2>&1
        sleep 1
        log_info "  Cycle ${i}: start..."
        docker start "$container" >/dev/null 2>&1
        sleep 2
    done

    sleep 5
    local leader_idx
    leader_idx=$(find_leader)
    if [[ "$leader_idx" != "-1" ]]; then
        log_info "✅ Cluster recovered after rapid restarts"
    else
        log_error "❌ No leader found after rapid restarts"
    fi

    log_info "=== Rapid Restart Test Complete ==="
}

# Main
case "${1:-all}" in
    leader_kill)   test_leader_kill ;;
    random_kill)   test_random_kill ;;
    partition)     test_partition ;;
    rapid_restart) test_rapid_restart ;;
    all)
        wait_for_cluster
        test_leader_kill
        echo
        test_random_kill
        echo
        test_partition
        echo
        test_rapid_restart
        echo
        log_info "=== All chaos tests complete ==="
        ;;
    *)
        echo "Usage: $0 {leader_kill|random_kill|partition|rapid_restart|all}"
        exit 1
        ;;
esac
