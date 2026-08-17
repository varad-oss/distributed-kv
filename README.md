# Distributed Key-Value Store

A **fault-tolerant, linearizable key-value store** built from scratch in Go, implementing the **Raft consensus protocol** for leader election and log replication, with a **WAL-backed LSM-tree storage engine** and inter-node RPC communication.

## Features

- **Raft Consensus** — Leader election with randomized timeouts, log replication with conflict resolution, and commit safety (§5.4.2)
- **LSM-Tree Storage Engine** — Write-ahead log, skip-list MemTable, sorted SSTables with bloom filters
- **Fault Tolerance** — Automatic leader failover, crash recovery via WAL replay, snapshot-based catch-up for lagging followers
- **Linearizable Reads & Writes** — All operations go through the Raft log for strong consistency
- **HTTP REST API** — Simple `PUT`/`GET`/`DELETE` interface with automatic leader forwarding
- **Dockerized Cluster** — 3-node cluster via Docker Compose with chaos testing scripts

## Architecture

```
                    ┌─────────────┐
                    │   Client    │
                    │  (HTTP)     │
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
         ┌─────────┐ ┌─────────┐ ┌─────────┐
         │ Node 1  │ │ Node 2  │ │ Node 3  │
         │(Leader) │ │(Follower)│ │(Follower)│
         ├─────────┤ ├─────────┤ ├─────────┤
         │ HTTP API│ │ HTTP API│ │ HTTP API│
         │ Raft    │◄─►Raft   │◄─►Raft   │
         │ Storage │ │ Storage │ │ Storage │
         └─────────┘ └─────────┘ └─────────┘
```

## Quick Start

### Local (3 terminals)

```bash
# Build
go build -o dkv ./cmd/server

# Terminal 1 — Node 1
./dkv --id=node1 --grpc-addr=:9001 --http-addr=:8001 \
      --peers=node2=:9002=:8002,node3=:9003=:8003

# Terminal 2 — Node 2
./dkv --id=node2 --grpc-addr=:9002 --http-addr=:8002 \
      --peers=node1=:9001=:8001,node3=:9003=:8003

# Terminal 3 — Node 3
./dkv --id=node3 --grpc-addr=:9003 --http-addr=:8003 \
      --peers=node1=:9001=:8001,node2=:9002=:8002
```

### Docker Compose

```bash
cd deployments
docker compose up --build
```

## Usage

```bash
# Write a key
curl -X PUT http://localhost:8001/kv/mykey -d "myvalue"

# Read a key
curl http://localhost:8001/kv/mykey

# Delete a key
curl -X DELETE http://localhost:8001/kv/mykey

# Check cluster status
curl http://localhost:8001/cluster/status
```

## Testing

### Chaos Testing

```bash
# Run all chaos tests (leader kill, random kill, partition, rapid restart)
./scripts/chaos.sh all

# Run a specific test
./scripts/chaos.sh leader_kill
```

### Benchmarking

```bash
# Run benchmarks against the leader (default: port 8001, 1000 requests)
./scripts/benchmark.sh 8001 1000
```

## Project Structure

```
distributed-kv/
├── cmd/server/          # Entry point
├── internal/
│   ├── raft/            # Raft consensus (election, replication, persistence)
│   ├── transport/       # Inter-node RPC communication
│   ├── storage/         # LSM-tree engine (WAL, MemTable, SSTable, bloom filter)
│   ├── server/          # HTTP API and KV state machine
│   └── config/          # Cluster configuration
├── proto/               # Protobuf definitions (reference)
├── deployments/         # Docker Compose for multi-node cluster
├── scripts/             # Chaos testing and benchmarking
└── tests/               # Integration tests
```

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Go** | Goroutines are natural for Raft's concurrent timers; first-class RPC support |
| **net/rpc over gRPC** | Zero external dependencies; same semantics without protoc toolchain |
| **Single event-loop Raft** | Avoids fine-grained locking; all state mutations in one goroutine |
| **Skip list MemTable** | O(log n) insert/lookup; simpler than balanced BST with comparable performance |
| **Bloom filters on SSTables** | Avoids unnecessary disk reads for missing keys (~1% false positive rate) |

## References

- [In Search of an Understandable Consensus Algorithm (Raft Paper)](https://raft.github.io/raft.pdf)
- [Raft Visualization](https://thesecretlivesofdata.com/raft/)
- [MIT 6.824 Distributed Systems](https://pdos.csail.mit.edu/6.824/)
