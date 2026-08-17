package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/varad/distributed-kv/internal/config"
	"github.com/varad/distributed-kv/internal/raft"
	"github.com/varad/distributed-kv/internal/server"
	"github.com/varad/distributed-kv/internal/storage"
	"github.com/varad/distributed-kv/internal/transport"
)

func main() {
	id := flag.String("id", "node1", "Node ID")
	grpcAddr := flag.String("grpc-addr", ":9001", "gRPC address")
	httpAddr := flag.String("http-addr", ":8001", "HTTP address")
	peersFlag := flag.String("peers", "", "Comma-separated peers: id=grpcAddr=httpAddr")
	dataDir := flag.String("data-dir", "", "Data directory")

	flag.Parse()

	// Parse peers
	var peers []config.NodeConfig
	peerHTTPMap := make(map[string]string)
	if *peersFlag != "" {
		for _, p := range strings.Split(*peersFlag, ",") {
			parts := strings.Split(p, "=")
			if len(parts) == 3 {
				peers = append(peers, config.NodeConfig{
					ID:       parts[0],
					GRPCAddr: parts[1],
					HTTPAddr: parts[2],
				})
				peerHTTPMap[parts[0]] = parts[2]
			} else {
				log.Fatalf("Invalid peer format, expected id=grpcAddr=httpAddr, got %s", p)
			}
		}
	}

	cfg := config.DefaultConfig(*id, *grpcAddr, *httpAddr)
	if *dataDir != "" {
		cfg.DataDir = *dataDir
		cfg.WALDir = fmt.Sprintf("%s/wal", *dataDir)
		cfg.SnapshotDir = fmt.Sprintf("%s/snapshots", *dataDir)
	}
	cfg.Peers = peers

	if err := cfg.EnsureDirectories(); err != nil {
		log.Fatalf("Failed to create directories: %v", err)
	}

	// 1. Create Storage Engine
	engine, err := storage.NewEngine(cfg.DataDir, cfg.MemTableMaxSize, cfg.BloomFilterBits)
	if err != nil {
		log.Fatalf("Failed to create storage engine: %v", err)
	}
	defer engine.Close()

	// 2. Create KV State Machine
	stateMachine := server.NewKVStateMachine(engine)

	// 3. Create Transport
	trans := transport.NewGRPCTransport(cfg.GRPCAddr)
	defer trans.Close()

	// Connect to peers
	for _, p := range cfg.Peers {
		if err := trans.Connect(p.ID, p.GRPCAddr); err != nil {
			log.Printf("Warning: failed to connect to peer %s at %s: %v", p.ID, p.GRPCAddr, err)
		}
	}

	// 4. Create Raft Node
	raftNode, err := raft.NewRaftNode(cfg, trans, stateMachine)
	if err != nil {
		log.Fatalf("Failed to create raft node: %v", err)
	}

	// 5. Create and start gRPC Server (RPC)
	rpcServer := transport.NewRaftGRPCServer(raftNode)
	go func() {
		log.Printf("Starting RPC server on %s", cfg.GRPCAddr)
		if err := rpcServer.Serve(cfg.GRPCAddr); err != nil {
			log.Fatalf("RPC server failed: %v", err)
		}
	}()

	// Start Raft Node
	raftNode.Start()
	defer raftNode.Stop()

	// 6. Create and start HTTP Server
	httpServer := server.NewHTTPServer(cfg.HTTPAddr, raftNode, engine, peerHTTPMap)
	go func() {
		log.Printf("Starting HTTP server on %s", cfg.HTTPAddr)
		if err := httpServer.Start(); err != nil {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("Shutting down...")
}

// Example usage:
// Node 1: ./server --id=node1 --grpc-addr=:9001 --http-addr=:8001 --peers=node2=:9002=:8002,node3=:9003=:8003
// Node 2: ./server --id=node2 --grpc-addr=:9002 --http-addr=:8002 --peers=node1=:9001=:8001,node3=:9003=:8003
// Node 3: ./server --id=node3 --grpc-addr=:9003 --http-addr=:8003 --peers=node1=:9001=:8001,node2=:9002=:8002
