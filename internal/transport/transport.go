package transport

import (
	"fmt"
	"log"
	"net/rpc"
	"sync"
	"time"

	"github.com/varad/distributed-kv/internal/raft"
)

// GRPCTransport implements the raft.Transport interface using Go's net/rpc.
// Despite the name (kept for consistency with the proto-based design), it
// uses net/rpc with gob encoding to avoid a protoc dependency.
type GRPCTransport struct {
	localAddr   string
	connections map[string]*rpc.Client
	peerAddrs   map[string]string // nodeID -> address for lazy reconnect
	mu          sync.RWMutex
}

func NewGRPCTransport(localAddr string) *GRPCTransport {
	return &GRPCTransport{
		localAddr:   localAddr,
		connections: make(map[string]*rpc.Client),
		peerAddrs:   make(map[string]string),
	}
}

// Connect registers a peer address and attempts an initial connection.
// If the initial connection fails, it is retried lazily on the next RPC call.
func (t *GRPCTransport) Connect(nodeID string, addr string) error {
	t.mu.Lock()
	t.peerAddrs[nodeID] = addr
	t.mu.Unlock()

	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		log.Printf("Initial connection to %s at %s failed (will retry lazily): %v", nodeID, addr, err)
		return nil // Don't fail on initial connect — retry lazily
	}
	t.mu.Lock()
	t.connections[nodeID] = client
	t.mu.Unlock()
	return nil
}

func (t *GRPCTransport) getClient(target string) (*rpc.Client, error) {
	t.mu.RLock()
	client, ok := t.connections[target]
	addr := t.peerAddrs[target]
	t.mu.RUnlock()

	if ok {
		return client, nil
	}

	// Lazy reconnect
	if addr == "" {
		return nil, fmt.Errorf("unknown peer: %s", target)
	}

	newClient, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("connect to %s (%s): %w", target, addr, err)
	}

	t.mu.Lock()
	t.connections[target] = newClient
	t.mu.Unlock()
	return newClient, nil
}

// reconnect drops a stale connection and forces a fresh dial on the next call.
func (t *GRPCTransport) reconnect(target string) {
	t.mu.Lock()
	if client, ok := t.connections[target]; ok {
		client.Close()
		delete(t.connections, target)
	}
	t.mu.Unlock()
}

func (t *GRPCTransport) SendAppendEntries(target string, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	client, err := t.getClient(target)
	if err != nil {
		return nil, err
	}

	var resp raft.AppendEntriesResponse
	call := client.Go("RaftGRPCServer.HandleAppendEntries", req, &resp, nil)
	select {
	case <-call.Done:
		if call.Error != nil {
			t.reconnect(target)
			return nil, call.Error
		}
		return &resp, nil
	case <-time.After(500 * time.Millisecond):
		t.reconnect(target)
		return nil, fmt.Errorf("timeout")
	}
}

func (t *GRPCTransport) SendRequestVote(target string, req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	client, err := t.getClient(target)
	if err != nil {
		return nil, err
	}

	var resp raft.RequestVoteResponse
	call := client.Go("RaftGRPCServer.HandleRequestVote", req, &resp, nil)
	select {
	case <-call.Done:
		if call.Error != nil {
			t.reconnect(target)
			return nil, call.Error
		}
		return &resp, nil
	case <-time.After(500 * time.Millisecond):
		t.reconnect(target)
		return nil, fmt.Errorf("timeout")
	}
}

func (t *GRPCTransport) SendInstallSnapshot(target string, req *raft.InstallSnapshotRequest) (*raft.InstallSnapshotResponse, error) {
	client, err := t.getClient(target)
	if err != nil {
		return nil, err
	}

	var resp raft.InstallSnapshotResponse
	call := client.Go("RaftGRPCServer.HandleInstallSnapshot", req, &resp, nil)
	select {
	case <-call.Done:
		if call.Error != nil {
			t.reconnect(target)
			return nil, call.Error
		}
		return &resp, nil
	case <-time.After(2 * time.Second):
		t.reconnect(target)
		return nil, fmt.Errorf("timeout")
	}
}

func (t *GRPCTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, client := range t.connections {
		client.Close()
	}
	return nil
}
