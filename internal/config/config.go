// Package config provides cluster configuration for the distributed KV store.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// NodeConfig represents the configuration for a single node in the cluster.
type NodeConfig struct {
	ID       string `json:"id"`
	GRPCAddr string `json:"grpc_addr"` // e.g., "localhost:9001"
	HTTPAddr string `json:"http_addr"` // e.g., "localhost:8001"
}

// Config holds all configuration for the distributed KV store.
type Config struct {
	// Node identity
	NodeID   string `json:"node_id"`
	GRPCAddr string `json:"grpc_addr"`
	HTTPAddr string `json:"http_addr"`

	// Cluster peers (excluding self)
	Peers []NodeConfig `json:"peers"`

	// Raft timing
	ElectionTimeoutMin  time.Duration `json:"-"`
	ElectionTimeoutMax  time.Duration `json:"-"`
	HeartbeatInterval   time.Duration `json:"-"`

	// Storage
	DataDir        string `json:"data_dir"`
	WALDir         string `json:"wal_dir"`
	SnapshotDir    string `json:"snapshot_dir"`

	// Storage engine tuning
	MemTableMaxSize  int `json:"memtable_max_size"`  // bytes before flush
	BloomFilterBits  int `json:"bloom_filter_bits"`   // bits per key

	// Snapshot
	SnapshotThreshold uint64 `json:"snapshot_threshold"` // log entries before snapshot
}

// DefaultConfig returns a sensible default configuration for development.
func DefaultConfig(nodeID, grpcAddr, httpAddr string) *Config {
	dataDir := fmt.Sprintf("data/%s", nodeID)
	return &Config{
		NodeID:   nodeID,
		GRPCAddr: grpcAddr,
		HTTPAddr: httpAddr,
		Peers:    []NodeConfig{},

		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,

		DataDir:     dataDir,
		WALDir:      fmt.Sprintf("%s/wal", dataDir),
		SnapshotDir: fmt.Sprintf("%s/snapshots", dataDir),

		MemTableMaxSize:   4 * 1024 * 1024, // 4MB
		BloomFilterBits:   10,               // ~1% false positive rate
		SnapshotThreshold: 10000,
	}
}

// LoadFromFile loads configuration from a JSON file.
func LoadFromFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	cfg := &Config{
		// Set timing defaults since they aren't in JSON
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		MemTableMaxSize:    4 * 1024 * 1024,
		BloomFilterBits:    10,
		SnapshotThreshold:  10000,
	}

	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	if cfg.DataDir == "" {
		cfg.DataDir = fmt.Sprintf("data/%s", cfg.NodeID)
	}
	if cfg.WALDir == "" {
		cfg.WALDir = fmt.Sprintf("%s/wal", cfg.DataDir)
	}
	if cfg.SnapshotDir == "" {
		cfg.SnapshotDir = fmt.Sprintf("%s/snapshots", cfg.DataDir)
	}

	return cfg, nil
}

// AllNodeAddrs returns a map of nodeID -> gRPC address for all known nodes
// (including self).
func (c *Config) AllNodeAddrs() map[string]string {
	addrs := make(map[string]string, len(c.Peers)+1)
	addrs[c.NodeID] = c.GRPCAddr
	for _, p := range c.Peers {
		addrs[p.ID] = p.GRPCAddr
	}
	return addrs
}

// PeerIDs returns a slice of all peer node IDs (excluding self).
func (c *Config) PeerIDs() []string {
	ids := make([]string, len(c.Peers))
	for i, p := range c.Peers {
		ids[i] = p.ID
	}
	return ids
}

// ClusterSize returns the total number of nodes in the cluster (self + peers).
func (c *Config) ClusterSize() int {
	return len(c.Peers) + 1
}

// MajoritySize returns the minimum number of nodes needed for a majority.
func (c *Config) MajoritySize() int {
	return c.ClusterSize()/2 + 1
}

// EnsureDirectories creates the data, WAL, and snapshot directories if they
// don't exist.
func (c *Config) EnsureDirectories() error {
	dirs := []string{c.DataDir, c.WALDir, c.SnapshotDir}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("creating directory %s: %w", dir, err)
		}
	}
	return nil
}
