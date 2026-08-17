package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// PersistentState holds the state that must be saved to disk before responding to RPCs.
type PersistentState struct {
	CurrentTerm uint64 `json:"current_term"`
	VotedFor    string `json:"voted_for"`
}

type StateManager struct {
	mu       sync.Mutex
	filePath string
	state    PersistentState
}

// NewStateManager creates a new StateManager and loads existing state.
func NewStateManager(dataDir string) (*StateManager, error) {
	filePath := filepath.Join(dataDir, "raft_state.json")
	sm := &StateManager{
		filePath: filePath,
	}
	if err := sm.Load(); err != nil {
		return nil, err
	}
	return sm, nil
}

// Load reads the persistent state from disk.
func (sm *StateManager) Load() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	data, err := os.ReadFile(sm.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			sm.state = PersistentState{CurrentTerm: 0, VotedFor: ""}
			return nil
		}
		return fmt.Errorf("failed to read state file: %w", err)
	}

	if err := json.Unmarshal(data, &sm.state); err != nil {
		return fmt.Errorf("failed to unmarshal state: %w", err)
	}
	return nil
}

// Save writes the current term and votedFor to disk safely.
func (sm *StateManager) Save(term uint64, votedFor string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.state.CurrentTerm = term
	sm.state.VotedFor = votedFor

	data, err := json.Marshal(sm.state)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	tmpPath := sm.filePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write state file: %w", err)
	}

	if err := os.Rename(tmpPath, sm.filePath); err != nil {
		return fmt.Errorf("failed to rename state file: %w", err)
	}

	return nil
}

// State returns a copy of the current persistent state.
func (sm *StateManager) State() PersistentState {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.state
}
