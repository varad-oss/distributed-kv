package raft

import (
	"fmt"
	"sync"
)

// RaftLog manages the log entries for the Raft node.
type RaftLog struct {
	mu      sync.RWMutex
	entries []LogEntry
	
	// Information about the last snapshot
	lastSnapshotIndex uint64
	lastSnapshotTerm  uint64
}

// NewRaftLog creates a new empty Raft log.
func NewRaftLog() *RaftLog {
	return &RaftLog{
		entries:           []LogEntry{},
		lastSnapshotIndex: 0,
		lastSnapshotTerm:  0,
	}
}

// Append adds entries to the log.
func (l *RaftLog) Append(entries ...LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entries...)
}

// GetEntry returns the entry at the given index.
// Returns an error if the index has been compacted or doesn't exist.
func (l *RaftLog) GetEntry(index uint64) (LogEntry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index < l.lastSnapshotIndex {
		return LogEntry{}, fmt.Errorf("index %d compacted", index)
	}
	if index == l.lastSnapshotIndex {
		return LogEntry{Index: l.lastSnapshotIndex, Term: l.lastSnapshotTerm}, nil
	}
	if index > l.lastIndex() {
		return LogEntry{}, fmt.Errorf("index %d out of bounds", index)
	}

	return l.entries[index-l.lastSnapshotIndex-1], nil
}

// GetEntries returns all entries from the given index to the end of the log.
func (l *RaftLog) GetEntries(from uint64) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if from <= l.lastSnapshotIndex {
		from = l.lastSnapshotIndex + 1
	}
	
	lastIndex := l.lastIndex()
	if from > lastIndex {
		return nil
	}

	start := from - l.lastSnapshotIndex - 1
	// return a copy to prevent mutation
	res := make([]LogEntry, len(l.entries[start:]))
	copy(res, l.entries[start:])
	return res
}

// LastIndex returns the index of the last entry in the log.
func (l *RaftLog) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastIndex()
}

func (l *RaftLog) lastIndex() uint64 {
	if len(l.entries) == 0 {
		return l.lastSnapshotIndex
	}
	return l.entries[len(l.entries)-1].Index
}

// LastTerm returns the term of the last entry in the log.
func (l *RaftLog) LastTerm() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	if len(l.entries) == 0 {
		return l.lastSnapshotTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// TruncateAfter removes all entries starting from the given index.
func (l *RaftLog) TruncateAfter(index uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index <= l.lastSnapshotIndex {
		l.entries = []LogEntry{}
		return
	}
	if index > l.lastIndex() {
		return
	}

	sliceIndex := index - l.lastSnapshotIndex - 1
	l.entries = l.entries[:sliceIndex]
}

// TruncateBefore removes all entries up to and including the given index.
// Used for log compaction.
func (l *RaftLog) TruncateBefore(index uint64, term uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index <= l.lastSnapshotIndex {
		return
	}
	
	if index >= l.lastIndex() {
		l.entries = []LogEntry{}
	} else {
		sliceIndex := index - l.lastSnapshotIndex
		remaining := make([]LogEntry, len(l.entries[sliceIndex:]))
		copy(remaining, l.entries[sliceIndex:])
		l.entries = remaining
	}

	l.lastSnapshotIndex = index
	l.lastSnapshotTerm = term
}
