package raft

// NodeState represents the state of a Raft node
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry represents a single entry in the Raft log
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

// AppendEntriesRequest represents an RPC to append entries to the log
type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesResponse is the response to an AppendEntriesRequest
type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}

// RequestVoteRequest represents an RPC to request a vote
type RequestVoteRequest struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteResponse is the response to a RequestVoteRequest
type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

// InstallSnapshotRequest represents an RPC to install a snapshot
type InstallSnapshotRequest struct {
	Term              uint64
	LeaderID          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

// InstallSnapshotResponse is the response to an InstallSnapshotRequest
type InstallSnapshotResponse struct {
	Term uint64
}

// Transport represents the interface for network communication
type Transport interface {
	SendAppendEntries(target string, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	SendRequestVote(target string, req *RequestVoteRequest) (*RequestVoteResponse, error)
	SendInstallSnapshot(target string, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
}

// StateMachine represents the interface for the key-value store state machine
type StateMachine interface {
	Apply(command []byte) ([]byte, error)
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

// ApplyResult is used to notify clients when their proposed command has been applied
type ApplyResult struct {
	Result []byte
	Error  error
}
