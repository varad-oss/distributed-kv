package raft

import (
	"bytes"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/varad/distributed-kv/internal/config"
)

type requestVoteTuple struct {
	req  *RequestVoteRequest
	resp chan *RequestVoteResponse
}

type appendEntriesTuple struct {
	req  *AppendEntriesRequest
	resp chan *AppendEntriesResponse
}

type proposeTuple struct {
	command []byte
	resp    chan ApplyResult
}

// RaftNode represents a single node in the Raft cluster.
type RaftNode struct {
	id    string
	peers []string
	cfg   *config.Config

	transport    Transport
	stateMachine StateMachine
	stateManager *StateManager

	currentTerm uint64
	votedFor    string
	log         *RaftLog

	commitIndex uint64
	lastApplied uint64
	state       NodeState

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	reqVoteCh       chan requestVoteTuple
	appendEntriesCh chan appendEntriesTuple
	proposeCh       chan proposeTuple
	
	// Channels for async RPC responses
	reqVoteRespCh       chan requestVoteRespTuple
	appendEntriesRespCh chan appendEntriesRespTuple
	
	stopCh          chan struct{}

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	logger *log.Logger
	
	// State for current election
	votesReceived int
}

type requestVoteRespTuple struct {
	target string
	resp   *RequestVoteResponse
	err    error
}

type appendEntriesRespTuple struct {
	target string
	req    *AppendEntriesRequest
	resp   *AppendEntriesResponse
	err    error
}

// NewRaftNode creates a new Raft node.
func NewRaftNode(cfg *config.Config, transport Transport, stateMachine StateMachine) (*RaftNode, error) {
	stateMgr, err := NewStateManager(cfg.DataDir)
	if err != nil {
		return nil, err
	}

	state := stateMgr.State()

	rn := &RaftNode{
		id:           cfg.NodeID,
		peers:        cfg.PeerIDs(),
		cfg:          cfg,
		transport:    transport,
		stateMachine: stateMachine,
		stateManager: stateMgr,
		currentTerm:  state.CurrentTerm,
		votedFor:     state.VotedFor,
		log:          NewRaftLog(),
		state:        Follower,
		nextIndex:    make(map[string]uint64),
		matchIndex:   make(map[string]uint64),

		reqVoteCh:           make(chan requestVoteTuple),
		appendEntriesCh:     make(chan appendEntriesTuple),
		proposeCh:           make(chan proposeTuple),
		reqVoteRespCh:       make(chan requestVoteRespTuple, 100),
		appendEntriesRespCh: make(chan appendEntriesRespTuple, 100),
		stopCh:              make(chan struct{}),

		logger: log.New(os.Stdout, "["+cfg.NodeID+"] ", log.LstdFlags|log.Lmsgprefix),
	}

	return rn, nil
}

// Start begins the Raft node's background event loop.
func (rn *RaftNode) Start() {
	rn.logger.Printf("Starting Raft node in term %d", rn.currentTerm)
	rn.electionTimer = time.NewTimer(rn.randomElectionTimeout())
	rn.heartbeatTimer = time.NewTimer(rn.cfg.HeartbeatInterval)
	rn.heartbeatTimer.Stop() // Only running when leader

	go rn.run()
}

// Stop halts the Raft node's background event loop.
func (rn *RaftNode) Stop() {
	close(rn.stopCh)
}

func (rn *RaftNode) randomElectionTimeout() time.Duration {
	diff := rn.cfg.ElectionTimeoutMax - rn.cfg.ElectionTimeoutMin
	if diff <= 0 {
		return rn.cfg.ElectionTimeoutMin
	}
	return rn.cfg.ElectionTimeoutMin + time.Duration(rand.Int63n(int64(diff)))
}

func (rn *RaftNode) resetElectionTimer() {
	if !rn.electionTimer.Stop() {
		select {
		case <-rn.electionTimer.C:
		default:
		}
	}
	rn.electionTimer.Reset(rn.randomElectionTimeout())
}

func (rn *RaftNode) persist() {
	if err := rn.stateManager.Save(rn.currentTerm, rn.votedFor); err != nil {
		rn.logger.Printf("Failed to persist state: %v", err)
	}
}

func (rn *RaftNode) run() {
	for {
		select {
		case <-rn.stopCh:
			rn.logger.Println("Raft node stopping")
			return

		case <-rn.electionTimer.C:
			rn.startElection()

		case <-rn.heartbeatTimer.C:
			if rn.state == Leader {
				rn.sendHeartbeats()
				rn.heartbeatTimer.Reset(rn.cfg.HeartbeatInterval)
			}

		case reqTuple := <-rn.reqVoteCh:
			resp := rn.handleRequestVote(reqTuple.req)
			reqTuple.resp <- resp

		case reqTuple := <-rn.appendEntriesCh:
			resp := rn.handleAppendEntries(reqTuple.req)
			reqTuple.resp <- resp

		case propTuple := <-rn.proposeCh:
			if rn.state != Leader {
				propTuple.resp <- ApplyResult{Error: bytes.ErrTooLarge} // Dummy error for "not leader"
				continue
			}
			rn.handlePropose(propTuple)

		case respTuple := <-rn.reqVoteRespCh:
			rn.handleRequestVoteResponse(respTuple.target, respTuple.resp, respTuple.err)

		case respTuple := <-rn.appendEntriesRespCh:
			rn.handleAppendEntriesResponse(respTuple.target, respTuple.req, respTuple.resp, respTuple.err)
		}
	}
}

func (rn *RaftNode) becomeFollower(term uint64) {
	rn.logger.Printf("Becoming Follower in term %d", term)
	rn.state = Follower
	rn.currentTerm = term
	rn.votedFor = ""
	rn.persist()
	rn.heartbeatTimer.Stop()
	rn.resetElectionTimer()
}

func (rn *RaftNode) becomeCandidate() {
	rn.state = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id
	rn.persist()
	rn.logger.Printf("Becoming Candidate in term %d", rn.currentTerm)
	rn.resetElectionTimer()
	rn.heartbeatTimer.Stop()
}

func (rn *RaftNode) becomeLeader() {
	rn.logger.Printf("Becoming Leader in term %d", rn.currentTerm)
	rn.state = Leader
	rn.heartbeatTimer.Reset(rn.cfg.HeartbeatInterval)
	rn.electionTimer.Stop()

	// Reinitialize volatile state on leaders (§5.3)
	lastIndex := rn.log.LastIndex()
	for _, peer := range rn.peers {
		rn.nextIndex[peer] = lastIndex + 1
		rn.matchIndex[peer] = 0
	}

	rn.sendHeartbeats()
}

func (rn *RaftNode) startElection() {
	rn.becomeCandidate()

	req := &RequestVoteRequest{
		Term:         rn.currentTerm,
		CandidateID:  rn.id,
		LastLogIndex: rn.log.LastIndex(),
		LastLogTerm:  rn.log.LastTerm(),
	}

	rn.votesReceived = 1 // Vote for self

	for _, peer := range rn.peers {
		go func(target string) {
			resp, err := rn.transport.SendRequestVote(target, req)
			rn.reqVoteRespCh <- requestVoteRespTuple{
				target: target,
				resp:   resp,
				err:    err,
			}
		}(peer)
	}
}

func (rn *RaftNode) handleRequestVoteResponse(target string, resp *RequestVoteResponse, err error) {
	if err != nil {
		rn.logger.Printf("RequestVote to %s failed: %v", target, err)
		return
	}

	if rn.state != Candidate {
		return
	}

	if resp.Term > rn.currentTerm {
		rn.becomeFollower(resp.Term)
		return
	}

	if resp.VoteGranted {
		rn.votesReceived++
		if rn.votesReceived >= rn.cfg.MajoritySize() {
			rn.becomeLeader()
		}
	}
}

// Internal method to handle RPC safely within the event loop
func (rn *RaftNode) handleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	resp := &RequestVoteResponse{
		Term:        rn.currentTerm,
		VoteGranted: false,
	}

	if req.Term > rn.currentTerm {
		rn.becomeFollower(req.Term)
	}

	if req.Term < rn.currentTerm {
		return resp
	}

	// Determine if candidate's log is at least as up-to-date as ours (§5.4.1)
	lastLogIndex := rn.log.LastIndex()
	lastLogTerm := rn.log.LastTerm()
	logIsUpToDate := (req.LastLogTerm > lastLogTerm) ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

	if (rn.votedFor == "" || rn.votedFor == req.CandidateID) && logIsUpToDate {
		rn.votedFor = req.CandidateID
		rn.persist()
		resp.VoteGranted = true
		rn.resetElectionTimer()
	}

	return resp
}

func (rn *RaftNode) handleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	resp := &AppendEntriesResponse{
		Term:    rn.currentTerm,
		Success: false,
	}

	if req.Term > rn.currentTerm {
		rn.becomeFollower(req.Term)
	}

	if req.Term < rn.currentTerm {
		return resp
	}

	// We are receiving from the current leader, so we are a follower
	if rn.state != Follower {
		rn.becomeFollower(req.Term)
	} else {
		rn.resetElectionTimer()
	}

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > rn.log.LastIndex() {
			return resp
		}
		entry, err := rn.log.GetEntry(req.PrevLogIndex)
		if err != nil {
			return resp
		}
		if entry.Term != req.PrevLogTerm {
			return resp
		}
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	// 4. Append any new entries not already in the log
	for i, newEntry := range req.Entries {
		index := newEntry.Index
		if index <= rn.log.LastIndex() {
			entry, _ := rn.log.GetEntry(index)
			if entry.Term != newEntry.Term {
				rn.log.TruncateAfter(index)
				rn.log.Append(req.Entries[i:]...)
				break
			}
		} else {
			rn.log.Append(req.Entries[i:]...)
			break
		}
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if req.LeaderCommit > rn.commitIndex {
		lastNewIndex := req.PrevLogIndex + uint64(len(req.Entries))
		if req.LeaderCommit < lastNewIndex {
			rn.commitIndex = req.LeaderCommit
		} else {
			rn.commitIndex = lastNewIndex
		}
		rn.applyCommitted()
	}

	resp.Success = true
	return resp
}

func (rn *RaftNode) sendHeartbeats() {
	rn.replicateLog()
}

func (rn *RaftNode) replicateLog() {
	for _, peer := range rn.peers {
		nextIdx := rn.nextIndex[peer]
		
		var prevLogIndex, prevLogTerm uint64
		if nextIdx > 1 {
			prevLogIndex = nextIdx - 1
			entry, err := rn.log.GetEntry(prevLogIndex)
			if err == nil {
				prevLogTerm = entry.Term
			}
		}

		entries := rn.log.GetEntries(nextIdx)

		req := &AppendEntriesRequest{
			Term:         rn.currentTerm,
			LeaderID:     rn.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rn.commitIndex,
		}

		go func(target string, request *AppendEntriesRequest) {
			resp, err := rn.transport.SendAppendEntries(target, request)
			rn.appendEntriesRespCh <- appendEntriesRespTuple{
				target: target,
				req:    request,
				resp:   resp,
				err:    err,
			}
		}(peer, req)
	}
}

func (rn *RaftNode) handleAppendEntriesResponse(target string, req *AppendEntriesRequest, resp *AppendEntriesResponse, err error) {
	if err != nil {
		// Log error, let next heartbeat retry
		return
	}

	if rn.state != Leader {
		return
	}

	if resp.Term > rn.currentTerm {
		rn.becomeFollower(resp.Term)
		return
	}

	if resp.Success {
		// Update nextIndex and matchIndex for follower
		newMatchIndex := req.PrevLogIndex + uint64(len(req.Entries))
		if newMatchIndex > rn.matchIndex[target] {
			rn.matchIndex[target] = newMatchIndex
			rn.nextIndex[target] = newMatchIndex + 1
			rn.advanceCommitIndex()
		}
	} else {
		// Decrement nextIndex and retry
		if rn.nextIndex[target] > 1 {
			rn.nextIndex[target]--
			// Optionally we could trigger replicateLog() here to retry immediately,
			// but waiting for the next heartbeat is also fine for a basic implementation.
		}
	}
}

func (rn *RaftNode) advanceCommitIndex() {
	// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
	// and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
	for N := rn.log.LastIndex(); N > rn.commitIndex; N-- {
		entry, err := rn.log.GetEntry(N)
		if err != nil {
			continue
		}
		if entry.Term != rn.currentTerm {
			continue
		}

		matchCount := 1 // self
		for _, peer := range rn.peers {
			if rn.matchIndex[peer] >= N {
				matchCount++
			}
		}

		if matchCount >= rn.cfg.MajoritySize() {
			rn.commitIndex = N
			rn.applyCommitted()
			break
		}
	}
}

func (rn *RaftNode) applyCommitted() {
	for rn.commitIndex > rn.lastApplied {
		rn.lastApplied++
		entry, err := rn.log.GetEntry(rn.lastApplied)
		if err != nil {
			rn.logger.Printf("Failed to get entry %d to apply: %v", rn.lastApplied, err)
			continue
		}
		
		if len(entry.Command) > 0 {
			_, err := rn.stateMachine.Apply(entry.Command)
			if err != nil {
				rn.logger.Printf("Failed to apply entry %d: %v", rn.lastApplied, err)
			}
		}
	}
}

func (rn *RaftNode) handlePropose(tuple proposeTuple) {
	entry := LogEntry{
		Term:    rn.currentTerm,
		Index:   rn.log.LastIndex() + 1,
		Command: tuple.command,
	}
	rn.log.Append(entry)
	
	// Force immediate replication
	rn.replicateLog()
	
	// Normally we would wait for commit, but for basic structure we can just return success or channel
	// We'll leave it up to the caller to wait for apply in a complete implementation
	tuple.resp <- ApplyResult{Error: nil}
}

// HandleRequestVote is called by the transport layer when a RequestVoteRPC is received.
func (rn *RaftNode) HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	respCh := make(chan *RequestVoteResponse)
	rn.reqVoteCh <- requestVoteTuple{req: req, resp: respCh}
	return <-respCh
}

// HandleAppendEntries is called by the transport layer when an AppendEntriesRPC is received.
func (rn *RaftNode) HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	respCh := make(chan *AppendEntriesResponse)
	rn.appendEntriesCh <- appendEntriesTuple{req: req, resp: respCh}
	return <-respCh
}

// Propose submits a new command to the Raft cluster.
func (rn *RaftNode) Propose(command []byte) <-chan ApplyResult {
	respCh := make(chan ApplyResult, 1)
	rn.proposeCh <- proposeTuple{command: command, resp: respCh}
	return respCh
}

func (rn *RaftNode) Leader() string {
	return rn.votedFor // Simplified
}

func (rn *RaftNode) IsLeader() bool {
	return rn.state == Leader
}

func (rn *RaftNode) NodeID() string {
	return rn.id
}

func (rn *RaftNode) State() NodeState {
	return rn.state
}

func (rn *RaftNode) Term() uint64 {
	return rn.currentTerm
}

func (rn *RaftNode) CommitIndex() uint64 {
	return rn.commitIndex
}

func (rn *RaftNode) LastApplied() uint64 {
	return rn.lastApplied
}
