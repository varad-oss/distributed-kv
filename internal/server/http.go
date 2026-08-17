package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/varad/distributed-kv/internal/raft"
)

type RaftProposer interface {
	Propose(cmd []byte) <-chan raft.ApplyResult
	Leader() string
	IsLeader() bool
	NodeID() string
	State() raft.NodeState
	Term() uint64
	CommitIndex() uint64
	LastApplied() uint64
}

type KVGetter interface {
	Get(key string) ([]byte, error)
}

type HTTPServer struct {
	addr        string
	raftNode    RaftProposer
	kvGetter    KVGetter
	peerHTTPMap map[string]string // maps nodeID to HTTP address
}

func NewHTTPServer(addr string, raftNode RaftProposer, kvGetter KVGetter, peerHTTPMap map[string]string) *HTTPServer {
	return &HTTPServer{
		addr:        addr,
		raftNode:    raftNode,
		kvGetter:    kvGetter,
		peerHTTPMap: peerHTTPMap,
	}
}

func (s *HTTPServer) getLeaderAddr() string {
	leaderID := s.raftNode.Leader()
	if leaderID == "" || leaderID == s.raftNode.NodeID() {
		return ""
	}
	return s.peerHTTPMap[leaderID]
}

func (s *HTTPServer) redirectOrError(w http.ResponseWriter, r *http.Request) bool {
	if s.raftNode.IsLeader() {
		return false
	}
	
	leaderAddr := s.getLeaderAddr()
	if leaderAddr == "" {
		http.Error(w, "no leader available", http.StatusServiceUnavailable)
		return true
	}

	// If the leader address is just a port (e.g., ":8003"), prepend the
	// hostname from the incoming request so the redirect URL is valid.
	if strings.HasPrefix(leaderAddr, ":") {
		host := r.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		if host == "" {
			host = "localhost"
		}
		leaderAddr = host + leaderAddr
	}

	url := fmt.Sprintf("http://%s%s", leaderAddr, r.URL.Path)
	http.Redirect(w, r, url, http.StatusTemporaryRedirect)
	return true
}

func (s *HTTPServer) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	if r.Method == http.MethodGet {
		if s.redirectOrError(w, r) {
			return
		}

		val, err := s.kvGetter.Get(key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if val == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Write(val)
		return
	}

	if r.Method == http.MethodPut {
		if s.redirectOrError(w, r) {
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}

		cmd := Command{
			Op:    "SET",
			Key:   key,
			Value: body,
		}
		data, err := SerializeCommand(cmd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resCh := s.raftNode.Propose(data)
		res := <-resCh
		if res.Error != nil {
			http.Error(w, res.Error.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method == http.MethodDelete {
		if s.redirectOrError(w, r) {
			return
		}

		cmd := Command{
			Op:  "DELETE",
			Key: key,
		}
		data, err := SerializeCommand(cmd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resCh := s.raftNode.Propose(data)
		res := <-resCh
		if res.Error != nil {
			http.Error(w, res.Error.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		return
	}

	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func (s *HTTPServer) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	status := map[string]interface{}{
		"node_id":      s.raftNode.NodeID(),
		"state":        s.raftNode.State().String(),
		"term":         s.raftNode.Term(),
		"leader":       s.raftNode.Leader(),
		"commit_index": s.raftNode.CommitIndex(),
		"last_applied": s.raftNode.LastApplied(),
		"peers":        s.peerHTTPMap,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (s *HTTPServer) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", s.handleKV)
	mux.HandleFunc("/cluster/status", s.handleClusterStatus)

	server := &http.Server{
		Addr:    s.addr,
		Handler: corsMiddleware(mux),
	}

	return server.ListenAndServe()
}
