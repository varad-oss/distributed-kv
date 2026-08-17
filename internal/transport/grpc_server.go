package transport

import (
	"fmt"
	"net"
	"net/rpc"

	"github.com/varad/distributed-kv/internal/raft"
)

type RaftRPCHandler interface {
	HandleAppendEntries(req *raft.AppendEntriesRequest) *raft.AppendEntriesResponse
	HandleRequestVote(req *raft.RequestVoteRequest) *raft.RequestVoteResponse
	// HandleInstallSnapshot(req *raft.InstallSnapshotRequest) *raft.InstallSnapshotResponse
}

type RaftGRPCServer struct {
	handler RaftRPCHandler
}

func NewRaftGRPCServer(handler RaftRPCHandler) *RaftGRPCServer {
	return &RaftGRPCServer{handler: handler}
}

func (s *RaftGRPCServer) HandleAppendEntries(req *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	res := s.handler.HandleAppendEntries(req)
	*resp = *res
	return nil
}

func (s *RaftGRPCServer) HandleRequestVote(req *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	res := s.handler.HandleRequestVote(req)
	*resp = *res
	return nil
}

func (s *RaftGRPCServer) HandleInstallSnapshot(req *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse) error {
	return fmt.Errorf("not implemented")
}

func (s *RaftGRPCServer) Serve(addr string) error {
	server := rpc.NewServer()
	err := server.Register(s)
	if err != nil {
		return err
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		go server.ServeConn(conn)
	}
}
