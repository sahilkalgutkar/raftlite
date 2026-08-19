package server

import "github.com/sahilkalgutkar/raftlite/internal/raft"

// Short aliases so the tests read as tests rather than as import paths.
type member = raft.Member

func nodeID(v uint64) raft.NodeID { return raft.NodeID(v) }
