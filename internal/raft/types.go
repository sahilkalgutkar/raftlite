// Package raft implements the Raft consensus algorithm as a deterministic
// state machine: it owns no sockets, no files and no clocks. Time enters
// through Tick, the network enters through Step, and everything the outside
// world has to do on the algorithm's behalf leaves through Ready.
//
// I split it this way so the hard part -- the consensus rules -- can be tested
// with ordinary table-driven tests and a simulated network, instead of only
// through flaky multi-process integration runs.
package raft

import "fmt"

// NodeID identifies a single member of a cluster. Zero is reserved to mean
// "no node", which is how an unset vote or an unknown leader is represented.
type NodeID uint64

// None is the zero NodeID: no vote cast, no leader known.
const None NodeID = 0

func (id NodeID) String() string { return fmt.Sprintf("node-%d", uint64(id)) }

// Role is the part a node is currently playing in the protocol.
type Role uint8

const (
	// Follower is the passive role: it only responds to leaders and candidates.
	Follower Role = iota
	// PreCandidate runs the pre-vote round from section 9.6 of the Raft
	// dissertation. It asks "would you vote for me?" without bumping the term,
	// so a partitioned node rejoining the cluster cannot force a term change
	// and depose a perfectly healthy leader.
	PreCandidate
	// Candidate is a node running a real election in a new term.
	Candidate
	// Leader is the single node accepting proposals for the current term.
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case PreCandidate:
		return "pre-candidate"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return fmt.Sprintf("role(%d)", uint8(r))
	}
}

// EntryType tags what a log entry means once it is committed.
type EntryType uint8

const (
	// EntryNormal carries an opaque command for the state machine.
	EntryNormal EntryType = iota
	// EntryNoOp is the blank entry a new leader appends to its own term. It
	// carries no command; its job is to get an entry from the current term
	// committed, which is what makes the leader's commit index trustworthy.
	EntryNoOp
	// EntryConfChange carries a marshalled ConfChange: a membership edit that
	// is replicated through the log like any other write.
	EntryConfChange
)

func (t EntryType) String() string {
	switch t {
	case EntryNormal:
		return "normal"
	case EntryNoOp:
		return "noop"
	case EntryConfChange:
		return "confchange"
	default:
		return fmt.Sprintf("entry(%d)", uint8(t))
	}
}

// Entry is one slot in the replicated log.
type Entry struct {
	Term  uint64
	Index uint64
	Type  EntryType
	Data  []byte
}

func (e Entry) String() string {
	return fmt.Sprintf("{idx=%d term=%d type=%s len=%d}", e.Index, e.Term, e.Type, len(e.Data))
}

// HardState is the slice of a node's state that must survive a crash. Losing
// any of these three fields can break Raft's safety guarantees, so they are
// fsync'd before any message that depends on them goes out on the wire.
type HardState struct {
	Term   uint64
	Vote   NodeID
	Commit uint64
}

// IsEmpty reports whether the state carries nothing worth persisting.
func (h HardState) IsEmpty() bool {
	return h.Term == 0 && h.Vote == None && h.Commit == 0
}

// Equal compares two hard states field by field.
func (h HardState) Equal(o HardState) bool {
	return h.Term == o.Term && h.Vote == o.Vote && h.Commit == o.Commit
}

func (h HardState) String() string {
	return fmt.Sprintf("{term=%d vote=%d commit=%d}", h.Term, uint64(h.Vote), h.Commit)
}

// SnapshotMeta describes the log prefix a snapshot replaces.
type SnapshotMeta struct {
	Index  uint64
	Term   uint64
	Config Config
}

// Snapshot is a point-in-time image of the state machine plus the metadata
// needed to splice it into a follower's log.
type Snapshot struct {
	Meta SnapshotMeta
	Data []byte
}

// IsEmpty reports whether the snapshot covers no entries at all.
func (s *Snapshot) IsEmpty() bool { return s == nil || s.Meta.Index == 0 }
