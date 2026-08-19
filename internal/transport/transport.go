// Package transport moves Raft messages between nodes over TCP.
//
// The contract it owes the consensus layer is narrower than it looks. Raft
// already assumes the network can drop, delay, duplicate and reorder anything,
// and every message is retried by the algorithm itself -- an unanswered append
// comes back on the next heartbeat, a lost vote is re-solicited by the next
// election. So this layer is deliberately best effort: it never blocks the
// consensus goroutine, never queues without bound, and never reports delivery.
//
// The failure it does have to avoid is head-of-line blocking. One unreachable
// follower must not be able to stall the leader, so each peer gets its own
// queue and its own goroutine, and a full queue drops the message rather than
// applying back pressure to the algorithm.
package transport

import (
	"errors"
	"sync/atomic"

	"github.com/sahilkalgutkar/raftlite/internal/raft"
)

// Handler receives messages that arrived from other nodes.
type Handler func(raft.Message)

// Transport is what the node runtime needs from the network. Keeping it an
// interface is what lets the integration tests swap in an in-memory mesh with
// controllable partitions.
type Transport interface {
	// Send delivers a message on a best-effort basis. It never blocks and
	// never returns a delivery guarantee.
	Send(msg raft.Message)
	// SetPeers updates the address book after a membership change.
	SetPeers(members []raft.Member)
	// Addr is the address this transport listens on.
	Addr() string
	// Close stops listening and disconnects every peer.
	Close() error
}

// ErrClosed is returned by operations on a transport that has shut down.
var ErrClosed = errors.New("transport: closed")

// Stats are cheap counters for the metrics endpoint and for tests that need to
// assert a message was dropped rather than delivered.
type Stats struct {
	Sent        atomic.Uint64
	Dropped     atomic.Uint64
	Received    atomic.Uint64
	DialFailed  atomic.Uint64
	Connections atomic.Int64
}

// Snapshot returns a plain copy of the counters.
func (s *Stats) Snapshot() StatsSnapshot {
	return StatsSnapshot{
		Sent:        s.Sent.Load(),
		Dropped:     s.Dropped.Load(),
		Received:    s.Received.Load(),
		DialFailed:  s.DialFailed.Load(),
		Connections: s.Connections.Load(),
	}
}

// StatsSnapshot is a point-in-time copy of Stats.
type StatsSnapshot struct {
	Sent        uint64
	Dropped     uint64
	Received    uint64
	DialFailed  uint64
	Connections int64
}
