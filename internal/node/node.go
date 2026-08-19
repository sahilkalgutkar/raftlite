// Package node is the runtime that turns the consensus state machine into a
// running server. It owns the event loop and everything with a side effect:
// the clock, the disk, the sockets and the key-value store.
//
// One goroutine drives all of it. raft.Node has no locks by design, so instead
// of guarding every field, every path into it goes through a channel and is
// serviced by the loop. Concurrent readers -- HTTP handlers asking for status
// or a key -- read from an atomically published snapshot rather than reaching
// into the algorithm.
package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sahilkalgutkar/raftlite/internal/fsm"
	"github.com/sahilkalgutkar/raftlite/internal/metrics"
	"github.com/sahilkalgutkar/raftlite/internal/raft"
	"github.com/sahilkalgutkar/raftlite/internal/storage"
	"github.com/sahilkalgutkar/raftlite/internal/transport"
)

var (
	// ErrStopped is returned once the node has shut down.
	ErrStopped = errors.New("node: stopped")
	// ErrLeadershipLost means a proposal was accepted by a leader that was
	// deposed before the entry committed. The write may or may not have
	// happened, which is exactly why the caller has to be told rather than
	// left waiting.
	ErrLeadershipLost = errors.New("node: leadership lost before the write committed")
)

// Config describes one server.
type Config struct {
	ID   raft.NodeID
	Addr string // address peers reach this node on
	Dir  string // directory for the log and snapshots

	// Peers is the cluster membership this node starts with. A founding
	// member lists everyone; a server joining an existing cluster lists the
	// current members plus itself.
	Peers []raft.Member
	// Bootstrap makes this node call the first election instead of waiting out
	// an election timeout. Exactly one founding member should set it.
	Bootstrap bool

	TickInterval   time.Duration
	ElectionTicks  int
	HeartbeatTicks int

	// SnapshotThreshold is how many applied entries trigger a snapshot. Zero
	// disables automatic snapshotting.
	SnapshotThreshold uint64

	// Metrics is where this node registers its counters and gauges. One is
	// created if none is supplied, so a node always has somewhere to record.
	Metrics *metrics.Registry

	// NewTransport builds this node's transport. It is a constructor rather
	// than a value because the transport needs the node's message handler,
	// which does not exist until the node does. Leaving it nil gets the real
	// TCP transport; tests pass an in-memory mesh.
	NewTransport func(transport.Handler) (transport.Transport, error)
	NoSync       bool
	Logger       *slog.Logger
}

func (c *Config) withDefaults() {
	if c.TickInterval <= 0 {
		c.TickInterval = 100 * time.Millisecond
	}
	if c.ElectionTicks <= 0 {
		c.ElectionTicks = 10
	}
	if c.HeartbeatTicks <= 0 {
		c.HeartbeatTicks = 1
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = 1000
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
}

// proposal is one client write waiting for its entry to commit and apply.
type proposal struct {
	entryType raft.EntryType
	data      []byte
	conf      *raft.ConfChange

	// term is the term the entry was appended in. If the entry that shows up
	// at this index carries a different term, a new leader overwrote ours and
	// the write did not happen.
	term   uint64
	result chan proposalResult
}

type proposalResult struct {
	res fsm.Result
	err error
}

// readRequest is a client asking for a linearizable read.
type readRequest struct {
	result chan error
}

// readWaiter is that request once the loop has taken it on: it needs a quorum
// to confirm the leader, and then the state machine to catch up to the index
// the leader named.
type readWaiter struct {
	index     uint64
	confirmed bool
	result    chan error
}

// Node is a running raftlite server.
type Node struct {
	cfg   Config
	raft  *raft.Node
	store *storage.Store
	kv    *fsm.KV
	tr    transport.Transport

	recvCh chan raft.Message
	propCh chan *proposal
	readCh chan *readRequest
	stopCh chan struct{}
	doneCh chan struct{}

	stopOnce sync.Once
	runErr   atomic.Pointer[error]
	status   atomic.Pointer[raft.Status]

	// waiters maps a log index to the client blocked on it. Only the event
	// loop touches it.
	waiters map[uint64]*proposal
	// reads maps a read identifier to the client blocked on it, and nextReadID
	// hands out those identifiers. Also loop-only.
	reads      map[uint64]*readWaiter
	nextReadID uint64

	knownConfig          raft.Config
	appliedSinceSnapshot uint64
	bootstrapPending     bool

	metrics        *nodeMetrics
	registry       *metrics.Registry
	lastSeenLeader raft.NodeID
}

// Start recovers a node from disk and begins running it.
func Start(cfg Config) (*Node, error) {
	cfg.withDefaults()

	store, state, err := storage.Open(storage.Options{Dir: cfg.Dir, NoSync: cfg.NoSync, Logger: cfg.Logger})
	if err != nil {
		return nil, err
	}

	kv := fsm.NewKV()
	log := raft.NewLog()
	if state.Snapshot != nil {
		if err := kv.Restore(state.Snapshot.Data); err != nil {
			store.Close()
			return nil, fmt.Errorf("node: restore snapshot: %w", err)
		}
		log = raft.NewLogFrom(state.Snapshot.Meta.Index, state.Snapshot.Meta.Term, state.Entries)
		cfg.Logger.Info("recovered from a snapshot",
			"id", uint64(cfg.ID), "index", state.Snapshot.Meta.Index, "keys", kv.Len())
	} else if len(state.Entries) > 0 {
		log.Append(state.Entries...)
	}

	n := &Node{
		cfg:     cfg,
		store:   store,
		kv:      kv,
		recvCh:  make(chan raft.Message, 512),
		propCh:  make(chan *proposal),
		readCh:  make(chan *readRequest),
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
		waiters: make(map[uint64]*proposal),
		reads:   make(map[uint64]*readWaiter),
	}

	n.raft = raft.NewNode(raft.Options{
		ID:             cfg.ID,
		Config:         raft.NewConfig(cfg.Peers...),
		ElectionTicks:  cfg.ElectionTicks,
		HeartbeatTicks: cfg.HeartbeatTicks,
		HardState:      state.HardState,
		Log:            log,
		Snapshot:       state.Snapshot,
		Logger:         cfg.Logger,
	})
	n.knownConfig = n.raft.Config()
	n.publishStatus()

	// Only bootstrap a genuinely fresh node. A restart that campaigned on
	// startup would interrupt whatever leader the cluster already has.
	n.bootstrapPending = cfg.Bootstrap && state.HardState.IsEmpty() && len(state.Entries) == 0

	newTransport := cfg.NewTransport
	if newTransport == nil {
		newTransport = func(h transport.Handler) (transport.Transport, error) {
			return transport.Listen(transport.Options{
				ID:     cfg.ID,
				Addr:   cfg.Addr,
				Logger: cfg.Logger,
			}, h)
		}
	}
	tr, err := newTransport(n.Receive)
	if err != nil {
		store.Close()
		return nil, err
	}
	n.tr = tr
	n.tr.SetPeers(n.knownConfig.Members)

	n.registry = cfg.Metrics
	if n.registry == nil {
		n.registry = metrics.NewRegistry()
	}
	n.registerMetrics(n.registry)

	go n.run()
	return n, nil
}

// Receive hands an inbound message to the event loop. The transport calls it
// from its own goroutines, so it must never block for long: a full queue drops
// the message, which the algorithm recovers from by retrying.
func (n *Node) Receive(m raft.Message) {
	select {
	case n.recvCh <- m:
	case <-n.stopCh:
	default:
		n.cfg.Logger.Warn("dropping an inbound message: the node is behind",
			"id", uint64(n.cfg.ID), "type", m.Type.String())
	}
}

// Stop shuts the node down and waits for the event loop to finish.
func (n *Node) Stop() error {
	n.stopOnce.Do(func() { close(n.stopCh) })
	<-n.doneCh
	if err := n.runErr.Load(); err != nil && *err != nil {
		return *err
	}
	return nil
}

// Done is closed when the node has stopped, whether by request or by failure.
func (n *Node) Done() <-chan struct{} { return n.doneCh }

// Addr is the address peers use to reach this node.
func (n *Node) Addr() string { return n.tr.Addr() }

// Status returns the most recently published view of the node's state. It is
// safe to call from any goroutine.
func (n *Node) Status() raft.Status {
	if s := n.status.Load(); s != nil {
		return *s
	}
	return raft.Status{ID: n.cfg.ID}
}

// Store exposes the state machine for reads.
func (n *Node) Store() *fsm.KV { return n.kv }

// Metrics returns the registry this node exports through.
func (n *Node) Metrics() *metrics.Registry { return n.registry }

// IsLeader reports whether this node is currently leading.
func (n *Node) IsLeader() bool { return n.Status().Role == raft.Leader }

// LeaderAddr returns the peer address of the current leader, or "" if unknown.
func (n *Node) LeaderAddr() string {
	if m, ok := n.leaderMember(); ok {
		return m.Addr
	}
	return ""
}

// LeaderClientAddr returns the address clients should be redirected to, or ""
// if there is no known leader.
func (n *Node) LeaderClientAddr() string {
	if m, ok := n.leaderMember(); ok {
		return m.ClientAddr
	}
	return ""
}

func (n *Node) leaderMember() (raft.Member, bool) {
	st := n.Status()
	if st.Leader == raft.None {
		return raft.Member{}, false
	}
	return st.Config.Member(st.Leader)
}

// Propose replicates a state machine command and waits for it to be applied.
func (n *Node) Propose(ctx context.Context, cmd fsm.Command) (fsm.Result, error) {
	return n.submit(ctx, &proposal{
		entryType: raft.EntryNormal,
		data:      cmd.Marshal(),
		result:    make(chan proposalResult, 1),
	})
}

// ProposeConfChange replicates a membership change and waits for it to apply.
func (n *Node) ProposeConfChange(ctx context.Context, cc raft.ConfChange) error {
	_, err := n.submit(ctx, &proposal{
		entryType: raft.EntryConfChange,
		conf:      &cc,
		result:    make(chan proposalResult, 1),
	})
	return err
}

func (n *Node) submit(ctx context.Context, p *proposal) (fsm.Result, error) {
	select {
	case n.propCh <- p:
	case <-ctx.Done():
		return fsm.Result{}, ctx.Err()
	case <-n.stopCh:
		return fsm.Result{}, ErrStopped
	case <-n.doneCh:
		return fsm.Result{}, ErrStopped
	}

	select {
	case out := <-p.result:
		return out.res, out.err
	case <-ctx.Done():
		// The entry may still commit. Giving up here only abandons the wait,
		// which is the honest thing to report: a cancelled write is unknown,
		// not undone.
		return fsm.Result{}, ctx.Err()
	case <-n.doneCh:
		return fsm.Result{}, ErrStopped
	}
}

// AddMember adds a server to the cluster. New servers join as learners so a
// cold replica can catch up without being counted in any quorum; promote it
// once it is close to the leader.
func (n *Node) AddMember(ctx context.Context, id raft.NodeID, addr, clientAddr string, voting bool) error {
	typ := raft.ConfChangeAddLearner
	if voting {
		typ = raft.ConfChangeAddVoter
	}
	return n.ProposeConfChange(ctx, raft.ConfChange{Type: typ, ID: id, Addr: addr, ClientAddr: clientAddr})
}

// PromoteMember turns a learner into a voter.
func (n *Node) PromoteMember(ctx context.Context, id raft.NodeID) error {
	return n.ProposeConfChange(ctx, raft.ConfChange{Type: raft.ConfChangePromote, ID: id})
}

// RemoveMember drops a server from the cluster.
func (n *Node) RemoveMember(ctx context.Context, id raft.NodeID) error {
	return n.ProposeConfChange(ctx, raft.ConfChange{Type: raft.ConfChangeRemove, ID: id})
}

// Members returns the cluster configuration as this node understands it.
func (n *Node) Members() []raft.Member { return n.Status().Config.Members }

// LinearizableRead blocks until this node can answer a read that is guaranteed
// to reflect every write committed before the call began.
//
// It is not free -- it costs a heartbeat round trip -- but it is far cheaper
// than replicating a log entry per read, and it is the only way to be sure the
// answer is not coming from a leader that was deposed while it was not
// looking.
func (n *Node) LinearizableRead(ctx context.Context) error {
	r := &readRequest{result: make(chan error, 1)}
	select {
	case n.readCh <- r:
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopCh:
		return ErrStopped
	case <-n.doneCh:
		return ErrStopped
	}

	select {
	case err := <-r.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-n.doneCh:
		return ErrStopped
	}
}

// Get reads a key. A linearizable read confirms leadership first; a stale one
// answers straight from local state, which any node can do and which is
// perfectly reasonable for a caller that does not need the newest value.
func (n *Node) Get(ctx context.Context, key string, linearizable bool) (fsm.Value, bool, error) {
	if linearizable {
		if err := n.LinearizableRead(ctx); err != nil {
			return fsm.Value{}, false, err
		}
	}
	v, ok := n.kv.Get(key)
	return v, ok, nil
}
