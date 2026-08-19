package raft

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

// network is a deterministic, in-memory cluster used to drive the consensus
// core from tests. No sockets, no goroutines, no wall clock: messages move
// only when the test says so, so an election or a partition plays out
// identically on every run and a failure is always reproducible.
type network struct {
	t     *testing.T
	nodes map[NodeID]*Node
	order []NodeID

	// group assigns each node to a partition. Nodes in different groups cannot
	// exchange messages.
	group map[NodeID]int
	// applied records what each node's state machine was handed, so tests can
	// assert that replicas converged.
	applied map[NodeID][]Entry
	// dropAll silences a node entirely, in either direction.
	down map[NodeID]bool
}

type networkOpts struct {
	learners        []NodeID
	preVoteDisabled bool
	electionTicks   int
	seed            uint64
}

func newNetwork(t *testing.T, ids ...NodeID) *network {
	return newNetworkWith(t, networkOpts{}, ids...)
}

func newNetworkWith(t *testing.T, opts networkOpts, ids ...NodeID) *network {
	t.Helper()
	if opts.electionTicks == 0 {
		opts.electionTicks = 10
	}

	learner := map[NodeID]bool{}
	for _, id := range opts.learners {
		learner[id] = true
	}
	cfg := Config{}
	for _, id := range ids {
		cfg = cfg.With(Member{ID: id, Addr: fmt.Sprintf("mem://%d", uint64(id)), Learner: learner[id]})
	}

	nw := &network{
		t:       t,
		nodes:   make(map[NodeID]*Node, len(ids)),
		order:   append([]NodeID(nil), ids...),
		group:   make(map[NodeID]int, len(ids)),
		applied: make(map[NodeID][]Entry, len(ids)),
		down:    make(map[NodeID]bool, len(ids)),
	}
	for _, id := range ids {
		nw.nodes[id] = NewNode(Options{
			ID:              id,
			Config:          cfg,
			ElectionTicks:   opts.electionTicks,
			HeartbeatTicks:  1,
			PreVoteDisabled: opts.preVoteDisabled,
			Rand:            rand.New(rand.NewPCG(opts.seed+1, uint64(id))),
		})
		nw.group[id] = 0
	}
	return nw
}

func (nw *network) node(id NodeID) *Node { return nw.nodes[id] }

func (nw *network) reachable(from, to NodeID) bool {
	if _, known := nw.nodes[to]; !known {
		// A server the test has not started yet. On a real network this is a
		// connection to an address that refuses it, which is exactly what
		// adding a member before booting it looks like.
		return false
	}
	if nw.down[from] || nw.down[to] {
		return false
	}
	return nw.group[from] == nw.group[to]
}

// isolate cuts a node off from everyone, the way a network partition or a
// hung process looks to the rest of the cluster.
func (nw *network) isolate(ids ...NodeID) {
	next := 1
	for _, g := range nw.group {
		if g >= next {
			next = g + 1
		}
	}
	for _, id := range ids {
		nw.group[id] = next
		next++
	}
}

// split partitions the cluster into two groups that can talk internally but
// not across.
func (nw *network) split(a []NodeID, b []NodeID) {
	for _, id := range a {
		nw.group[id] = 1
	}
	for _, id := range b {
		nw.group[id] = 2
	}
}

// heal puts everyone back on the same side of the network.
func (nw *network) heal() {
	for id := range nw.group {
		nw.group[id] = 0
		nw.down[id] = false
	}
}

// stop makes a node deaf and mute without destroying its state, which is what
// a crashed-but-not-yet-restarted process looks like.
func (nw *network) stop(id NodeID)  { nw.down[id] = true }
func (nw *network) start(id NodeID) { nw.down[id] = false }

// deliver runs the cluster until no node has any pending work. Messages are
// routed in a fixed order so a run is fully reproducible.
func (nw *network) deliver() {
	for round := 0; round < 200; round++ {
		busy := false
		for _, id := range nw.order {
			n := nw.nodes[id]
			if !n.HasReady() {
				continue
			}
			busy = true
			rd := n.Ready()
			if len(rd.CommittedEntries) > 0 {
				nw.applied[id] = append(nw.applied[id], rd.CommittedEntries...)
			}
			for _, m := range rd.Messages {
				if !nw.reachable(id, m.To) {
					continue
				}
				if err := nw.nodes[m.To].Step(m); err != nil {
					nw.t.Fatalf("step %v on %d: %v", m, uint64(m.To), err)
				}
			}
		}
		if !busy {
			return
		}
	}
	nw.t.Fatal("network did not quiesce after 200 rounds")
}

// tick advances every node by n ticks, delivering messages after each one.
func (nw *network) tick(n int) {
	for i := 0; i < n; i++ {
		for _, id := range nw.order {
			if nw.down[id] {
				continue
			}
			nw.nodes[id].Tick()
		}
		nw.deliver()
	}
}

// tickUntilLeader ticks until some node reaches leadership, and fails the test
// if that never happens.
func (nw *network) tickUntilLeader(maxTicks int) *Node {
	nw.t.Helper()
	for i := 0; i < maxTicks; i++ {
		nw.tick(1)
		if l := nw.leader(); l != nil {
			return l
		}
	}
	nw.t.Fatalf("no leader elected within %d ticks", maxTicks)
	return nil
}

// leader returns the single reachable leader, or nil. It fails the test if two
// nodes in the same partition both think they are leader in the same term,
// which would be an election safety violation.
func (nw *network) leader() *Node {
	var found *Node
	for _, id := range nw.order {
		n := nw.nodes[id]
		if n.Role() != Leader || nw.down[id] {
			continue
		}
		if found != nil && found.Term() == n.Term() {
			nw.t.Fatalf("two leaders in term %d: %d and %d", n.Term(), uint64(found.ID()), uint64(id))
		}
		if found == nil || n.Term() > found.Term() {
			found = n
		}
	}
	return found
}

func (nw *network) campaign(id NodeID) {
	nw.nodes[id].Campaign()
	nw.deliver()
}

func (nw *network) mustLeader() *Node {
	nw.t.Helper()
	l := nw.leader()
	if l == nil {
		nw.t.Fatal("expected a leader, found none")
	}
	return l
}

// propose submits a command through the given node and lets the cluster settle.
func (nw *network) propose(id NodeID, cmd string) (uint64, error) {
	idx, err := nw.nodes[id].Propose(EntryNormal, []byte(cmd))
	if err != nil {
		return 0, err
	}
	nw.deliver()
	return idx, nil
}

// commands returns the payloads a node's state machine was handed, skipping
// the bookkeeping entries the algorithm writes for itself.
func (nw *network) commands(id NodeID) []string {
	var out []string
	for _, e := range nw.applied[id] {
		if e.Type == EntryNormal {
			out = append(out, string(e.Data))
		}
	}
	return out
}

// assertConverged checks that every reachable node holds the same log and has
// applied the same commands -- the property the whole algorithm exists for.
func (nw *network) assertConverged(ids ...NodeID) {
	nw.t.Helper()
	var ref NodeID
	for _, id := range ids {
		if ref == 0 {
			ref = id
			continue
		}
		a, b := nw.nodes[ref], nw.nodes[id]
		if a.log.LastIndex() != b.log.LastIndex() {
			nw.t.Fatalf("nodes %d and %d disagree on last index: %d vs %d",
				uint64(ref), uint64(id), a.log.LastIndex(), b.log.LastIndex())
		}
		for i := a.log.FirstIndex(); i <= a.log.LastIndex(); i++ {
			ta, _ := a.Log().Term(i)
			tb, _ := b.Log().Term(i)
			if ta != tb {
				nw.t.Fatalf("nodes %d and %d disagree at index %d: term %d vs %d",
					uint64(ref), uint64(id), i, ta, tb)
			}
		}
		ca, cb := nw.commands(ref), nw.commands(id)
		if len(ca) != len(cb) {
			nw.t.Fatalf("nodes %d and %d applied different commands: %v vs %v", uint64(ref), uint64(id), ca, cb)
		}
		for i := range ca {
			if ca[i] != cb[i] {
				nw.t.Fatalf("nodes %d and %d diverged at command %d: %q vs %q", uint64(ref), uint64(id), i, ca[i], cb[i])
			}
		}
	}
}

// join adds a brand new server to the running network. It is created with the
// configuration the cluster is about to adopt, which is what the daemon's
// --join flag does in practice: a joining server is told who its peers are, it
// does not have to divine it from an empty log.
func (nw *network) join(id NodeID, cfg Config) *Node {
	nw.t.Helper()
	n := NewNode(Options{
		ID:             id,
		Config:         cfg,
		ElectionTicks:  10,
		HeartbeatTicks: 1,
		Rand:           rand.New(rand.NewPCG(99, uint64(id))),
	})
	nw.nodes[id] = n
	nw.order = append(nw.order, id)
	nw.group[id] = 0
	return n
}

// confChange proposes a membership edit through the leader and settles.
func (nw *network) confChange(leader NodeID, cc ConfChange) error {
	if _, err := nw.nodes[leader].ProposeConfChange(cc); err != nil {
		return err
	}
	nw.deliver()
	return nil
}
