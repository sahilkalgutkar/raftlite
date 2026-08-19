package chaos

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sahilkalgutkar/raftlite/internal/fsm"
	"github.com/sahilkalgutkar/raftlite/internal/node"
	"github.com/sahilkalgutkar/raftlite/internal/raft"
	"github.com/sahilkalgutkar/raftlite/internal/transport"
)

// cluster is a set of real nodes on a simulated network.
type cluster struct {
	t    *testing.T
	mesh *transport.Mesh

	mu    sync.Mutex
	nodes map[raft.NodeID]*node.Node

	dirs  map[raft.NodeID]string
	peers []raft.Member
	ids   []raft.NodeID
	tune  func(*node.Config)
}

func newCluster(t *testing.T, size int, tune func(*node.Config)) *cluster {
	t.Helper()
	c := &cluster{
		t:     t,
		mesh:  transport.NewMesh(),
		nodes: make(map[raft.NodeID]*node.Node),
		dirs:  make(map[raft.NodeID]string),
		tune:  tune,
	}

	base := t.TempDir()
	for i := 1; i <= size; i++ {
		id := raft.NodeID(i)
		c.ids = append(c.ids, id)
		c.dirs[id] = filepath.Join(base, fmt.Sprintf("node%d", i))
		c.peers = append(c.peers, raft.Member{
			ID:         id,
			Addr:       fmt.Sprintf("mem://%d", i),
			ClientAddr: fmt.Sprintf("mem://%d-client", i),
		})
	}
	for _, id := range c.ids {
		c.startNode(id, id == 1)
	}
	t.Cleanup(c.stopAll)
	return c
}

func (c *cluster) startNode(id raft.NodeID, bootstrap bool) {
	c.t.Helper()
	cfg := node.Config{
		ID:                id,
		Addr:              fmt.Sprintf("mem://%d", uint64(id)),
		Dir:               c.dirs[id],
		Peers:             c.peers,
		Bootstrap:         bootstrap,
		TickInterval:      5 * time.Millisecond,
		ElectionTicks:     10,
		HeartbeatTicks:    1,
		SnapshotThreshold: 1 << 30,
		NewTransport:      c.mesh.Factory(id, fmt.Sprintf("mem://%d", uint64(id))),
		// These tests kill processes, not machines: the log still has to
		// survive a restart, it just does not have to survive a power cut, and
		// an fsync per write would make a thousand-write chaos run glacial.
		NoSync: true,
		Logger: slog.New(slog.DiscardHandler),
	}
	if c.tune != nil {
		c.tune(&cfg)
	}
	n, err := node.Start(cfg)
	if err != nil {
		c.t.Fatalf("start node %d: %v", uint64(id), err)
	}
	c.mu.Lock()
	c.nodes[id] = n
	c.mu.Unlock()
}

func (c *cluster) stopAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, n := range c.nodes {
		_ = n.Stop()
		delete(c.nodes, id)
	}
}

func (c *cluster) node(id raft.NodeID) *node.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

func (c *cluster) running() []*node.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*node.Node, 0, len(c.nodes))
	for _, id := range c.ids {
		if n, ok := c.nodes[id]; ok {
			out = append(out, n)
		}
	}
	return out
}

// crash stops a node without warning anyone. Its data directory stays, which
// is what makes restart meaningful.
func (c *cluster) crash(id raft.NodeID) {
	c.mu.Lock()
	n, ok := c.nodes[id]
	delete(c.nodes, id)
	c.mu.Unlock()
	if ok {
		_ = n.Stop()
	}
}

// restart brings a crashed node back from its own log.
func (c *cluster) restart(id raft.NodeID) {
	c.mu.Lock()
	_, alive := c.nodes[id]
	c.mu.Unlock()
	if alive {
		return
	}
	c.startNode(id, false)
}

func (c *cluster) isRunning(id raft.NodeID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.nodes[id]
	return ok
}

// leader returns the current leader, waiting for one to appear.
func (c *cluster) leader() *node.Node {
	c.t.Helper()
	n := c.leaderWithin(20 * time.Second)
	if n == nil {
		c.t.Fatal("no leader emerged")
	}
	return n
}

func (c *cluster) leaderWithin(d time.Duration) *node.Node {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		for _, n := range c.running() {
			if n.IsLeader() {
				return n
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	return nil
}

// put writes through whichever node currently leads, retrying while leadership
// moves. It returns nil only when the cluster actually acknowledged the write.
func (c *cluster) put(key, value string, deadline time.Duration) error {
	end := time.Now().Add(deadline)
	var lastErr error
	for time.Now().Before(end) {
		leader := c.leaderWithin(time.Until(end))
		if leader == nil {
			return fmt.Errorf("no leader available: %w", lastErr)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := leader.Propose(ctx, fsm.Put(key, []byte(value)))
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		// Every one of these means "ask again", not "this failed for good".
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, node.ErrLeadershipLost) ||
			errors.Is(err, node.ErrStopped) || errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		return err
	}
	return fmt.Errorf("gave up writing %s: %w", key, lastErr)
}

func (c *cluster) waitFor(what string, d time.Duration, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("timed out waiting for %s", what)
}

// assertAcknowledgedWritesSurvive is the invariant every test in this package
// exists to check. A write that returned success must be readable on every
// running node, with the value that was written.
func (c *cluster) assertAcknowledgedWritesSurvive(acked map[string]string) {
	c.t.Helper()
	for _, n := range c.running() {
		st := n.Status()
		for key, want := range acked {
			v, ok := n.Store().Get(key)
			if !ok {
				c.t.Fatalf("node %d lost acknowledged write %q (applied=%d, keys=%d)",
					uint64(st.ID), key, st.Applied, n.Store().Len())
			}
			if string(v.Data) != want {
				c.t.Fatalf("node %d has %q = %q, want %q", uint64(st.ID), key, v.Data, want)
			}
		}
	}
}

// waitConverged waits for every running node to reach the same applied index.
func (c *cluster) waitConverged(d time.Duration) {
	c.t.Helper()
	c.waitFor("every node to converge", d, func() bool {
		nodes := c.running()
		if len(nodes) == 0 {
			return false
		}
		target := nodes[0].Status().Applied
		keys := nodes[0].Store().Len()
		for _, n := range nodes[1:] {
			if n.Status().Applied != target || n.Store().Len() != keys {
				return false
			}
		}
		return true
	})
}

// chaosRNG is seeded explicitly so a failing run can be replayed exactly.
func chaosRNG(seed uint64) *rand.Rand { return rand.New(rand.NewPCG(seed, seed^0x5DEECE66D)) }

func (c *cluster) runningCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.nodes)
}
