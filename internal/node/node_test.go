package node

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sahilkalgutkar/raftlite/internal/fsm"
	"github.com/sahilkalgutkar/raftlite/internal/raft"
	"github.com/sahilkalgutkar/raftlite/internal/transport"
)

// cluster is a set of nodes wired together through an in-memory mesh, each
// with its own real directory on disk. The consensus and the durability are
// the genuine article; only the network is simulated.
type cluster struct {
	t     *testing.T
	mesh  *transport.Mesh
	nodes map[raft.NodeID]*Node
	dirs  map[raft.NodeID]string
	ids   []raft.NodeID
}

func startCluster(t *testing.T, size int, tune func(*Config)) *cluster {
	t.Helper()
	c := &cluster{
		t:     t,
		mesh:  transport.NewMesh(),
		nodes: make(map[raft.NodeID]*Node),
		dirs:  make(map[raft.NodeID]string),
	}

	var peers []raft.Member
	for i := 1; i <= size; i++ {
		id := raft.NodeID(i)
		c.ids = append(c.ids, id)
		c.dirs[id] = filepath.Join(t.TempDir(), fmt.Sprintf("node%d", i))
		peers = append(peers, raft.Member{ID: id, Addr: fmt.Sprintf("mem://%d", i)})
	}

	for _, id := range c.ids {
		c.start(id, peers, id == 1, tune)
	}
	t.Cleanup(c.stopAll)
	return c
}

func (c *cluster) start(id raft.NodeID, peers []raft.Member, bootstrap bool, tune func(*Config)) {
	c.t.Helper()
	cfg := Config{
		ID:                id,
		Addr:              fmt.Sprintf("mem://%d", uint64(id)),
		Dir:               c.dirs[id],
		Peers:             peers,
		Bootstrap:         bootstrap,
		TickInterval:      5 * time.Millisecond,
		ElectionTicks:     8,
		HeartbeatTicks:    1,
		SnapshotThreshold: 1 << 30, // effectively off unless a test lowers it
		NewTransport:      c.mesh.Factory(id, fmt.Sprintf("mem://%d", uint64(id))),
		NoSync:            true, // these tests restart processes, not machines
	}
	if tune != nil {
		tune(&cfg)
	}
	n, err := Start(cfg)
	if err != nil {
		c.t.Fatalf("Start node %d: %v", uint64(id), err)
	}
	c.nodes[id] = n
}

func (c *cluster) stopAll() {
	for _, n := range c.nodes {
		_ = n.Stop()
	}
}

// leader waits for a single leader to emerge and returns it.
func (c *cluster) leader() *Node {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var found *Node
		count := 0
		for _, id := range c.ids {
			n, ok := c.nodes[id]
			if ok && n.IsLeader() {
				count++
				found = n
			}
		}
		if count == 1 {
			return found
		}
		time.Sleep(2 * time.Millisecond)
	}
	c.t.Fatal("no leader emerged")
	return nil
}

func (c *cluster) waitFor(what string, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	c.t.Fatalf("timed out waiting for %s", what)
}

func put(t *testing.T, n *Node, key, value string) fsm.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := n.Propose(ctx, fsm.Put(key, []byte(value)))
	if err != nil {
		t.Fatalf("put %s=%s: %v", key, value, err)
	}
	return res
}

func TestSingleNodeAcceptsWrites(t *testing.T) {
	c := startCluster(t, 1, nil)
	leader := c.leader()

	res := put(t, leader, "colour", "green")
	if res.Revision == 0 {
		t.Fatalf("result = %+v", res)
	}
	v, ok := leader.Store().Get("colour")
	if !ok || string(v.Data) != "green" {
		t.Fatalf("read back %+v, %v", v, ok)
	}
	if leader.Status().Role != raft.Leader {
		t.Fatalf("status = %+v", leader.Status())
	}
	if leader.LeaderAddr() != leader.Addr() {
		t.Fatalf("leader addr = %q, want %q", leader.LeaderAddr(), leader.Addr())
	}
}

func TestWritesReplicateToEveryNode(t *testing.T) {
	c := startCluster(t, 3, nil)
	leader := c.leader()

	for i := 0; i < 20; i++ {
		put(t, leader, fmt.Sprintf("key-%02d", i), fmt.Sprintf("value-%d", i))
	}

	c.waitFor("every node to apply every write", func() bool {
		for _, id := range c.ids {
			if c.nodes[id].Store().Len() != 20 {
				return false
			}
		}
		return true
	})
	for _, id := range c.ids {
		v, ok := c.nodes[id].Store().Get("key-07")
		if !ok || string(v.Data) != "value-7" {
			t.Fatalf("node %d has %+v", uint64(id), v)
		}
	}
}

func TestFollowersRefuseWrites(t *testing.T) {
	c := startCluster(t, 3, nil)
	leader := c.leader()

	for _, id := range c.ids {
		n := c.nodes[id]
		if n == leader {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := n.Propose(ctx, fsm.Put("k", []byte("v")))
		cancel()
		if !errors.Is(err, raft.ErrNotLeader) {
			t.Fatalf("follower %d returned %v, want ErrNotLeader", uint64(id), err)
		}
	}
}

func TestClusterSurvivesLosingItsLeader(t *testing.T) {
	c := startCluster(t, 3, nil)
	old := c.leader()
	put(t, old, "before", "the outage")

	// Cut the leader off entirely, the way a machine dying looks to everyone
	// else, and let the survivors elect a replacement.
	c.mesh.Isolate(old.Status().ID)

	var fresh *Node
	c.waitFor("a new leader among the survivors", func() bool {
		for _, id := range c.ids {
			n := c.nodes[id]
			if n != old && n.IsLeader() {
				fresh = n
				return true
			}
		}
		return false
	})

	put(t, fresh, "after", "the failover")
	if v, ok := fresh.Store().Get("before"); !ok || string(v.Data) != "the outage" {
		t.Fatalf("the new leader lost a committed write: %+v", v)
	}

	// The old leader rejoins, learns it was deposed, and catches up.
	c.mesh.Heal()
	c.waitFor("the old leader to catch up", func() bool {
		v, ok := old.Store().Get("after")
		return ok && string(v.Data) == "the failover"
	})
	if old.IsLeader() {
		t.Fatal("the old leader is leading again alongside the new one")
	}
}

func TestAMinorityCannotCommit(t *testing.T) {
	c := startCluster(t, 3, nil)
	leader := c.leader()
	put(t, leader, "committed", "yes")

	// Strand the leader with no followers. It still thinks it is leader for a
	// moment, but nothing it accepts can reach a quorum.
	c.mesh.Isolate(leader.Status().ID)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := leader.Propose(ctx, fsm.Put("stranded", []byte("no")))
	if err == nil {
		t.Fatal("a write committed inside a minority partition")
	}

	c.mesh.Heal()
	c.waitFor("the cluster to settle again", func() bool { return c.leaderCount() == 1 })
}

func (c *cluster) leaderCount() int {
	count := 0
	for _, id := range c.ids {
		if n, ok := c.nodes[id]; ok && n.IsLeader() {
			count++
		}
	}
	return count
}

func TestStateSurvivesARestart(t *testing.T) {
	c := startCluster(t, 1, nil)
	leader := c.leader()
	for i := 0; i < 10; i++ {
		put(t, leader, fmt.Sprintf("key-%d", i), "value")
	}
	term := leader.Status().Term
	if err := leader.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	delete(c.nodes, 1)

	// Same directory, fresh process.
	c.start(1, []raft.Member{{ID: 1, Addr: "mem://1"}}, false, nil)
	restarted := c.leader()

	if restarted.Store().Len() != 10 {
		t.Fatalf("recovered %d keys, want 10", restarted.Store().Len())
	}
	if restarted.Status().Term < term {
		t.Fatalf("term went backwards: %d -> %d", term, restarted.Status().Term)
	}
	put(t, restarted, "after-restart", "ok")
}

func TestSnapshotsCompactTheLogAndSurviveRestart(t *testing.T) {
	c := startCluster(t, 1, func(cfg *Config) { cfg.SnapshotThreshold = 10 })
	leader := c.leader()

	for i := 0; i < 60; i++ {
		put(t, leader, fmt.Sprintf("key-%02d", i), fmt.Sprintf("value-%d", i))
	}
	c.waitFor("the log to be compacted", func() bool { return leader.Status().Snapshot > 0 })

	snapDir := filepath.Join(c.dirs[1], "snapshots")
	if entries, err := filepathGlob(snapDir); err != nil || len(entries) == 0 {
		t.Fatalf("no snapshot files in %s (%v)", snapDir, err)
	}
	if err := leader.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	delete(c.nodes, 1)

	c.start(1, []raft.Member{{ID: 1, Addr: "mem://1"}}, false, func(cfg *Config) { cfg.SnapshotThreshold = 10 })
	restarted := c.leader()

	if restarted.Store().Len() != 60 {
		t.Fatalf("recovered %d keys from a snapshot, want 60", restarted.Store().Len())
	}
	v, ok := restarted.Store().Get("key-42")
	if !ok || string(v.Data) != "value-42" {
		t.Fatalf("recovered key-42 as %+v", v)
	}
}

func TestALaggingNodeCatchesUpThroughASnapshot(t *testing.T) {
	c := startCluster(t, 3, func(cfg *Config) { cfg.SnapshotThreshold = 15 })
	leader := c.leader()

	// Take one follower off the network, write enough to compact past what it
	// holds, then bring it back.
	var lagging raft.NodeID
	for _, id := range c.ids {
		if id != leader.Status().ID {
			lagging = id
			break
		}
	}
	c.mesh.Isolate(lagging)

	for i := 0; i < 60; i++ {
		put(t, leader, fmt.Sprintf("key-%02d", i), "v")
	}
	c.waitFor("the leader to compact", func() bool { return leader.Status().Snapshot > 0 })

	c.mesh.Heal()
	c.waitFor("the lagging node to catch up", func() bool {
		return c.nodes[lagging].Store().Len() == leader.Store().Len()
	})
}

func TestMembershipChangesAtRuntime(t *testing.T) {
	c := startCluster(t, 3, nil)
	leader := c.leader()
	put(t, leader, "existing", "data")

	// A fourth server joins as a learner: it replicates without being counted
	// in any quorum, so it cannot stall writes while it catches up.
	newID := raft.NodeID(4)
	c.ids = append(c.ids, newID)
	c.dirs[newID] = filepath.Join(t.TempDir(), "node4")
	peers := append(append([]raft.Member(nil), leader.Members()...),
		raft.Member{ID: newID, Addr: "mem://4", Learner: true})
	c.start(newID, peers, false, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := leader.AddMember(ctx, newID, "mem://4", false); err != nil {
		t.Fatalf("AddMember: %v", err)
	}
	c.waitFor("the learner to catch up", func() bool {
		v, ok := c.nodes[newID].Store().Get("existing")
		return ok && string(v.Data) == "data"
	})
	if len(leader.Members()) != 4 {
		t.Fatalf("members = %v", leader.Members())
	}

	if err := leader.PromoteMember(ctx, newID); err != nil {
		t.Fatalf("PromoteMember: %v", err)
	}
	c.waitFor("the promotion to reach every node", func() bool {
		for _, id := range c.ids {
			if !c.nodes[id].Status().Config.IsVoter(newID) {
				return false
			}
		}
		return true
	})

	// And it can leave again.
	if err := leader.RemoveMember(ctx, newID); err != nil {
		t.Fatalf("RemoveMember: %v", err)
	}
	c.waitFor("the removal to reach every node", func() bool {
		return len(leader.Members()) == 3
	})
}

func TestProposalsFailWhenLeadershipIsLost(t *testing.T) {
	c := startCluster(t, 3, nil)
	leader := c.leader()

	// Strand the leader mid-write. The entry can never commit, and the client
	// must be told rather than left hanging.
	c.mesh.Isolate(leader.Status().ID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := leader.Propose(ctx, fsm.Put("doomed", []byte("write")))
	if err == nil {
		t.Fatal("a write inside a one-node partition reported success")
	}
	if !errors.Is(err, ErrLeadershipLost) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	c.mesh.Heal()
}

func TestStopIsIdempotentAndReleasesWaiters(t *testing.T) {
	c := startCluster(t, 1, nil)
	leader := c.leader()

	if err := leader.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := leader.Stop(); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
	select {
	case <-leader.Done():
	default:
		t.Fatal("Done was not closed after Stop")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := leader.Propose(ctx, fsm.Put("k", []byte("v"))); !errors.Is(err, ErrStopped) {
		t.Fatalf("propose after stop = %v, want ErrStopped", err)
	}
	delete(c.nodes, 1)
}

func TestCancelledProposalsReturnPromptly(t *testing.T) {
	c := startCluster(t, 3, nil)
	leader := c.leader()
	c.mesh.Isolate(leader.Status().ID)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before it is submitted

	if _, err := leader.Propose(ctx, fsm.Put("k", []byte("v"))); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	c.mesh.Heal()
}

func TestStartRejectsAnUnusableDirectory(t *testing.T) {
	if _, err := Start(Config{ID: 1, Dir: ""}); err == nil {
		t.Fatal("Start accepted an empty directory")
	}
}

func TestStatusBeforeTheLoopRuns(t *testing.T) {
	// Status must be safe to call from any goroutine at any point, including
	// before anything has been published.
	n := &Node{cfg: Config{ID: 7}}
	if st := n.Status(); st.ID != 7 {
		t.Fatalf("status = %+v", st)
	}
}

func TestReceiveDropsWhenTheNodeIsBehind(t *testing.T) {
	// A full inbound queue must never block the transport's reader goroutine.
	n := &Node{cfg: Config{ID: 1, Logger: nil}, recvCh: make(chan raft.Message, 1), stopCh: make(chan struct{})}
	n.cfg.withDefaults()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			n.Receive(raft.Message{Type: raft.MsgHeartbeatReq})
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Receive blocked on a full queue")
	}
}

func TestAFatalErrorIsReportedByStop(t *testing.T) {
	// Anything that stops the node making its state durable is fatal by
	// definition: serving after a failed write means answering from state the
	// node cannot promise to remember. The failure has to surface, not be
	// swallowed into a node that looks healthy.
	c := startCluster(t, 1, nil)
	n := c.leader()

	n.fail(errors.New("the disk went away"))
	err := n.Stop()
	if err == nil {
		t.Fatal("Stop reported success after a fatal error")
	}
	if !strings.Contains(err.Error(), "the disk went away") {
		t.Fatalf("err = %v", err)
	}
	delete(c.nodes, 1)
}
