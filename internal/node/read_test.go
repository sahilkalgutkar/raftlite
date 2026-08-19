package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sahilkalgutkar/raftlite/internal/fsm"
	"github.com/sahilkalgutkar/raftlite/internal/raft"
)

func TestLinearizableReadSeesTheLatestWrite(t *testing.T) {
	c := startCluster(t, 3, nil)
	leader := c.leader()
	put(t, leader, "counter", "1")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	v, ok, err := leader.Get(ctx, "counter", true)
	if err != nil || !ok || string(v.Data) != "1" {
		t.Fatalf("read = %+v, %v, %v", v, ok, err)
	}

	put(t, leader, "counter", "2")
	v, _, err = leader.Get(ctx, "counter", true)
	if err != nil || string(v.Data) != "2" {
		t.Fatalf("read after write = %+v, %v", v, err)
	}
}

func TestLinearizableReadIsRefusedOnAFollower(t *testing.T) {
	c := startCluster(t, 3, nil)
	leader := c.leader()
	put(t, leader, "k", "v")

	for _, id := range c.ids {
		n := c.nodes[id]
		if n == leader {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, _, err := n.Get(ctx, "k", true)
		cancel()
		if !errors.Is(err, raft.ErrNotLeader) {
			t.Fatalf("follower %d = %v, want ErrNotLeader", uint64(id), err)
		}

		// The same key is readable from local state when the caller says it
		// does not need the newest value.
		c.waitFor("the follower to apply the write", func() bool {
			_, ok := n.Store().Get("k")
			return ok
		})
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		v, ok, err := n.Get(ctx, "k", false)
		cancel()
		if err != nil || !ok || string(v.Data) != "v" {
			t.Fatalf("stale read on follower %d = %+v, %v, %v", uint64(id), v, ok, err)
		}
	}
}

func TestAPartitionedLeaderRefusesLinearizableReads(t *testing.T) {
	// This is the whole reason ReadIndex exists. A leader that has been cut
	// off does not know it yet, and its local state machine will happily
	// answer with a value the rest of the cluster has already replaced. The
	// linearizable path has to refuse; the stale path is allowed to lie, and
	// this test shows it doing exactly that.
	c := startCluster(t, 3, nil)
	old := c.leader()
	put(t, old, "leader", "first")

	c.mesh.Isolate(old.Status().ID)

	var fresh *Node
	c.waitFor("a new leader on the majority side", func() bool {
		for _, id := range c.ids {
			n := c.nodes[id]
			if n != old && n.IsLeader() {
				fresh = n
				return true
			}
		}
		return false
	})
	put(t, fresh, "leader", "second")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// The stale path answers from the deposed leader's own memory.
	stale, _, err := old.Get(ctx, "leader", false)
	if err != nil {
		t.Fatalf("stale read: %v", err)
	}
	if string(stale.Data) != "first" {
		t.Fatalf("stale read returned %q; the test needs the old leader to still hold the old value", stale.Data)
	}

	// The linearizable path must not.
	if _, _, err := old.Get(ctx, "leader", true); err == nil {
		t.Fatal("a partitioned leader served a linearizable read")
	}

	c.mesh.Heal()
	c.waitFor("the old leader to catch up", func() bool {
		v, ok := old.Store().Get("leader")
		return ok && string(v.Data) == "second"
	})
}

func TestManyConcurrentLinearizableReads(t *testing.T) {
	c := startCluster(t, 3, nil)
	leader := c.leader()
	put(t, leader, "k", "v")

	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, _, err := leader.Get(ctx, "k", true)
			errs <- err
		}()
	}
	for i := 0; i < 20; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent read %d: %v", i, err)
		}
	}
}

func TestLinearizableReadOnAStoppedNode(t *testing.T) {
	c := startCluster(t, 1, nil)
	leader := c.leader()
	if err := leader.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	delete(c.nodes, 1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := leader.LinearizableRead(ctx); !errors.Is(err, ErrStopped) {
		t.Fatalf("err = %v, want ErrStopped", err)
	}
}

func TestCancelledLinearizableReadReturnsPromptly(t *testing.T) {
	c := startCluster(t, 3, nil)
	leader := c.leader()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := leader.LinearizableRead(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}

	// The node is still perfectly usable afterwards.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	if _, err := leader.Propose(ctx2, fsm.Put("after", []byte("cancel"))); err != nil {
		t.Fatalf("Propose after a cancelled read: %v", err)
	}
}
