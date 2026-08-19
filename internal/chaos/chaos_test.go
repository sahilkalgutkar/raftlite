package chaos

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sahilkalgutkar/raftlite/internal/node"
	"github.com/sahilkalgutkar/raftlite/internal/raft"
)

func TestRollingRestartKeepsTheClusterAvailable(t *testing.T) {
	// The everyday operation: upgrade a cluster one node at a time. It must
	// stay writable throughout and lose nothing.
	c := newCluster(t, 3, nil)
	acked := map[string]string{}

	write := func(tag string) {
		t.Helper()
		key, value := "key-"+tag, "value-"+tag
		if err := c.put(key, value, 20*time.Second); err != nil {
			t.Fatalf("write %s: %v", tag, err)
		}
		acked[key] = value
	}

	write("before")
	for _, id := range c.ids {
		c.crash(id)
		write(fmt.Sprintf("while-%d-is-down", uint64(id)))
		c.restart(id)
		c.waitFor(fmt.Sprintf("node %d to rejoin", uint64(id)), 20*time.Second, func() bool {
			n := c.node(id)
			return n != nil && n.Status().Leader != raft.None
		})
		write(fmt.Sprintf("after-%d-returns", uint64(id)))
	}

	c.waitConverged(20 * time.Second)
	c.assertAcknowledgedWritesSurvive(acked)
}

func TestWritesSurviveLeaderFailuresUnderLoad(t *testing.T) {
	// Kill the leader repeatedly while writes are in flight. Some writes will
	// fail, and that is fine -- the contract is only about the ones that
	// succeeded.
	c := newCluster(t, 5, nil)
	c.leader()

	var (
		mu    sync.Mutex
		acked = map[string]string{}
		stop  = make(chan struct{})
		wg    sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			key, value := fmt.Sprintf("key-%04d", i), fmt.Sprintf("value-%d", i)
			if err := c.put(key, value, 10*time.Second); err == nil {
				mu.Lock()
				acked[key] = value
				mu.Unlock()
			}
		}
	}()

	for round := 0; round < 3; round++ {
		time.Sleep(150 * time.Millisecond)
		leader := c.leaderWithin(10 * time.Second)
		if leader == nil {
			t.Fatal("no leader to kill")
		}
		id := leader.Status().ID
		c.crash(id)
		time.Sleep(100 * time.Millisecond)
		c.restart(id)
	}

	close(stop)
	wg.Wait()

	c.waitConverged(30 * time.Second)
	mu.Lock()
	defer mu.Unlock()
	if len(acked) < 5 {
		t.Fatalf("only %d writes were acknowledged; the test did not exercise anything", len(acked))
	}
	c.assertAcknowledgedWritesSurvive(acked)
	t.Logf("%d acknowledged writes survived three leader failures", len(acked))
}

func TestAMinorityPartitionCannotDiverge(t *testing.T) {
	// The classic split brain scenario. Two nodes are cut off from three; the
	// majority keeps working, the minority must not accept anything, and after
	// healing the minority must adopt the majority's history.
	c := newCluster(t, 5, nil)
	leader := c.leader()
	if err := c.put("before", "partition", 10*time.Second); err != nil {
		t.Fatalf("write: %v", err)
	}

	// The sitting leader goes into the minority, which is the interesting
	// case: it starts out still believing it is in charge.
	minority := []raft.NodeID{leader.Status().ID}
	var majority []raft.NodeID
	for _, id := range c.ids {
		switch {
		case id == leader.Status().ID:
		case len(minority) < 2:
			minority = append(minority, id)
		default:
			majority = append(majority, id)
		}
	}
	c.mesh.Partition(minority, majority)

	// The majority elects someone and carries on.
	c.waitFor("the majority to elect a leader", 20*time.Second, func() bool {
		for _, id := range majority {
			if n := c.node(id); n != nil && n.IsLeader() {
				return true
			}
		}
		return false
	})
	acked := map[string]string{"before": "partition"}
	for i := 0; i < 5; i++ {
		key, value := fmt.Sprintf("majority-%d", i), "written"
		if err := c.put(key, value, 10*time.Second); err != nil {
			t.Fatalf("majority write: %v", err)
		}
		acked[key] = value
	}

	// Nothing in the minority may have accepted a write of its own.
	for _, id := range minority {
		n := c.node(id)
		if n.Store().Len() > 1 {
			t.Fatalf("minority node %d applied %d keys", uint64(id), n.Store().Len())
		}
	}

	c.mesh.Heal()
	c.waitConverged(30 * time.Second)
	c.assertAcknowledgedWritesSurvive(acked)

	// And there is exactly one leader again.
	leaders := 0
	for _, n := range c.running() {
		if n.IsLeader() {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("%d leaders after healing", leaders)
	}
}

func TestRandomisedChaos(t *testing.T) {
	// Crash, restart, isolate and heal at random while a writer keeps going.
	// The seed is fixed so a failure can be replayed exactly; the schedule it
	// produces is far more inventive than anything worth writing by hand.
	const (
		seed   = 20260819
		rounds = 40
	)
	c := newCluster(t, 5, nil)
	c.leader()

	var (
		mu    sync.Mutex
		acked = map[string]string{}
		stop  = make(chan struct{})
		wg    sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			key, value := fmt.Sprintf("chaos-%04d", i), fmt.Sprintf("value-%d", i)
			if err := c.put(key, value, 5*time.Second); err == nil {
				mu.Lock()
				acked[key] = value
				mu.Unlock()
			}
		}
	}()

	rng := chaosRNG(seed)
	for round := 0; round < rounds; round++ {
		victim := c.ids[rng.IntN(len(c.ids))]

		switch rng.IntN(4) {
		case 0:
			// Crash a node, but never enough of them to lose the quorum: a
			// cluster that cannot commit is not a bug, and testing it here
			// would only prove the writer times out.
			if c.runningCount() > len(c.ids)/2+1 {
				c.crash(victim)
			}
		case 1:
			c.restart(victim)
		case 2:
			c.mesh.Isolate(victim)
		default:
			c.mesh.Heal()
		}
		time.Sleep(time.Duration(10+rng.IntN(40)) * time.Millisecond)
	}

	close(stop)
	wg.Wait()

	// Put the cluster back together and let it settle.
	c.mesh.Heal()
	for _, id := range c.ids {
		c.restart(id)
	}
	c.waitFor("a leader after the chaos stops", 30*time.Second, func() bool {
		return c.leaderWithin(time.Second) != nil
	})
	c.waitConverged(60 * time.Second)

	mu.Lock()
	defer mu.Unlock()
	if len(acked) < 10 {
		t.Fatalf("only %d writes were acknowledged across %d rounds of chaos", len(acked), rounds)
	}
	c.assertAcknowledgedWritesSurvive(acked)
	t.Logf("%d acknowledged writes survived %d rounds of random failures", len(acked), rounds)
}

func TestSnapshotsAndRestartsTogether(t *testing.T) {
	// Compaction plus restarts is where the interesting interactions live: a
	// node that comes back to find the leader no longer holds the entries it
	// needs has to be caught up with a snapshot instead.
	c := newCluster(t, 3, func(cfg *node.Config) { cfg.SnapshotThreshold = 20 })
	c.leader()

	acked := map[string]string{}
	writeBatch := func(prefix string, count int) {
		t.Helper()
		for i := 0; i < count; i++ {
			key, value := fmt.Sprintf("%s-%03d", prefix, i), fmt.Sprintf("value-%d", i)
			if err := c.put(key, value, 15*time.Second); err != nil {
				t.Fatalf("write %s: %v", key, err)
			}
			acked[key] = value
		}
	}

	writeBatch("first", 30)
	c.waitFor("the leader to compact", 20*time.Second, func() bool {
		l := c.leaderWithin(time.Second)
		return l != nil && l.Status().Snapshot > 0
	})

	// Take a follower down, write far past what it holds, and bring it back.
	var victim raft.NodeID
	leader := c.leader()
	for _, id := range c.ids {
		if id != leader.Status().ID {
			victim = id
			break
		}
	}
	c.crash(victim)
	writeBatch("second", 60)
	c.restart(victim)

	c.waitConverged(60 * time.Second)
	c.assertAcknowledgedWritesSurvive(acked)

	returned := c.node(victim)
	if returned.Status().Snapshot == 0 {
		t.Fatal("the returning node never installed a snapshot")
	}
	if returned.Store().Len() != len(acked) {
		t.Fatalf("returning node holds %d keys, want %d", returned.Store().Len(), len(acked))
	}
}

func TestEveryNodeCrashesAndTheDataComesBack(t *testing.T) {
	// A whole-cluster outage: every node down at once, then every node back.
	// Nothing acknowledged before the outage may be missing afterwards, which
	// is the entire argument for writing to disk before answering a client.
	c := newCluster(t, 3, nil)
	c.leader()

	acked := map[string]string{}
	for i := 0; i < 25; i++ {
		key, value := fmt.Sprintf("key-%02d", i), fmt.Sprintf("value-%d", i)
		if err := c.put(key, value, 15*time.Second); err != nil {
			t.Fatalf("write: %v", err)
		}
		acked[key] = value
	}

	for _, id := range c.ids {
		c.crash(id)
	}
	if len(c.running()) != 0 {
		t.Fatal("nodes survived a full shutdown")
	}
	for _, id := range c.ids {
		c.restart(id)
	}

	c.leader()
	c.waitConverged(30 * time.Second)
	c.assertAcknowledgedWritesSurvive(acked)
}
