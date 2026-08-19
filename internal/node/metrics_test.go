package node

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sahilkalgutkar/raftlite/internal/fsm"
	"github.com/sahilkalgutkar/raftlite/internal/metrics"
)

func scrape(t *testing.T, n *Node) string {
	t.Helper()
	var sb strings.Builder
	if _, err := n.Metrics().WriteTo(&sb); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	return sb.String()
}

func TestNodeExportsItsState(t *testing.T) {
	c := startCluster(t, 3, nil)
	leader := c.leader()
	put(t, leader, "k", "v")

	out := scrape(t, leader)
	for _, want := range []string{
		`raftlite_is_leader{node="node-`,
		"raftlite_term{",
		"raftlite_commit_index{",
		"raftlite_applied_index{",
		"raftlite_keys{",
		"raftlite_members{",
		"raftlite_voters{",
		"raftlite_proposals_total{",
		"raftlite_entries_applied_total{",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}
	if !strings.Contains(out, "raftlite_keys{node=\"node-"+"") {
		t.Fatalf("no keys gauge:\n%s", out)
	}

	// The leader reports itself as leader; a follower does not.
	if !strings.Contains(out, "raftlite_is_leader{node=\""+leader.Status().ID.String()+"\"} 1") {
		t.Fatalf("leader gauge:\n%s", out)
	}
	for _, id := range c.ids {
		n := c.nodes[id]
		if n == leader {
			continue
		}
		followerOut := scrape(t, n)
		if !strings.Contains(followerOut, "raftlite_is_leader{node=\""+id.String()+"\"} 0") {
			t.Fatalf("follower %d reports itself leader:\n%s", uint64(id), followerOut)
		}
	}
}

func TestGaugesTrackTheNodeWithoutBeingUpdated(t *testing.T) {
	c := startCluster(t, 1, nil)
	leader := c.leader()

	before := scrape(t, leader)
	if !strings.Contains(before, "raftlite_keys{node=\"node-1\"} 0") {
		t.Fatalf("initial keys gauge:\n%s", before)
	}
	for i := 0; i < 5; i++ {
		put(t, leader, string(rune('a'+i)), "v")
	}
	after := scrape(t, leader)
	if !strings.Contains(after, "raftlite_keys{node=\"node-1\"} 5") {
		t.Fatalf("keys gauge after five writes:\n%s", after)
	}
}

func TestCountersRecordFailuresToo(t *testing.T) {
	c := startCluster(t, 3, nil)
	leader := c.leader()
	put(t, leader, "k", "v")

	// A follower refusing a write is a failed proposal on that node.
	var follower *Node
	for _, id := range c.ids {
		if c.nodes[id] != leader {
			follower = c.nodes[id]
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := follower.Propose(ctx, fsm.Put("k", []byte("v"))); err == nil {
		t.Fatal("a follower accepted a write")
	}
	out := scrape(t, follower)
	if !strings.Contains(out, "raftlite_proposals_failed_total{node=\""+follower.Status().ID.String()+"\"} 1") {
		t.Fatalf("failed proposal was not counted:\n%s", out)
	}
	if _, _, err := follower.Get(ctx, "k", true); err == nil {
		t.Fatal("a follower served a linearizable read")
	}
	out = scrape(t, follower)
	if !strings.Contains(out, "raftlite_linearizable_reads_failed_total{node=\""+follower.Status().ID.String()+"\"} 1") {
		t.Fatalf("failed read was not counted:\n%s", out)
	}
}

func TestLeaderChangesAreCounted(t *testing.T) {
	c := startCluster(t, 3, nil)
	old := c.leader()
	c.mesh.Isolate(old.Status().ID)

	var fresh *Node
	c.waitFor("a new leader", func() bool {
		for _, id := range c.ids {
			n := c.nodes[id]
			if n != old && n.IsLeader() {
				fresh = n
				return true
			}
		}
		return false
	})
	c.mesh.Heal()

	// The survivor saw the original leader and then itself: two changes.
	c.waitFor("the leader change to be counted", func() bool {
		return strings.Contains(scrape(t, fresh), "raftlite_leader_changes_total{node=\""+fresh.Status().ID.String()+"\"} 2")
	})
}

func TestTransportCountersAreExportedWhenAvailable(t *testing.T) {
	// The in-memory mesh does not expose counters, so a node using it must
	// simply not export transport series rather than failing to start.
	c := startCluster(t, 1, nil)
	out := scrape(t, c.leader())
	if strings.Contains(out, "raftlite_transport_") {
		t.Fatalf("mesh transport exported counters it does not have:\n%s", out)
	}
}

func TestASuppliedRegistryIsUsed(t *testing.T) {
	reg := metrics.NewRegistry()
	shared := reg.Counter("my_own_total", "Something the caller tracks.", nil)
	shared.Inc()

	c := startCluster(t, 1, func(cfg *Config) { cfg.Metrics = reg })
	leader := c.leader()

	out := scrape(t, leader)
	if !strings.Contains(out, "my_own_total 1") {
		t.Fatalf("the caller's own metric is missing:\n%s", out)
	}
	if !strings.Contains(out, "raftlite_term{") {
		t.Fatalf("the node did not register into the supplied registry:\n%s", out)
	}
}
