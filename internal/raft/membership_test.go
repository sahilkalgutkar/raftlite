package raft

import (
	"errors"
	"fmt"
	"testing"
)

func TestAddALearnerThenPromoteIt(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)
	for i := 0; i < 3; i++ {
		if _, err := nw.propose(leader.ID(), fmt.Sprintf("cmd-%d", i)); err != nil {
			t.Fatalf("propose: %v", err)
		}
	}

	// A joining server starts as a learner: it replicates the log without
	// being counted in any quorum, so a slow new node cannot stall writes.
	joined := leader.Config().With(Member{ID: 4, Addr: "mem://4", Learner: true})
	nw.join(4, joined)
	if err := nw.confChange(leader.ID(), ConfChange{Type: ConfChangeAddLearner, ID: 4, Addr: "mem://4"}); err != nil {
		t.Fatalf("add learner: %v", err)
	}
	nw.tick(5)

	if leader.Config().Quorum() != 2 {
		t.Fatalf("quorum moved to %d when a learner joined", leader.Config().Quorum())
	}
	learner := nw.node(4)
	if learner.Log().LastIndex() != leader.Log().LastIndex() {
		t.Fatalf("learner is at %d, leader at %d", learner.Log().LastIndex(), leader.Log().LastIndex())
	}
	if got := nw.commands(4); len(got) != 3 {
		t.Fatalf("learner applied %v", got)
	}

	// Promotion is a second, separate change -- and only legal once the first
	// one committed.
	if err := nw.confChange(leader.ID(), ConfChange{Type: ConfChangePromote, ID: 4}); err != nil {
		t.Fatalf("promote: %v", err)
	}
	nw.tick(5)

	if leader.Config().Quorum() != 3 {
		t.Fatalf("quorum = %d after promotion, want 3", leader.Config().Quorum())
	}
	// Everyone must agree on the membership, not just the leader.
	for _, id := range nw.order {
		cfg := nw.node(id).Config()
		if !cfg.IsVoter(4) {
			t.Fatalf("node %d still thinks 4 is a learner: %v", uint64(id), cfg)
		}
	}
	nw.assertConverged(1, 2, 3, 4)
}

func TestANewVoterCanCarryAQuorum(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)

	joined := leader.Config().With(Member{ID: 4, Addr: "mem://4"})
	nw.join(4, joined)
	if err := nw.confChange(leader.ID(), ConfChange{Type: ConfChangeAddVoter, ID: 4, Addr: "mem://4"}); err != nil {
		t.Fatalf("add voter: %v", err)
	}
	nw.tick(5)
	if leader.Config().Quorum() != 3 {
		t.Fatalf("quorum = %d with four voters, want 3", leader.Config().Quorum())
	}

	// Stop one of the originals. Three of four voters remain, including the
	// new one, so writes must still commit.
	var victim NodeID
	for _, id := range []NodeID{1, 2, 3} {
		if id != leader.ID() {
			victim = id
			break
		}
	}
	nw.stop(victim)
	before := leader.Log().Committed()
	if _, err := nw.propose(leader.ID(), "needs the new voter"); err != nil {
		t.Fatalf("propose: %v", err)
	}
	nw.tick(3)
	if leader.Log().Committed() <= before {
		t.Fatalf("commit stuck at %d: the new voter is not counted", leader.Log().Committed())
	}
}

func TestRemovingAServerShrinksTheQuorum(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3, 4)
	leader := nw.tickUntilLeader(200)
	if leader.Config().Quorum() != 3 {
		t.Fatalf("starting quorum = %d, want 3 of four voters", leader.Config().Quorum())
	}

	var victim NodeID
	for _, id := range nw.order {
		if id != leader.ID() {
			victim = id
			break
		}
	}
	if err := nw.confChange(leader.ID(), ConfChange{Type: ConfChangeRemove, ID: victim}); err != nil {
		t.Fatalf("remove: %v", err)
	}
	nw.tick(5)

	if leader.Config().Quorum() != 2 {
		t.Fatalf("quorum = %d after removing a server, want 2", leader.Config().Quorum())
	}
	if leader.Config().Has(victim) {
		t.Fatalf("removed server is still in the configuration: %v", leader.Config())
	}
	if _, tracked := leader.Status().Progress[victim]; tracked {
		t.Fatal("leader still tracks progress for a removed server")
	}
}

func TestOnlyOneChangeMayBeInFlight(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)

	// Stop the followers so the first change cannot commit.
	for _, id := range nw.order {
		if id != leader.ID() {
			nw.stop(id)
		}
	}
	if _, err := leader.ProposeConfChange(ConfChange{Type: ConfChangeAddVoter, ID: 4, Addr: "mem://4"}); err != nil {
		t.Fatalf("first change: %v", err)
	}
	_, err := leader.ProposeConfChange(ConfChange{Type: ConfChangeAddVoter, ID: 5, Addr: "mem://5"})
	if !errors.Is(err, ErrConfChangeInFlight) {
		t.Fatalf("second change = %v, want ErrConfChangeInFlight", err)
	}

	// Once the first one commits, the next is allowed.
	nw.join(4, leader.Config().With(Member{ID: 4, Addr: "mem://4"}))
	nw.heal()
	nw.tick(5)
	if _, err := leader.ProposeConfChange(ConfChange{Type: ConfChangeAddVoter, ID: 5, Addr: "mem://5"}); err != nil {
		t.Fatalf("change after the first committed: %v", err)
	}
}

func TestConfChangeIsRefusedOnAFollower(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)
	for _, id := range nw.order {
		if id == leader.ID() {
			continue
		}
		_, err := nw.node(id).ProposeConfChange(ConfChange{Type: ConfChangeRemove, ID: 1})
		if !errors.Is(err, ErrNotLeader) {
			t.Fatalf("follower %d accepted a membership change: %v", uint64(id), err)
		}
	}
}

func TestImpossibleChangesAreRejectedBeforeReplication(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)
	before := leader.Log().LastIndex()

	for _, cc := range []ConfChange{
		{Type: ConfChangeRemove, ID: 99},              // not a member
		{Type: ConfChangePromote, ID: 99},             // not a member
		{Type: ConfChangeAddLearner, ID: leader.ID()}, // would demote a voter
	} {
		if _, err := leader.ProposeConfChange(cc); !errors.Is(err, ErrInvalidConfChange) {
			t.Fatalf("%v was accepted: %v", cc, err)
		}
	}
	if leader.Log().LastIndex() != before {
		t.Fatalf("a rejected change still reached the log: %d -> %d", before, leader.Log().LastIndex())
	}
}

func TestLeaderRemovingItselfStepsDown(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)

	if err := nw.confChange(leader.ID(), ConfChange{Type: ConfChangeRemove, ID: leader.ID()}); err != nil {
		t.Fatalf("remove self: %v", err)
	}
	nw.tick(5)

	if leader.Role() == Leader {
		t.Fatal("a leader that removed itself kept leading")
	}
	// The two remaining voters elect someone else.
	nw.tick(60)
	fresh := nw.leader()
	if fresh == nil {
		t.Fatal("the remaining servers never elected a leader")
	}
	if fresh.ID() == leader.ID() {
		t.Fatal("the removed server was elected")
	}
	if fresh.Config().Has(leader.ID()) {
		t.Fatalf("removed server is back in the configuration: %v", fresh.Config())
	}
}

func TestRemovedServerStopsCampaigning(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)

	var victim NodeID
	for _, id := range nw.order {
		if id != leader.ID() {
			victim = id
			break
		}
	}
	if err := nw.confChange(leader.ID(), ConfChange{Type: ConfChangeRemove, ID: victim}); err != nil {
		t.Fatalf("remove: %v", err)
	}
	nw.tick(5)

	removed := nw.node(victim)
	if removed.Config().Has(victim) {
		t.Fatalf("removed server did not apply its own removal: %v", removed.Config())
	}
	nw.isolate(victim)
	nw.tick(60)
	if removed.Role() != Follower {
		t.Fatalf("a removed server became %s", removed.Role())
	}
}

func TestUndecodableConfChangeIsSurvivable(t *testing.T) {
	// A corrupt payload that somehow reached the log must not take the node
	// down; the configuration simply stays as it was.
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)
	before := leader.Config()

	leader.applyConfChangeEntry(Entry{Index: 99, Type: EntryConfChange, Data: []byte{0xFF}})
	if !leader.Config().Equal(before) {
		t.Fatalf("configuration changed on a corrupt entry: %v", leader.Config())
	}

	// Same for a change that decodes but cannot be applied.
	bad := ConfChange{Type: ConfChangeRemove, ID: 77}
	leader.applyConfChangeEntry(Entry{Index: 100, Type: EntryConfChange, Data: bad.Marshal()})
	if !leader.Config().Equal(before) {
		t.Fatalf("configuration changed on an inapplicable entry: %v", leader.Config())
	}
}

func TestRemovingALaggardCanAdvanceTheCommitIndex(t *testing.T) {
	// Shrinking the cluster shrinks the quorum, which can make an entry that
	// was one acknowledgement short committable immediately. Four voters with
	// two of them behind commit nothing; drop one of the stragglers and the
	// remaining majority is already there.
	cfg := NewConfig(Member{ID: 1}, Member{ID: 2}, Member{ID: 3}, Member{ID: 4})
	n := NewNode(Options{ID: 1, Config: cfg})
	n.becomeCandidate()
	n.becomeLeader()
	for i := 0; i < 2; i++ {
		if _, err := n.Propose(EntryNormal, []byte("x")); err != nil {
			t.Fatalf("Propose: %v", err)
		}
	}
	last := n.Log().LastIndex()

	n.progress[2].maybeUpdate(last) // caught up
	n.progress[3].maybeUpdate(1)    // behind
	n.progress[4].maybeUpdate(1)    // behind
	n.maybeCommit()
	if got := n.Log().Committed(); got >= last {
		t.Fatalf("commit = %d before shrinking; the test needs it to be short of %d", got, last)
	}

	cc := ConfChange{Type: ConfChangeRemove, ID: 4}
	n.applyConfChangeEntry(Entry{Index: last, Type: EntryConfChange, Data: cc.Marshal()})

	if n.Config().Quorum() != 2 {
		t.Fatalf("quorum = %d after the removal", n.Config().Quorum())
	}
	if got := n.Log().Committed(); got != last {
		t.Fatalf("commit = %d after shrinking the cluster, want %d", got, last)
	}
}

func TestSyncProgressWithoutALeaderIsANoOp(t *testing.T) {
	n := newNetwork(t, 1, 2, 3).node(1)
	n.syncProgress() // progress is nil on a follower
	if n.Status().Progress != nil {
		t.Fatal("a follower grew a progress table")
	}
}
