package raft

import (
	"errors"
	"testing"
)

func TestReadIndexNeedsAQuorumConfirmation(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)
	nw.deliver()

	leader.msgs = nil
	if err := leader.ReadIndex(42); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	// Nothing is servable yet: the leader has only its own word for it.
	rd := leader.Ready()
	if len(rd.ReadStates) != 0 {
		t.Fatalf("a read was confirmed with no acknowledgements: %v", rd.ReadStates)
	}

	// Heartbeats carrying the read identifier went out to both followers.
	var carried int
	for _, m := range rd.Messages {
		if m.Type == MsgHeartbeatReq && m.ReadID == 42 {
			carried++
		}
	}
	if carried != 2 {
		t.Fatalf("%d heartbeats carried the read id, want 2", carried)
	}

	// One follower answering is a majority in a three-node cluster.
	var follower NodeID
	for _, id := range nw.order {
		if id != leader.ID() {
			follower = id
			break
		}
	}
	if err := leader.Step(Message{
		Type: MsgHeartbeatResp, From: follower, To: leader.ID(), Term: leader.Term(), ReadID: 42,
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}

	rd = leader.Ready()
	if len(rd.ReadStates) != 1 || rd.ReadStates[0].ID != 42 {
		t.Fatalf("read states = %v", rd.ReadStates)
	}
	if rd.ReadStates[0].Index != leader.Log().Committed() {
		t.Fatalf("read index = %d, want the commit index %d", rd.ReadStates[0].Index, leader.Log().Committed())
	}
}

func TestSingleVoterServesAReadImmediately(t *testing.T) {
	nw := newNetwork(t, 1)
	leader := nw.tickUntilLeader(50)
	nw.deliver()

	if err := leader.ReadIndex(1); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	rd := leader.Ready()
	if len(rd.ReadStates) != 1 {
		t.Fatalf("a lone voter needed confirmation from someone: %v", rd.ReadStates)
	}
}

func TestReadIndexIsRefusedOnAFollower(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)
	for _, id := range nw.order {
		if id == leader.ID() {
			continue
		}
		if err := nw.node(id).ReadIndex(1); !errors.Is(err, ErrNotLeader) {
			t.Fatalf("follower %d = %v, want ErrNotLeader", uint64(id), err)
		}
	}
}

func TestReadIndexWaitsForTheCurrentTermsFirstCommit(t *testing.T) {
	// A leader inherits a commit index it cannot yet vouch for. Until its own
	// no-op commits, that index is not a promise it can keep.
	l := NewLog()
	l.Append(Entry{Term: 1}, Entry{Term: 1})
	n := NewNode(Options{
		ID:        1,
		Config:    NewConfig(Member{ID: 1}, Member{ID: 2}, Member{ID: 3}),
		HardState: HardState{Term: 2, Commit: 2},
		Log:       l,
	})
	n.becomeCandidate()
	n.becomeLeader()

	if err := n.ReadIndex(1); !errors.Is(err, ErrReadIndexUnavailable) {
		t.Fatalf("ReadIndex = %v, want ErrReadIndexUnavailable", err)
	}

	// Once a majority stores the no-op, the commit index is trustworthy.
	n.progress[2].maybeUpdate(n.Log().LastIndex())
	n.maybeCommit()
	if err := n.ReadIndex(2); err != nil {
		t.Fatalf("ReadIndex after committing in-term: %v", err)
	}
}

func TestALearnerCannotConfirmARead(t *testing.T) {
	nw := newNetworkWith(t, networkOpts{learners: []NodeID{3}}, 1, 2, 3)
	leader := nw.tickUntilLeader(100)
	nw.deliver()

	if err := leader.ReadIndex(7); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if err := leader.Step(Message{
		Type: MsgHeartbeatResp, From: 3, To: leader.ID(), Term: leader.Term(), ReadID: 7,
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if rd := leader.Ready(); len(rd.ReadStates) != 0 {
		t.Fatalf("a learner confirmed a read: %v", rd.ReadStates)
	}
}

func TestLosingLeadershipAbandonsReadsInFlight(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)
	nw.deliver()

	if err := leader.ReadIndex(5); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	// A higher term arrives: this node is no longer leader, so it can neither
	// finish confirming the read nor answer it.
	if err := leader.Step(Message{
		Type: MsgHeartbeatReq, From: 99, To: leader.ID(), Term: leader.Term() + 1,
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if err := leader.Step(Message{
		Type: MsgHeartbeatResp, From: 2, To: leader.ID(), Term: leader.Term(), ReadID: 5,
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if rd := leader.Ready(); len(rd.ReadStates) != 0 {
		t.Fatalf("a deposed leader confirmed a read: %v", rd.ReadStates)
	}
	if len(leader.pendingReads) != 0 {
		t.Fatal("reads in flight survived the loss of leadership")
	}
}

func TestUnknownReadAcknowledgementsAreIgnored(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)
	nw.deliver()

	leader.ackRead(999, 2) // no such read
	if rd := leader.Ready(); len(rd.ReadStates) != 0 {
		t.Fatalf("read states = %v", rd.ReadStates)
	}
}

func TestConcurrentReadsAreTrackedSeparately(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)
	nw.deliver()

	for _, id := range []uint64{1, 2, 3} {
		if err := leader.ReadIndex(id); err != nil {
			t.Fatalf("ReadIndex(%d): %v", id, err)
		}
	}
	if len(leader.pendingReads) != 3 {
		t.Fatalf("%d reads in flight, want 3", len(leader.pendingReads))
	}

	// Confirm the middle one only.
	if err := leader.Step(Message{
		Type: MsgHeartbeatResp, From: 2, To: leader.ID(), Term: leader.Term(), ReadID: 2,
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	rd := leader.Ready()
	if len(rd.ReadStates) != 1 || rd.ReadStates[0].ID != 2 {
		t.Fatalf("read states = %v", rd.ReadStates)
	}
	if len(leader.pendingReads) != 2 {
		t.Fatalf("%d reads still in flight, want 2", len(leader.pendingReads))
	}
}
