package raft

import (
	"errors"
	"fmt"
	"testing"
)

func TestNewLeaderAppendsAndCommitsANoOp(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)
	nw.deliver()

	last, err := leader.Log().Entry(leader.Log().LastIndex())
	if err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if last.Type != EntryNoOp || last.Term != leader.Term() {
		t.Fatalf("last entry = %v, want a no-op in the current term", last)
	}
	if leader.Log().Committed() != last.Index {
		t.Fatalf("no-op at %d is not committed (commit=%d)", last.Index, leader.Log().Committed())
	}
}

func TestProposeReplicatesAndCommitsEverywhere(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)

	for _, cmd := range []string{"set a=1", "set b=2", "del a"} {
		if _, err := nw.propose(leader.ID(), cmd); err != nil {
			t.Fatalf("propose %q: %v", cmd, err)
		}
	}

	for _, id := range nw.order {
		if got := nw.commands(id); len(got) != 3 || got[0] != "set a=1" || got[2] != "del a" {
			t.Fatalf("node %d applied %v", uint64(id), got)
		}
	}
	nw.assertConverged(1, 2, 3)
}

func TestProposeOnAFollowerIsRefused(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)

	for _, id := range nw.order {
		if id == leader.ID() {
			continue
		}
		if _, err := nw.node(id).Propose(EntryNormal, []byte("x")); !errors.Is(err, ErrNotLeader) {
			t.Fatalf("propose on follower %d = %v, want ErrNotLeader", uint64(id), err)
		}
	}
}

func TestCommitNeedsAQuorum(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)
	committed := leader.Log().Committed()

	// Cut both followers off. The leader can still append, but with one of
	// three replicas the entry must not commit.
	for _, id := range nw.order {
		if id != leader.ID() {
			nw.stop(id)
		}
	}
	if _, err := nw.propose(leader.ID(), "lonely write"); err != nil {
		t.Fatalf("propose: %v", err)
	}
	if got := leader.Log().Committed(); got != committed {
		t.Fatalf("commit advanced to %d without a quorum", got)
	}

	// One follower is enough to make two of three.
	nw.start(nw.order[0])
	if nw.order[0] == leader.ID() {
		nw.start(nw.order[1])
	}
	nw.tick(3)
	if got := leader.Log().Committed(); got <= committed {
		t.Fatalf("commit did not advance after quorum returned: %d", got)
	}
}

func TestLeaderRepairsADivergedFollowerLog(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	old := nw.tickUntilLeader(100)
	if _, err := nw.propose(old.ID(), "committed everywhere"); err != nil {
		t.Fatalf("propose: %v", err)
	}

	// Partition the leader away and let it accept writes nobody else sees.
	nw.isolate(old.ID())
	for i := 0; i < 3; i++ {
		if _, err := old.Propose(EntryNormal, []byte(fmt.Sprintf("orphan-%d", i))); err != nil {
			t.Fatalf("propose: %v", err)
		}
	}
	nw.deliver()
	orphanTail := old.Log().LastIndex()

	// The majority elects a new leader and does its own work.
	nw.tick(60)
	var survivors []NodeID
	for _, id := range nw.order {
		if id != old.ID() {
			survivors = append(survivors, id)
		}
	}
	fresh := nw.leader()
	if fresh == nil || fresh.ID() == old.ID() {
		t.Fatalf("majority did not elect a new leader (got %v)", fresh)
	}
	if _, err := nw.propose(fresh.ID(), "written by the new leader"); err != nil {
		t.Fatalf("propose: %v", err)
	}

	// Heal. The old leader must throw away its orphaned tail and take the
	// new leader's log instead.
	nw.heal()
	nw.tick(20)

	if old.Role() == Leader {
		t.Fatal("old leader did not step down")
	}
	if got := nw.commands(old.ID()); contains(got, "orphan-0") {
		t.Fatalf("orphaned entries were applied on the old leader: %v", got)
	}
	for _, cmd := range nw.commands(old.ID()) {
		if len(cmd) >= 6 && cmd[:6] == "orphan" {
			t.Fatalf("orphaned command %q survived", cmd)
		}
	}
	if old.Log().LastIndex() < orphanTail {
		t.Logf("old leader's log was truncated from %d to %d, as expected", orphanTail, old.Log().LastIndex())
	}
	nw.assertConverged(append(survivors, old.ID())...)
}

func contains(hay []string, needle string) bool {
	for _, s := range hay {
		if s == needle {
			return true
		}
	}
	return false
}

func TestConflictHintSkipsAnEntireTerm(t *testing.T) {
	// A follower carrying twenty entries from a term that never committed
	// should cost the leader one rejection, not twenty. Backing up one index
	// per round trip is correct but pathologically slow, so the follower
	// reports the first index of the conflicting term instead.
	leader := newNetwork(t, 1, 2, 3).node(1)
	for i := 0; i < 25; i++ {
		leader.log.Append(Entry{Term: 1})
	}
	leader.term = 7
	leader.becomeCandidate() // term 8
	leader.becomeLeader()

	follower := newNetwork(t, 1, 2, 3).node(2)
	for i := 0; i < 20; i++ {
		follower.log.Append(Entry{Term: 7})
	}
	follower.term = 8

	// Start the leader probing from the end of its own log, which is what
	// actually happens when a leader takes over.
	leader.progress[2].Next = leader.Log().LastIndex() + 1
	leader.progress[2].Match = 0

	rejections := 0
	sawTermHint := false
	for round := 0; round < 30; round++ {
		leader.msgs = nil
		leader.sendAppend(2)
		if len(leader.msgs) != 1 {
			t.Fatalf("round %d: leader sent %d messages", round, len(leader.msgs))
		}
		req := leader.msgs[0]
		req.From, req.Term = 1, follower.term

		follower.msgs = nil
		if err := follower.Step(req); err != nil {
			t.Fatalf("Step: %v", err)
		}
		if len(follower.msgs) != 1 {
			t.Fatalf("round %d: follower sent %d messages", round, len(follower.msgs))
		}
		resp := follower.msgs[0]
		resp.From = 2
		if !resp.Reject {
			// Two rejections: one to learn the follower's log is shorter, one
			// to learn the whole tail belongs to a dead term. Backing up an
			// index at a time would have taken twenty.
			if rejections > 3 {
				t.Fatalf("repair took %d rejections; the term hint is not being used", rejections)
			}
			if !sawTermHint {
				t.Fatal("no rejection ever reported the conflicting term")
			}
			if follower.Log().LastTerm() != leader.Log().LastTerm() {
				t.Fatalf("follower accepted but its log still ends in term %d", follower.Log().LastTerm())
			}
			return
		}
		if resp.ConflictTerm == 7 {
			if resp.ConflictIndex != 1 {
				t.Fatalf("hint index = %d, want the first index of term 7", resp.ConflictIndex)
			}
			sawTermHint = true
		}
		rejections++
		leader.handleAppendResponse(resp)
	}
	t.Fatalf("log repair never converged after %d rejections", rejections)
}

func TestLeaderOnlyCommitsByCountingItsOwnTerm(t *testing.T) {
	// Section 5.4.2: entries from an earlier term must not be committed just
	// because they are replicated on a majority. Only committing an entry of
	// the leader's own term unlocks them.
	l := NewLog()
	l.Append(Entry{Term: 1}, Entry{Term: 1}) // inherited from a previous leader
	n := NewNode(Options{
		ID:        1,
		Config:    NewConfig(Member{ID: 1}, Member{ID: 2}, Member{ID: 3}),
		HardState: HardState{Term: 2},
		Log:       l,
	})
	n.becomeCandidate() // term 3
	n.becomeLeader()    // appends a no-op at index 3

	if got := n.Log().Committed(); got != 0 {
		t.Fatalf("commit = %d before any follower acknowledged anything", got)
	}

	// A majority now stores the inherited entries -- still not committable.
	n.progress[2].maybeUpdate(2)
	if n.maybeCommit() {
		t.Fatalf("committed an inherited entry on replica count alone (commit=%d)", n.Log().Committed())
	}

	// Once the same majority stores the leader's own no-op, everything below
	// it commits at once.
	n.progress[2].maybeUpdate(3)
	if !n.maybeCommit() {
		t.Fatal("current-term entry replicated on a majority did not commit")
	}
	if got := n.Log().Committed(); got != 3 {
		t.Fatalf("commit = %d, want 3", got)
	}
}

func TestMaybeCommitIgnoresLearners(t *testing.T) {
	cfg := NewConfig(Member{ID: 1}, Member{ID: 2}, Member{ID: 3, Learner: true})
	n := NewNode(Options{ID: 1, Config: cfg})
	n.becomeCandidate()
	n.becomeLeader() // no-op at index 1; quorum of the two voters is 2

	if got := n.Log().Committed(); got != 0 {
		t.Fatalf("commit = %d before any follower answered", got)
	}

	n.progress[3].maybeUpdate(1) // the learner is fully caught up
	if n.maybeCommit() {
		t.Fatal("a learner's acknowledgement carried the quorum")
	}

	n.progress[2].maybeUpdate(1) // a real voter answers
	if !n.maybeCommit() {
		t.Fatal("voter acknowledgement did not commit")
	}
	if got := n.Log().Committed(); got != 1 {
		t.Fatalf("commit = %d, want 1", got)
	}
}

func TestCheckQuorumStepsDownAnIsolatedLeader(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)

	nw.isolate(leader.ID())
	nw.tick(60)

	if leader.Role() == Leader {
		t.Fatal("isolated leader never stepped down")
	}
	if leader.Leader() != None {
		t.Fatalf("stepped-down leader still points at %d", uint64(leader.Leader()))
	}
}

func TestLeaderKeepsLeadingWhileAQuorumAnswers(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)

	// Losing one of three followers is not losing the quorum.
	for _, id := range nw.order {
		if id != leader.ID() {
			nw.stop(id)
			break
		}
	}
	nw.tick(60)
	if leader.Role() != Leader {
		t.Fatalf("leader stepped down with a quorum still reachable (role=%s)", leader.Role())
	}
}

func TestAppendsAreCappedPerMessage(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)

	var lagging NodeID
	for _, id := range nw.order {
		if id != leader.ID() {
			lagging = id
			break
		}
	}
	nw.stop(lagging)
	for i := 0; i < maxAppendEntries*2; i++ {
		if _, err := leader.Propose(EntryNormal, []byte(fmt.Sprintf("cmd-%d", i))); err != nil {
			t.Fatalf("propose: %v", err)
		}
	}
	nw.deliver()

	leader.msgs = nil
	leader.sendAppend(lagging)
	if len(leader.msgs) != 1 {
		t.Fatalf("expected one append, got %d", len(leader.msgs))
	}
	if got := len(leader.msgs[0].Entries); got > maxAppendEntries {
		t.Fatalf("append carried %d entries, cap is %d", got, maxAppendEntries)
	}

	// And the backlog still drains completely once the follower is back.
	nw.start(lagging)
	nw.tick(20)
	nw.assertConverged(1, 2, 3)
}

func TestFollowerAtSnapshotBoundaryReportsItsPosition(t *testing.T) {
	n := newNetwork(t, 1, 2, 3).node(2)
	n.log.Append(Entry{Term: 1}, Entry{Term: 1}, Entry{Term: 1})
	n.log.CommitTo(3)
	n.log.AppliedTo(3)
	if err := n.log.Compact(3); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// The leader is still probing below our snapshot boundary. There is
	// nothing left to verify down there, so report where we actually are.
	if err := n.Step(Message{Type: MsgAppendReq, From: 1, To: 2, Term: 1, PrevLogIndex: 1, PrevLogTerm: 1}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	rd := n.Ready()
	if len(rd.Messages) != 1 {
		t.Fatalf("expected one response, got %v", rd.Messages)
	}
	if rd.Messages[0].Reject || rd.Messages[0].MatchIndex != 3 {
		t.Fatalf("response = %+v, want an acknowledgement at index 3", rd.Messages[0])
	}
}

func TestHeartbeatCommitIsClampedToWhatTheFollowerHolds(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)
	for i := 0; i < 3; i++ {
		if _, err := nw.propose(leader.ID(), fmt.Sprintf("cmd-%d", i)); err != nil {
			t.Fatalf("propose: %v", err)
		}
	}

	var follower NodeID
	for _, id := range nw.order {
		if id != leader.ID() {
			follower = id
			break
		}
	}
	leader.progress[follower].Match = 1

	leader.msgs = nil
	leader.sendHeartbeat(follower)
	if len(leader.msgs) != 1 {
		t.Fatalf("expected one heartbeat, got %d", len(leader.msgs))
	}
	if got := leader.msgs[0].Commit; got != 1 {
		t.Fatalf("heartbeat commit = %d, want it clamped to the follower's match index 1", got)
	}
}

func TestStaleLeaderIsRejected(t *testing.T) {
	n := newNetwork(t, 1, 2, 3).node(1)
	n.becomeFollower(9, 2)

	if err := n.Step(Message{Type: MsgAppendReq, From: 3, To: 1, Term: 4}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	rd := n.Ready()
	if len(rd.Messages) != 1 || !rd.Messages[0].Reject || rd.Messages[0].Term != 9 {
		t.Fatalf("stale leader got %v, want a rejection carrying term 9", rd.Messages)
	}
	if n.Leader() != 2 {
		t.Fatalf("stale append changed our leader to %d", uint64(n.Leader()))
	}
}

func TestAppendResponseFromAnUnknownServerIsIgnored(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)
	before := leader.Log().Committed()

	leader.handleAppendResponse(Message{Type: MsgAppendResp, From: 99, MatchIndex: 99})
	leader.handleHeartbeatResponse(Message{Type: MsgHeartbeatResp, From: 99})
	if got := leader.Log().Committed(); got != before {
		t.Fatalf("a stranger's acknowledgement moved the commit index to %d", got)
	}
}

func TestProgressBookkeeping(t *testing.T) {
	p := &Progress{Next: 10}

	if !p.maybeUpdate(5) || p.Match != 5 || p.Next != 10 {
		t.Fatalf("after first update: %v", p)
	}
	if p.maybeUpdate(3) {
		t.Fatalf("an out-of-order acknowledgement moved Match backwards: %v", p)
	}
	if p.Match != 5 {
		t.Fatalf("Match regressed to %d", p.Match)
	}
	if !p.maybeUpdate(12) || p.Next != 13 {
		t.Fatalf("Next did not follow Match: %v", p)
	}

	if p.maybeDecrTo(99) {
		t.Fatal("a stale rejection moved Next forward")
	}
	if !p.maybeDecrTo(8) || p.Next != 13 {
		// Next can never drop below Match+1: those entries are confirmed.
		t.Fatalf("decrement below Match was allowed: %v", p)
	}
	if p.String() == "" {
		t.Fatal("progress has no string form")
	}

	fresh := &Progress{Next: 20}
	if !fresh.maybeDecrTo(4) || fresh.Next != 4 {
		t.Fatalf("decrement to a hint failed: %v", fresh)
	}
	if !fresh.maybeDecrTo(0) || fresh.Next != 1 {
		t.Fatalf("decrement clamped wrong: %v", fresh)
	}
}

func TestStatusExposesProgress(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)
	nw.deliver()

	st := leader.Status()
	if len(st.Progress) != 3 {
		t.Fatalf("leader status has %d progress entries", len(st.Progress))
	}
	if st.Progress[leader.ID()].Match != leader.Log().LastIndex() {
		t.Fatalf("leader's own progress = %v", st.Progress[leader.ID()])
	}
	for _, id := range nw.order {
		if id == leader.ID() {
			continue
		}
		if st := nw.node(id).Status(); st.Progress != nil {
			t.Fatalf("follower %d reports follower progress: %v", uint64(id), st.Progress)
		}
	}
}

func TestConflictHintCases(t *testing.T) {
	n := newNetwork(t, 1, 2, 3).node(1)
	n.log.Append(Entry{Term: 1}, Entry{Term: 4}, Entry{Term: 4}, Entry{Term: 4}, Entry{Term: 6})

	t.Run("log too short", func(t *testing.T) {
		idx, term := n.conflictHint(99)
		if idx != n.Log().LastIndex()+1 || term != 0 {
			t.Fatalf("hint = %d/%d, want %d/0", idx, term, n.Log().LastIndex()+1)
		}
	})

	t.Run("term mismatch rewinds to the start of the term", func(t *testing.T) {
		idx, term := n.conflictHint(4)
		if term != 4 || idx != 2 {
			t.Fatalf("hint = %d/%d, want 2/4", idx, term)
		}
	})

	t.Run("single-entry term", func(t *testing.T) {
		idx, term := n.conflictHint(5)
		if term != 6 || idx != 5 {
			t.Fatalf("hint = %d/%d, want 5/6", idx, term)
		}
	})

	t.Run("compacted prefix falls back to the end of the log", func(t *testing.T) {
		if err := n.log.Compact(3); err != nil {
			t.Fatalf("Compact: %v", err)
		}
		idx, term := n.conflictHint(1)
		if idx != n.Log().LastIndex()+1 || term != 0 {
			t.Fatalf("hint = %d/%d", idx, term)
		}
	})
}

func TestNextIndexFromHint(t *testing.T) {
	n := newNetwork(t, 1, 2, 3).node(1)
	n.log.Append(Entry{Term: 1}, Entry{Term: 4}, Entry{Term: 4}, Entry{Term: 6})

	// No term in the hint: take the follower's word for where its log ends.
	if got := n.nextIndexFromHint(Message{ConflictIndex: 9}); got != 9 {
		t.Fatalf("next = %d, want 9", got)
	}
	// We also hold entries from that term: resume just after our last one.
	if got := n.nextIndexFromHint(Message{ConflictTerm: 4, ConflictIndex: 2}); got != 4 {
		t.Fatalf("next = %d, want 4", got)
	}
	// We never had that term at all: fall back to the follower's hint.
	if got := n.nextIndexFromHint(Message{ConflictTerm: 5, ConflictIndex: 2}); got != 2 {
		t.Fatalf("next = %d, want 2", got)
	}
	// A term newer than anything we hold.
	if got := n.nextIndexFromHint(Message{ConflictTerm: 9, ConflictIndex: 3}); got != 3 {
		t.Fatalf("next = %d, want 3", got)
	}
}
