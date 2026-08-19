package raft

import (
	"errors"
	"fmt"
	"testing"
)

// buildLeaderWithBacklog elects a leader, writes n commands, and returns the
// leader plus the id of a follower that was offline the whole time.
func buildLeaderWithBacklog(t *testing.T, n int) (*network, *Node, NodeID) {
	t.Helper()
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
	for i := 0; i < n; i++ {
		if _, err := nw.propose(leader.ID(), fmt.Sprintf("cmd-%d", i)); err != nil {
			t.Fatalf("propose: %v", err)
		}
	}
	return nw, leader, lagging
}

func TestCompactDiscardsTheLogPrefix(t *testing.T) {
	nw, leader, _ := buildLeaderWithBacklog(t, 5)
	nw.deliver()

	applied := leader.Log().Applied()
	if err := leader.Compact(applied, []byte("state machine image")); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if got := leader.Log().SnapshotIndex(); got != applied {
		t.Fatalf("snapshot boundary = %d, want %d", got, applied)
	}
	if leader.Log().FirstIndex() != applied+1 {
		t.Fatalf("first index = %d after compaction", leader.Log().FirstIndex())
	}
	snap := leader.Snapshot()
	if snap == nil || snap.Meta.Index != applied || string(snap.Data) != "state machine image" {
		t.Fatalf("snapshot = %+v", snap)
	}
	if len(snap.Meta.Config.Members) != 3 {
		t.Fatalf("snapshot did not capture the configuration: %v", snap.Meta.Config)
	}
	// Compaction must not disturb what is committed or applied.
	if leader.Log().Committed() < applied {
		t.Fatalf("commit regressed to %d", leader.Log().Committed())
	}
}

func TestCompactRefusesUnappliedIndices(t *testing.T) {
	nw, leader, _ := buildLeaderWithBacklog(t, 3)
	nw.deliver()

	if err := leader.Compact(leader.Log().LastIndex()+50, nil); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("compacting past the log = %v, want ErrUnavailable", err)
	}
	if err := leader.Compact(leader.Log().Applied(), nil); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := leader.Compact(1, nil); !errors.Is(err, ErrCompacted) {
		t.Fatalf("re-compacting = %v, want ErrCompacted", err)
	}
}

func TestLeaderSendsASnapshotWhenTheEntriesAreGone(t *testing.T) {
	nw, leader, lagging := buildLeaderWithBacklog(t, 6)
	nw.deliver()

	if err := leader.Compact(leader.Log().Applied(), []byte("image")); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	leader.msgs = nil
	leader.sendAppend(lagging)
	if len(leader.msgs) != 1 || leader.msgs[0].Type != MsgSnapshotReq {
		t.Fatalf("leader sent %v, want a snapshot request", leader.msgs)
	}
	if leader.msgs[0].Snapshot == nil || leader.msgs[0].Snapshot.Meta.Index == 0 {
		t.Fatalf("snapshot request carries no snapshot: %+v", leader.msgs[0])
	}
	if leader.progress[lagging].PendingSnapshot == 0 {
		t.Fatal("follower was not marked as having a snapshot in flight")
	}

	// While one is in flight the leader stops probing, since every append it
	// could send would be rejected until the snapshot lands.
	leader.msgs = nil
	leader.sendAppend(lagging)
	if len(leader.msgs) != 0 {
		t.Fatalf("leader kept probing during a pending snapshot: %v", leader.msgs)
	}
}

func TestCompactedLeaderWithoutASnapshotDoesNotSpin(t *testing.T) {
	// Compacting the log directly, without producing an image, is a bug
	// elsewhere. The leader must complain rather than send garbage or loop.
	nw, leader, lagging := buildLeaderWithBacklog(t, 4)
	nw.deliver()
	if err := leader.log.Compact(leader.Log().Applied()); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	leader.msgs = nil
	leader.progress[lagging].Next = 1
	leader.sendAppend(lagging)
	if len(leader.msgs) != 0 {
		t.Fatalf("leader sent %v with no snapshot to send", leader.msgs)
	}
}

func TestFollowerInstallsASnapshot(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	follower := nw.node(2)
	follower.becomeFollower(5, 1)

	cfg := NewConfig(Member{ID: 1}, Member{ID: 2}, Member{ID: 3}, Member{ID: 4, Learner: true})
	snap := &Snapshot{
		Meta: SnapshotMeta{Index: 40, Term: 4, Config: cfg},
		Data: []byte("image"),
	}
	if err := follower.Step(Message{Type: MsgSnapshotReq, From: 1, To: 2, Term: 5, Snapshot: snap}); err != nil {
		t.Fatalf("Step: %v", err)
	}

	if got := follower.Log().SnapshotIndex(); got != 40 {
		t.Fatalf("log snapshot boundary = %d", got)
	}
	if follower.Log().Committed() != 40 || follower.Log().Applied() != 40 {
		t.Fatalf("commit/applied = %d/%d", follower.Log().Committed(), follower.Log().Applied())
	}
	// The membership travels with the image: a follower installing a snapshot
	// may be adopting a configuration it never saw an entry for.
	if len(follower.Config().Members) != 4 {
		t.Fatalf("configuration = %v", follower.Config())
	}

	rd := follower.Ready()
	if rd.Snapshot == nil || rd.Snapshot.Meta.Index != 40 {
		t.Fatalf("Ready did not hand the snapshot to the runtime: %+v", rd.Snapshot)
	}
	if len(rd.Messages) != 1 || rd.Messages[0].Type != MsgSnapshotResp || rd.Messages[0].MatchIndex != 40 {
		t.Fatalf("response = %v", rd.Messages)
	}
	if len(rd.CommittedEntries) != 0 {
		t.Fatalf("a restored node re-applied %d entries it already has", len(rd.CommittedEntries))
	}
	// And a second Ready must not hand out the same snapshot twice.
	if next := follower.Ready(); next.Snapshot != nil {
		t.Fatal("Ready re-issued the snapshot")
	}
}

func TestFollowerIgnoresAStaleSnapshot(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	follower := nw.node(2)
	follower.becomeFollower(5, 1)
	follower.log.Append(Entry{Term: 5}, Entry{Term: 5}, Entry{Term: 5})
	follower.log.CommitTo(3)

	snap := &Snapshot{Meta: SnapshotMeta{Index: 2, Term: 5}, Data: []byte("old")}
	if err := follower.Step(Message{Type: MsgSnapshotReq, From: 1, To: 2, Term: 5, Snapshot: snap}); err != nil {
		t.Fatalf("Step: %v", err)
	}

	if follower.Log().LastIndex() != 3 {
		t.Fatalf("a stale snapshot truncated the log to %d", follower.Log().LastIndex())
	}
	rd := follower.Ready()
	if rd.Snapshot != nil {
		t.Fatal("a stale snapshot was handed to the state machine")
	}
	if len(rd.Messages) != 1 || rd.Messages[0].MatchIndex != 3 {
		t.Fatalf("response = %v, want an acknowledgement at our own commit index", rd.Messages)
	}
}

func TestEmptySnapshotIsRejected(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	follower := nw.node(2)
	follower.becomeFollower(5, 1)

	if err := follower.Step(Message{Type: MsgSnapshotReq, From: 1, To: 2, Term: 5}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	rd := follower.Ready()
	if len(rd.Messages) != 1 || !rd.Messages[0].Reject {
		t.Fatalf("response = %v, want a rejection", rd.Messages)
	}
}

func TestStaleTermSnapshotIsRejected(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	n := nw.node(1)
	n.becomeFollower(9, None)

	snap := &Snapshot{Meta: SnapshotMeta{Index: 100, Term: 3}}
	if err := n.Step(Message{Type: MsgSnapshotReq, From: 2, To: 1, Term: 3, Snapshot: snap}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	rd := n.Ready()
	if len(rd.Messages) != 1 || !rd.Messages[0].Reject || rd.Messages[0].Term != 9 {
		t.Fatalf("response = %v", rd.Messages)
	}
	if n.Log().SnapshotIndex() != 0 {
		t.Fatal("a stale-term snapshot was installed")
	}
}

func TestLeaderResumesReplicationAfterASnapshot(t *testing.T) {
	nw, leader, lagging := buildLeaderWithBacklog(t, 6)
	nw.deliver()
	if err := leader.Compact(leader.Log().Applied(), []byte("image")); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	snapIndex := leader.Snapshot().Meta.Index

	leader.sendAppend(lagging)
	leader.msgs = nil

	// The follower reports it installed the image.
	leader.handleSnapshotResponse(Message{Type: MsgSnapshotResp, From: lagging, MatchIndex: snapIndex})
	p := leader.progress[lagging]
	if p.PendingSnapshot != 0 {
		t.Fatalf("pending snapshot not cleared: %v", p)
	}
	if p.Match != snapIndex {
		t.Fatalf("progress after snapshot = %v, want match %d", p, snapIndex)
	}

	// New writes now replicate normally.
	if _, err := leader.Propose(EntryNormal, []byte("after")); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	var sawAppend bool
	for _, m := range leader.msgs {
		if m.To == lagging && m.Type == MsgAppendReq {
			sawAppend = true
		}
	}
	if !sawAppend {
		t.Fatalf("no append sent after the snapshot: %v", leader.msgs)
	}
}

func TestRejectedSnapshotClearsTheInFlightMarker(t *testing.T) {
	nw, leader, lagging := buildLeaderWithBacklog(t, 4)
	nw.deliver()
	if err := leader.Compact(leader.Log().Applied(), []byte("image")); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	leader.sendAppend(lagging)

	leader.handleSnapshotResponse(Message{Type: MsgSnapshotResp, From: lagging, Reject: true})
	if leader.progress[lagging].PendingSnapshot != 0 {
		t.Fatal("a rejected snapshot left the follower marked as in flight")
	}
	// A response from a server we do not track is simply ignored.
	leader.handleSnapshotResponse(Message{Type: MsgSnapshotResp, From: 99, MatchIndex: 5})
}

func TestLaggingFollowerCatchesUpThroughASnapshot(t *testing.T) {
	// The end-to-end story: a follower is offline while the leader writes and
	// compacts, then comes back to a leader that no longer holds the entries
	// it needs.
	nw, leader, lagging := buildLeaderWithBacklog(t, 10)
	nw.deliver()
	if err := leader.Compact(leader.Log().Applied(), []byte("image at 12")); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if leader.Log().FirstIndex() <= nw.node(lagging).Log().LastIndex()+1 {
		t.Fatal("test setup did not actually put the follower off the end of the leader's log")
	}

	nw.start(lagging)
	nw.tick(10)

	follower := nw.node(lagging)
	if follower.Log().SnapshotIndex() == 0 {
		t.Fatal("follower never installed a snapshot")
	}
	if follower.Log().Committed() != leader.Log().Committed() {
		t.Fatalf("commit indices diverged: follower %d, leader %d",
			follower.Log().Committed(), leader.Log().Committed())
	}

	// And it keeps up with new writes from there.
	if _, err := nw.propose(leader.ID(), "after the snapshot"); err != nil {
		t.Fatalf("propose: %v", err)
	}
	nw.tick(5)
	if follower.Log().LastIndex() != leader.Log().LastIndex() {
		t.Fatalf("follower is at %d, leader at %d", follower.Log().LastIndex(), leader.Log().LastIndex())
	}
}

func TestNodeRestartsFromASnapshotAlone(t *testing.T) {
	cfg := NewConfig(Member{ID: 1, Addr: "a"}, Member{ID: 2, Addr: "b"}, Member{ID: 3, Addr: "c"})
	snap := &Snapshot{Meta: SnapshotMeta{Index: 25, Term: 4, Config: cfg}, Data: []byte("image")}

	n := NewNode(Options{
		ID:        2,
		Snapshot:  snap,
		HardState: HardState{Term: 4, Commit: 25},
		Log:       NewLogFrom(25, 4, nil),
	})

	if n.Log().LastIndex() != 25 || n.Log().Committed() != 25 {
		t.Fatalf("recovered last/commit = %d/%d", n.Log().LastIndex(), n.Log().Committed())
	}
	// With no configuration passed in, membership comes from the snapshot.
	if len(n.Config().Members) != 3 {
		t.Fatalf("configuration = %v", n.Config())
	}
	if n.Snapshot() == nil {
		t.Fatal("node did not retain the snapshot it booted from")
	}
	// The image is already in the state machine, so it must not be replayed.
	if rd := n.Ready(); rd.Snapshot != nil || len(rd.CommittedEntries) != 0 {
		t.Fatalf("restart re-issued state: %+v", rd)
	}
}

func TestSnapshotIsSentWithTheCurrentConfiguration(t *testing.T) {
	nw, leader, _ := buildLeaderWithBacklog(t, 3)
	nw.deliver()
	if err := leader.Compact(leader.Log().Applied(), []byte("x")); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if !leader.Snapshot().Meta.Config.Equal(leader.Config()) {
		t.Fatalf("snapshot config %v != node config %v", leader.Snapshot().Meta.Config, leader.Config())
	}
}

func TestCandidateConcedesToASnapshot(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	n := nw.node(1)
	n.becomeCandidate()

	snap := &Snapshot{Meta: SnapshotMeta{Index: 8, Term: 3, Config: n.Config()}, Data: []byte("image")}
	if err := n.Step(Message{Type: MsgSnapshotReq, From: 2, To: 1, Term: n.Term() + 1, Snapshot: snap}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if n.Role() != Follower || n.Leader() != 2 {
		t.Fatalf("candidate did not concede: role=%s leader=%d", n.Role(), uint64(n.Leader()))
	}
	if n.Log().SnapshotIndex() != 8 {
		t.Fatalf("snapshot was not installed: boundary = %d", n.Log().SnapshotIndex())
	}
}
