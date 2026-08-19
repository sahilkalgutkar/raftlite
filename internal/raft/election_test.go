package raft

import "testing"

func TestSingleVoterWinsWithoutMessages(t *testing.T) {
	nw := newNetwork(t, 1)
	n := nw.node(1)

	n.Campaign()
	if n.Role() != Leader {
		t.Fatalf("role = %s, want leader", n.Role())
	}
	if n.Term() != 1 {
		t.Fatalf("term = %d, want 1", n.Term())
	}
	rd := n.Ready()
	if len(rd.Messages) != 0 {
		t.Fatalf("a single-node cluster sent %d messages", len(rd.Messages))
	}
	if rd.HardState == nil || rd.HardState.Vote != 1 {
		t.Fatalf("hard state = %v, want a self-vote", rd.HardState)
	}
}

func TestElectionProducesExactlyOneLeader(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)

	followers := 0
	for _, id := range nw.order {
		n := nw.node(id)
		if n.ID() == leader.ID() {
			continue
		}
		followers++
		if n.Role() != Follower {
			t.Fatalf("node %d is %s, want follower", uint64(id), n.Role())
		}
		if n.Term() != leader.Term() {
			t.Fatalf("node %d term %d != leader term %d", uint64(id), n.Term(), leader.Term())
		}
		if n.Leader() != leader.ID() {
			t.Fatalf("node %d follows %d, want %d", uint64(id), uint64(n.Leader()), uint64(leader.ID()))
		}
	}
	if followers != 2 {
		t.Fatalf("expected 2 followers, got %d", followers)
	}
}

func TestElectionInFiveNodeCluster(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3, 4, 5)
	leader := nw.tickUntilLeader(200)
	if got := leader.Config().Quorum(); got != 3 {
		t.Fatalf("quorum = %d, want 3", got)
	}
}

func TestPreVoteKeepsAPartitionedNodeFromBumpingTheTerm(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)
	startTerm := leader.Term()

	// Pick a follower and cut it off. It will campaign over and over, but the
	// pre-vote round never reaches anyone, so its term must not move.
	var victim NodeID
	for _, id := range nw.order {
		if id != leader.ID() {
			victim = id
			break
		}
	}
	nw.isolate(victim)
	nw.tick(120)

	if got := nw.node(victim).Term(); got != startTerm {
		t.Fatalf("isolated node's term moved from %d to %d despite pre-vote", startTerm, got)
	}
	if nw.node(victim).Role() == Leader {
		t.Fatal("isolated node elected itself leader of a one-node partition")
	}

	// Healing must not disturb the sitting leader.
	nw.heal()
	nw.tick(10)
	if nw.node(leader.ID()).Role() != Leader || nw.node(leader.ID()).Term() != startTerm {
		t.Fatalf("rejoining node disrupted the leader: role=%s term=%d",
			nw.node(leader.ID()).Role(), nw.node(leader.ID()).Term())
	}
}

func TestWithoutPreVoteAPartitionedNodeRunsUpTheTerm(t *testing.T) {
	// The mirror image of the test above: with pre-vote off, the isolated node
	// burns through terms, and rejoining forces the healthy leader to step
	// down and hold a fresh election. This is the disruption pre-vote exists
	// to prevent.
	nw := newNetworkWith(t, networkOpts{preVoteDisabled: true}, 1, 2, 3)
	leader := nw.tickUntilLeader(100)
	startTerm := leader.Term()

	var victim NodeID
	for _, id := range nw.order {
		if id != leader.ID() {
			victim = id
			break
		}
	}
	nw.isolate(victim)
	nw.tick(120)

	if got := nw.node(victim).Term(); got <= startTerm {
		t.Fatalf("term stayed at %d without pre-vote; expected it to climb past %d", got, startTerm)
	}
}

func TestVoteRefusedToNodeWithStaleLog(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	voter := nw.node(2)
	voter.log.Append(Entry{Term: 5}, Entry{Term: 5})
	voter.term = 5

	// A candidate whose log ends earlier in the same term must be refused.
	if err := voter.Step(Message{
		Type: MsgVoteReq, From: 1, To: 2, Term: 6, LastLogIndex: 1, LastLogTerm: 5,
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	rd := voter.Ready()
	if len(rd.Messages) != 1 || !rd.Messages[0].Reject {
		t.Fatalf("stale candidate was not rejected: %v", rd.Messages)
	}

	// The same candidate with a higher final term wins the vote even though
	// its log is shorter -- terms dominate length.
	if err := voter.Step(Message{
		Type: MsgVoteReq, From: 3, To: 2, Term: 7, LastLogIndex: 1, LastLogTerm: 6,
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	rd = voter.Ready()
	if len(rd.Messages) != 1 || rd.Messages[0].Reject {
		t.Fatalf("up-to-date candidate was rejected: %v", rd.Messages)
	}
	if voter.vote != 3 {
		t.Fatalf("vote recorded as %d, want 3", uint64(voter.vote))
	}
}

func TestOneVotePerTerm(t *testing.T) {
	n := newNetwork(t, 1, 2, 3).node(1)

	must := func(from NodeID, term uint64) Message {
		t.Helper()
		if err := n.Step(Message{Type: MsgVoteReq, From: from, To: 1, Term: term}); err != nil {
			t.Fatalf("Step: %v", err)
		}
		rd := n.Ready()
		if len(rd.Messages) != 1 {
			t.Fatalf("expected exactly one response, got %v", rd.Messages)
		}
		return rd.Messages[0]
	}

	if resp := must(2, 1); resp.Reject {
		t.Fatal("first vote request in a term was rejected")
	}
	if resp := must(2, 1); resp.Reject {
		t.Fatal("repeat request from the same candidate was rejected")
	}
	if resp := must(3, 1); !resp.Reject {
		t.Fatal("a second candidate got a vote in the same term")
	}
	if resp := must(3, 2); resp.Reject {
		t.Fatal("a new term should free the vote")
	}
}

func TestLeaderLeaseRejectsDisruptiveCandidate(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)

	var follower *Node
	for _, id := range nw.order {
		if id != leader.ID() {
			follower = nw.node(id)
			break
		}
	}

	// A candidate arrives claiming a much higher term, immediately after the
	// follower heard from its leader. The lease check refuses it.
	err := follower.Step(Message{
		Type: MsgVoteReq, From: leader.ID() + 100, To: follower.ID(),
		Term: leader.Term() + 5, LastLogIndex: 99, LastLogTerm: 99,
	})
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	rd := follower.Ready()
	if len(rd.Messages) != 1 || !rd.Messages[0].Reject {
		t.Fatalf("lease did not reject the candidate: %v", rd.Messages)
	}
	if follower.Leader() != leader.ID() {
		t.Fatalf("follower abandoned its leader: now follows %d", uint64(follower.Leader()))
	}
}

func TestCandidateConcedesToAHeartbeat(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	n := nw.node(1)

	nw.isolate(1) // campaign without anyone answering
	n.Campaign()
	if n.Role() != PreCandidate {
		t.Fatalf("role = %s, want pre-candidate", n.Role())
	}
	// Force a real election so the node is a full candidate.
	n.becomeCandidate()
	if n.Role() != Candidate {
		t.Fatalf("role = %s, want candidate", n.Role())
	}

	if err := n.Step(Message{Type: MsgHeartbeatReq, From: 2, To: 1, Term: n.Term() + 1}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if n.Role() != Follower || n.Leader() != 2 {
		t.Fatalf("candidate did not concede: role=%s leader=%d", n.Role(), uint64(n.Leader()))
	}
}

func TestCandidateLosingMajorityStepsDown(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	n := nw.node(1)
	n.becomeCandidate()
	term := n.Term()

	for _, from := range []NodeID{2, 3} {
		if err := n.Step(Message{Type: MsgVoteResp, From: from, To: 1, Term: term, Reject: true}); err != nil {
			t.Fatalf("Step: %v", err)
		}
	}
	if n.Role() != Follower {
		t.Fatalf("role = %s, want follower after losing", n.Role())
	}
	if n.Term() != term {
		t.Fatalf("term changed to %d on a lost election", n.Term())
	}
}

func TestPreVoteAndRealVoteTalliesDoNotMix(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	n := nw.node(1)
	n.becomePreCandidate()

	// A response from the real-vote round must be ignored while pre-voting.
	if err := n.Step(Message{Type: MsgVoteResp, From: 2, To: 1, Term: n.Term(), PreVote: false}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if n.Role() != PreCandidate {
		t.Fatalf("role = %s: a real-vote response decided the pre-vote round", n.Role())
	}

	// And the pre-vote grant does advance it.
	if err := n.Step(Message{Type: MsgVoteResp, From: 2, To: 1, Term: n.Term() + 1, PreVote: true}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if n.Role() != Candidate {
		t.Fatalf("role = %s, want candidate once the pre-vote carried", n.Role())
	}
}

func TestGrantedPreVoteDoesNotAdvanceTheCandidatesTerm(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	n := nw.node(1)
	term := n.Term()

	n.becomePreCandidate()
	if n.Term() != term {
		t.Fatalf("pre-candidate moved the term to %d", n.Term())
	}
	// A grant carries term+1, which must not be mistaken for a real term.
	if err := n.Step(Message{Type: MsgVoteResp, From: 2, To: 1, Term: term + 1, PreVote: true}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if n.Term() != term+1 || n.Role() != Candidate {
		t.Fatalf("after winning pre-vote: term=%d role=%s", n.Term(), n.Role())
	}
}

func TestStaleTermMessagesGetAnswered(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	n := nw.node(1)
	n.becomeFollower(9, None)

	cases := []struct {
		name string
		msg  Message
		want MessageType
	}{
		{"stale heartbeat", Message{Type: MsgHeartbeatReq, From: 2, To: 1, Term: 3}, MsgHeartbeatResp},
		{"stale vote", Message{Type: MsgVoteReq, From: 2, To: 1, Term: 3}, MsgVoteResp},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := n.Step(tc.msg); err != nil {
				t.Fatalf("Step: %v", err)
			}
			rd := n.Ready()
			if len(rd.Messages) != 1 {
				t.Fatalf("expected one reply, got %v", rd.Messages)
			}
			got := rd.Messages[0]
			if got.Type != tc.want || !got.Reject || got.Term != 9 {
				t.Fatalf("reply = %v, want a rejection carrying term 9", got)
			}
		})
	}

	// An unroutable stale message type is simply dropped.
	if err := n.Step(Message{Type: MsgHeartbeatResp, From: 2, To: 1, Term: 3}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if rd := n.Ready(); len(rd.Messages) != 0 {
		t.Fatalf("stale response produced replies: %v", rd.Messages)
	}
}

func TestLeaderStepsDownOnHigherTerm(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)

	err := leader.Step(Message{Type: MsgHeartbeatReq, From: 99, To: leader.ID(), Term: leader.Term() + 1})
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if leader.Role() != Follower {
		t.Fatalf("role = %s, want follower", leader.Role())
	}
	if leader.Leader() != 99 {
		t.Fatalf("leader = %d, want 99", uint64(leader.Leader()))
	}
}

func TestLearnerNeverCampaigns(t *testing.T) {
	nw := newNetworkWith(t, networkOpts{learners: []NodeID{3}}, 1, 2, 3)
	learner := nw.node(3)

	learner.Campaign()
	if learner.Role() != Follower {
		t.Fatalf("learner became %s", learner.Role())
	}
	nw.tick(60)
	if learner.Role() != Follower {
		t.Fatalf("learner became %s after timing out", learner.Role())
	}
	// The cluster still elects a leader from the two voters.
	if l := nw.mustLeader(); l.ID() == 3 {
		t.Fatal("a learner won an election")
	}
}

func TestLearnerVoteIsNotCounted(t *testing.T) {
	nw := newNetworkWith(t, networkOpts{learners: []NodeID{3}}, 1, 2, 3)
	n := nw.node(1)
	n.becomeCandidate() // votes for itself; quorum of two voters is 2

	if err := n.Step(Message{Type: MsgVoteResp, From: 3, To: 1, Term: n.Term()}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if n.Role() == Leader {
		t.Fatal("a learner's vote carried the election")
	}
	if err := n.Step(Message{Type: MsgVoteResp, From: 2, To: 1, Term: n.Term()}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if n.Role() != Leader {
		t.Fatalf("role = %s after a real voter answered", n.Role())
	}
}

func TestElectionTimeoutIsRandomised(t *testing.T) {
	seen := map[int]bool{}
	n := newNetwork(t, 1, 2, 3).node(1)
	for i := 0; i < 50; i++ {
		n.resetElectionTimeout()
		if n.electionTimeout < n.electionTicks || n.electionTimeout >= 2*n.electionTicks {
			t.Fatalf("timeout %d outside [%d,%d)", n.electionTimeout, n.electionTicks, 2*n.electionTicks)
		}
		seen[n.electionTimeout] = true
	}
	if len(seen) < 3 {
		t.Fatalf("only %d distinct timeouts in 50 draws: jitter is not working", len(seen))
	}
}

func TestNodeRestoresTermAndVote(t *testing.T) {
	l := NewLog()
	l.Append(Entry{Term: 4}, Entry{Term: 4})
	n := NewNode(Options{
		ID:        1,
		Config:    NewConfig(Member{ID: 1}, Member{ID: 2}, Member{ID: 3}),
		HardState: HardState{Term: 4, Vote: 2, Commit: 1},
		Log:       l,
	})

	if n.Term() != 4 || n.vote != 2 {
		t.Fatalf("recovered term/vote = %d/%d", n.Term(), uint64(n.vote))
	}
	if n.log.Committed() != 1 {
		t.Fatalf("recovered commit = %d", n.log.Committed())
	}
	// Recovered entries are already durable, so they must not come back out of
	// Ready as work to persist.
	rd := n.Ready()
	if len(rd.Entries) != 0 {
		t.Fatalf("Ready re-issued %d recovered entries", len(rd.Entries))
	}
	if len(rd.CommittedEntries) != 1 {
		t.Fatalf("expected the committed entry to be replayed to the state machine, got %d", len(rd.CommittedEntries))
	}
	if n.String() == "" {
		t.Fatal("node has no string form")
	}
}

func TestReadyReportsHardStateOnlyWhenItChanges(t *testing.T) {
	n := newNetwork(t, 1, 2, 3).node(1)
	n.Ready() // drain the initial state

	if n.HasReady() {
		t.Fatal("a quiet node reports pending work")
	}
	n.becomeCandidate()
	if !n.HasReady() {
		t.Fatal("a term change is not reported as work")
	}
	rd := n.Ready()
	if rd.HardState == nil || rd.HardState.Term != 1 || rd.HardState.Vote != 1 {
		t.Fatalf("hard state = %v", rd.HardState)
	}
	if rd.IsEmpty() {
		t.Fatal("Ready with a hard state reported itself empty")
	}
	if next := n.Ready(); next.HardState != nil {
		t.Fatalf("unchanged hard state reported again: %v", next.HardState)
	}
	if !n.Ready().IsEmpty() {
		t.Fatal("drained node still reports work")
	}
}

func TestMessageAndStatusFormatting(t *testing.T) {
	m := Message{Type: MsgVoteReq, From: 1, To: 2, Term: 3, PreVote: true, Reject: true}
	if m.String() == "" || MessageType(99).String() == "" {
		t.Fatal("message has no string form")
	}

	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)
	st := leader.Status()
	if st.ID != leader.ID() || st.Role != Leader || st.Term != leader.Term() {
		t.Fatalf("status = %+v", st)
	}
	if len(st.Config.Members) != 3 {
		t.Fatalf("status config = %v", st.Config)
	}
}

func TestHeartbeatCarriesCommitIndexForward(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := nw.tickUntilLeader(100)
	follower := nw.node(leader.ID()%3 + 1)

	// Both sides hold the same two entries; only the leader knows they are
	// committed. The heartbeat is what tells the follower.
	leader.log.Append(Entry{Term: leader.Term()}, Entry{Term: leader.Term()})
	follower.log.Append(Entry{Term: leader.Term()}, Entry{Term: leader.Term()})
	leader.log.CommitTo(2)

	leader.bcastHeartbeat()
	nw.deliver()

	if got := follower.log.Committed(); got != 2 {
		t.Fatalf("follower commit = %d, want 2", got)
	}
}

func TestCommitFromLeaderNeverPassesOurOwnLog(t *testing.T) {
	n := newNetwork(t, 1, 2, 3).node(1)
	n.log.Append(Entry{Term: 1})

	n.commitFromLeader(99)
	if got := n.log.Committed(); got != 1 {
		t.Fatalf("commit = %d, want it clamped to our last index 1", got)
	}
}
