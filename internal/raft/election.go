package raft

// handleVoteRequest decides whether to vote for a candidate. The leader lease
// has already been checked in Step; what is left are the two gates from the
// paper: one vote per term, and the log completeness restriction that
// guarantees a committed entry can never be lost.
func (n *Node) handleVoteRequest(m Message) {
	canVote := false
	switch {
	case m.PreVote:
		// A pre-vote costs nothing and binds nobody, so it is granted purely
		// on whether the candidate could plausibly win.
		canVote = m.Term > n.term
	case n.vote == m.From:
		canVote = true // repeat request from whoever we already backed
	case n.vote == None && n.lead == None:
		canVote = true // no vote cast in this term yet
	}

	if canVote && n.log.IsUpToDate(m.LastLogIndex, m.LastLogTerm) {
		// A granted response carries the term the candidate asked about. For a
		// pre-vote that term does not exist yet, which is exactly why the
		// candidate must not treat it as evidence of a higher term.
		n.send(Message{Type: MsgVoteResp, To: m.From, Term: m.Term, PreVote: m.PreVote})
		if !m.PreVote {
			n.vote = m.From
			n.electionElapsed = 0
			n.logger.Debug("granted vote", "id", uint64(n.id), "term", n.term, "candidate", uint64(m.From))
		}
		return
	}

	n.send(Message{Type: MsgVoteResp, To: m.From, PreVote: m.PreVote, Reject: true})
}

func (n *Node) handleVoteResponse(m Message) {
	// Pre-vote and real-vote rounds must not be allowed to cross-count: a
	// stale response from the pre-vote round would otherwise help decide the
	// real election.
	if (n.role == PreCandidate) != m.PreVote {
		return
	}

	won, decided := n.poll(m.From, !m.Reject)
	if !decided {
		return
	}
	if won {
		n.electionWon(n.role == PreCandidate)
		return
	}
	// A majority said no. Stop campaigning and wait for a leader to appear,
	// rather than burning through terms with elections that cannot succeed.
	n.logger.Debug("election lost", "id", uint64(n.id), "term", n.term)
	n.becomeFollower(n.term, None)
}

// poll records a vote and reports whether the election is decided, and if so
// whether it was won. Counting both ways matters: recognising a loss early
// lets a candidate step down instead of waiting out another timeout.
func (n *Node) poll(from NodeID, granted bool) (won bool, decided bool) {
	if _, seen := n.votes[from]; !seen {
		n.votes[from] = granted
	}

	var yes, no int
	for id, v := range n.votes {
		if !n.cfg.IsVoter(id) {
			continue // a learner's opinion does not count
		}
		if v {
			yes++
		} else {
			no++
		}
	}

	quorum := n.cfg.Quorum()
	switch {
	case yes >= quorum:
		return true, true
	case no >= quorum:
		return false, true
	default:
		return false, false
	}
}

// electionWon advances a pre-vote win into a real election, or a real win into
// leadership.
func (n *Node) electionWon(wasPreVote bool) {
	if wasPreVote {
		n.becomeCandidate()
		n.solicitVotes(false)
		return
	}
	n.becomeLeader()
}
