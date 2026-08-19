package raft

import "sort"

// maxAppendEntries bounds how many entries ride in one AppendEntries message.
// Without a cap, a follower that has been down for an hour gets sent its whole
// backlog in a single message, which is the kind of thing that turns a
// recovering node into an out-of-memory event on the leader.
const maxAppendEntries = 64

// Propose appends a command to the log. Only a leader can accept one, and the
// caller learns the index it landed at so it can wait for that index to be
// applied.
func (n *Node) Propose(typ EntryType, data []byte) (uint64, error) {
	if n.role != Leader {
		return 0, ErrNotLeader
	}
	idx := n.appendOwn(Entry{Term: n.term, Type: typ, Data: data})
	n.bcastAppend()
	return idx, nil
}

// appendOwn appends the leader's own entries and books them as replicated on
// itself -- the leader's disk is one of the quorum's copies.
func (n *Node) appendOwn(ents ...Entry) uint64 {
	last := n.log.Append(ents...)
	if p := n.progress[n.id]; p != nil {
		p.maybeUpdate(last)
	}
	n.maybeCommit()
	return last
}

// onBecomeLeader resets per-follower progress and appends the no-op entry that
// makes the new term's commit index trustworthy.
//
// The no-op is not ceremony. A leader may not commit an entry from an earlier
// term just because it is replicated on a majority (paper, section 5.4.2) --
// doing so can un-commit an entry a later leader overwrites. Committing one
// entry of its own term is what unlocks everything before it.
func (n *Node) onBecomeLeader() {
	n.progress = make(map[NodeID]*Progress, len(n.cfg.Members))
	last := n.log.LastIndex()
	for _, m := range n.cfg.Members {
		p := &Progress{Next: last + 1, Learner: m.Learner}
		if m.ID == n.id {
			p.Match = last
			p.RecentActive = true
		}
		n.progress[m.ID] = p
	}
	n.appendOwn(Entry{Term: n.term, Type: EntryNoOp})
	n.bcastAppend()
}

func (n *Node) bcastAppend() {
	for _, m := range n.cfg.Members {
		if m.ID == n.id {
			continue
		}
		n.sendAppend(m.ID)
	}
}

// sendAppend streams whatever the follower is missing. If the entries it needs
// have already been compacted away, it gets a snapshot instead.
func (n *Node) sendAppend(to NodeID) {
	p := n.progress[to]
	if p == nil || p.PendingSnapshot > 0 {
		return
	}

	prevIndex := p.Next - 1
	prevTerm, err := n.log.Term(prevIndex)
	if err != nil {
		n.sendSnapshot(to)
		return
	}

	hi := n.log.LastIndex() + 1
	if hi > p.Next+maxAppendEntries {
		hi = p.Next + maxAppendEntries
	}
	ents, err := n.log.Slice(p.Next, hi)
	if err != nil {
		n.sendSnapshot(to)
		return
	}

	n.send(Message{
		Type:         MsgAppendReq,
		To:           to,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      ents,
		Commit:       n.log.Committed(),
	})
}

// handleAppendRequest is the follower side of replication.
func (n *Node) handleAppendRequest(m Message) {
	n.electionElapsed = 0
	n.lead = m.From

	// Anything at or below our snapshot boundary is settled history; there is
	// nothing to verify, so just tell the leader where we actually are.
	if m.PrevLogIndex < n.log.SnapshotIndex() {
		n.send(Message{Type: MsgAppendResp, To: m.From, MatchIndex: n.log.SnapshotIndex()})
		return
	}

	if last, ok := n.log.MaybeAppend(m.PrevLogIndex, m.PrevLogTerm, m.Entries); ok {
		// Commit only up to the last entry we actually hold: the leader's
		// commit index may already cover entries still in flight to us.
		commit := m.Commit
		if commit > last {
			commit = last
		}
		n.commitFromLeader(commit)
		n.send(Message{Type: MsgAppendResp, To: m.From, MatchIndex: last})
		return
	}

	idx, term := n.conflictHint(m.PrevLogIndex)
	n.logger.Debug("rejecting append",
		"id", uint64(n.id), "prev_index", m.PrevLogIndex, "hint_index", idx, "hint_term", term)
	n.send(Message{
		Type:          MsgAppendResp,
		To:            m.From,
		Reject:        true,
		ConflictIndex: idx,
		ConflictTerm:  term,
	})
}

// conflictHint tells the leader where to resume from after a rejection.
//
// Backing up one index per round trip costs a round trip per missing entry,
// which is brutal for a follower that has been offline. Reporting the first
// index of the conflicting term lets the leader skip the entire term in one
// step (paper, section 5.3).
func (n *Node) conflictHint(prevIndex uint64) (index, term uint64) {
	if prevIndex > n.log.LastIndex() {
		// Our log is simply too short: point at the end of it.
		return n.log.LastIndex() + 1, 0
	}
	conflictTerm, err := n.log.Term(prevIndex)
	if err != nil {
		return n.log.LastIndex() + 1, 0
	}
	// Walk back to the first entry of that term.
	first := n.log.FirstIndex()
	idx := prevIndex
	for idx > first {
		t, err := n.log.Term(idx - 1)
		if err != nil || t != conflictTerm {
			break
		}
		idx--
	}
	return idx, conflictTerm
}

// handleAppendResponse is the leader side: advance the follower's progress on
// success, or back up and retry on rejection.
func (n *Node) handleAppendResponse(m Message) {
	p := n.progress[m.From]
	if p == nil {
		return // a server that is no longer in the configuration
	}
	p.RecentActive = true

	if m.Reject {
		next := n.nextIndexFromHint(m)
		if p.maybeDecrTo(next) {
			n.sendAppend(m.From)
		}
		return
	}

	if p.maybeUpdate(m.MatchIndex) && n.maybeCommit() {
		// The commit index moved: tell everyone, so followers can apply.
		n.bcastAppend()
	}
	// Keep streaming while the follower is behind.
	if p.Next <= n.log.LastIndex() {
		n.sendAppend(m.From)
	}
}

// nextIndexFromHint turns a follower's conflict hint into the next index to
// try. When the leader also has entries from the conflicting term, it can
// resume just after its own last one; otherwise it takes the follower's word.
func (n *Node) nextIndexFromHint(m Message) uint64 {
	if m.ConflictTerm == 0 {
		return m.ConflictIndex
	}
	if idx, ok := n.lastIndexOfTerm(m.ConflictTerm); ok {
		return idx + 1
	}
	return m.ConflictIndex
}

func (n *Node) lastIndexOfTerm(term uint64) (uint64, bool) {
	for i := n.log.LastIndex(); i >= n.log.FirstIndex(); i-- {
		t, err := n.log.Term(i)
		if err != nil {
			return 0, false
		}
		if t == term {
			return i, true
		}
		if t < term {
			return 0, false
		}
	}
	return 0, false
}

// maybeCommit advances the commit index to the highest index stored on a
// quorum of voters, subject to the current-term rule.
func (n *Node) maybeCommit() bool {
	voters := n.cfg.Voters()
	if len(voters) == 0 {
		return false
	}
	matches := make([]uint64, 0, len(voters))
	for _, id := range voters {
		if p := n.progress[id]; p != nil {
			matches = append(matches, p.Match)
		} else {
			matches = append(matches, 0)
		}
	}
	// The quorum-th largest match index is replicated on a majority.
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	candidate := matches[n.cfg.Quorum()-1]
	if candidate <= n.log.Committed() {
		return false
	}

	// Section 5.4.2: a leader only commits by counting replicas for entries
	// from its own term. Older entries ride along once one of ours commits.
	term, err := n.log.Term(candidate)
	if err != nil || term != n.term {
		return false
	}
	return n.log.CommitTo(candidate)
}

// checkQuorum makes a leader that has lost contact with a majority step down.
// Without it a partitioned leader keeps believing it is in charge and keeps
// answering reads from a state the rest of the cluster has moved past.
func (n *Node) checkQuorum() {
	active := 0
	for _, id := range n.cfg.Voters() {
		p := n.progress[id]
		if p == nil {
			continue
		}
		if id == n.id || p.RecentActive {
			active++
		}
		if id != n.id {
			p.RecentActive = false
		}
	}
	if active < n.cfg.Quorum() {
		n.logger.Warn("stepping down: lost contact with a quorum",
			"id", uint64(n.id), "term", n.term, "active", active, "quorum", n.cfg.Quorum())
		n.becomeFollower(n.term, None)
	}
}

func (n *Node) handleHeartbeatResponse(m Message) {
	p := n.progress[m.From]
	if p == nil {
		return
	}
	p.RecentActive = true
	if p.Match < n.log.LastIndex() {
		n.sendAppend(m.From)
	}
}

func (n *Node) progressSnapshot() map[NodeID]Progress {
	if n.progress == nil {
		return nil
	}
	out := make(map[NodeID]Progress, len(n.progress))
	for id, p := range n.progress {
		out[id] = *p
	}
	return out
}
