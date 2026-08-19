package raft

import "fmt"

// Membership changes go through the log like any other write, which is what
// makes them agree with everything else that is replicated. The dangerous part
// is that the configuration decides what a quorum *is*, so for a moment two
// nodes can be using different rules for counting votes.
//
// Raft's answer, and the one implemented here, is to only ever change one
// server at a time. Any old configuration and any new configuration that
// differ by a single server necessarily share a majority, so the two can never
// each elect a leader in the same term. That single restriction removes the
// need for joint consensus entirely -- at the cost of not being able to swap
// two servers in one step, which is a trade I will take for something I have
// to be able to reason about.

// ProposeConfChange asks the cluster to add, promote or remove one server.
func (n *Node) ProposeConfChange(cc ConfChange) (uint64, error) {
	if n.role != Leader {
		return 0, ErrNotLeader
	}
	// Only one change may be in flight. Two overlapping single-server changes
	// can compose into a two-server difference, which is exactly the case the
	// one-at-a-time rule exists to prevent.
	if n.pendingConfIndex > n.log.Committed() {
		return 0, fmt.Errorf("%w: change at index %d is not committed yet",
			ErrConfChangeInFlight, n.pendingConfIndex)
	}
	// Reject an impossible change now, at the caller, rather than replicating
	// it and having every node quietly refuse to apply it.
	if _, err := n.cfg.Apply(cc); err != nil {
		return 0, err
	}

	idx := n.appendOwn(Entry{Term: n.term, Type: EntryConfChange, Data: cc.Marshal()})
	n.pendingConfIndex = idx
	n.bcastAppend()
	n.logger.Info("proposed configuration change",
		"id", uint64(n.id), "change", cc.String(), "index", idx)
	return idx, nil
}

// applyConfChangeEntry folds a committed membership change into the live
// configuration. Every node runs this, not just the leader: the configuration
// has to be identical everywhere or two nodes will disagree about what counts
// as a majority.
func (n *Node) applyConfChangeEntry(e Entry) {
	cc, err := UnmarshalConfChange(e.Data)
	if err != nil {
		n.logger.Error("committed an undecodable configuration change",
			"id", uint64(n.id), "index", e.Index, "err", err)
		return
	}
	next, err := n.cfg.Apply(cc)
	if err != nil {
		n.logger.Error("committed configuration change is not applicable",
			"id", uint64(n.id), "index", e.Index, "change", cc.String(), "err", err)
		return
	}

	n.cfg = next
	n.logger.Info("applied configuration change",
		"id", uint64(n.id), "index", e.Index, "change", cc.String(), "config", next.String())

	if n.role != Leader {
		return
	}
	n.syncProgress()

	// A leader that removed itself is no longer allowed to lead. It has to
	// keep serving until this entry is committed -- which it just was -- and
	// then get out of the way.
	if !n.cfg.Has(n.id) {
		n.logger.Info("stepping down: removed from the configuration", "id", uint64(n.id))
		n.becomeFollower(n.term, None)
		return
	}
	// Removing a server shrinks the quorum, which can make an entry that was
	// one acknowledgement short committable straight away.
	if n.maybeCommit() {
		n.bcastAppend()
	}
}

// syncProgress reconciles the leader's per-follower tracking with the
// configuration: new members start from scratch, departed ones are dropped.
func (n *Node) syncProgress() {
	if n.progress == nil {
		return
	}
	for _, m := range n.cfg.Members {
		p, ok := n.progress[m.ID]
		if !ok {
			// A server that just joined holds nothing, so start probing from
			// the beginning; it will either be caught up with entries or, if
			// the leader has compacted past them, handed a snapshot.
			n.progress[m.ID] = &Progress{Next: 1, Learner: m.Learner, RecentActive: true}
			n.logger.Info("tracking a new member",
				"id", uint64(n.id), "member", uint64(m.ID), "learner", m.Learner)
			continue
		}
		p.Learner = m.Learner
	}
	for id := range n.progress {
		if !n.cfg.Has(id) {
			delete(n.progress, id)
		}
	}
}
