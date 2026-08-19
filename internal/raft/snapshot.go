package raft

// Snapshots exist because a log that only ever grows is a log that eventually
// fills the disk and makes restarts take longer than the outage did. A
// snapshot is the state machine's contents at one index; once it is durable,
// every entry at or below that index can be thrown away.
//
// They are also the only way to catch up a replica that has fallen further
// behind than the leader's retained log reaches -- a node that was down for an
// hour, or a brand new server joining an old cluster.

// Compact records a snapshot the state machine produced at the given index and
// discards the log prefix it covers. The index must already be applied: the
// snapshot has to describe state the state machine actually reached.
func (n *Node) Compact(index uint64, data []byte) error {
	if index > n.log.Applied() {
		return ErrUnavailable
	}
	term, err := n.log.Term(index)
	if err != nil {
		return err
	}
	if err := n.log.Compact(index); err != nil {
		return err
	}
	n.snapshot = &Snapshot{
		Meta: SnapshotMeta{Index: index, Term: term, Config: n.cfg.Clone()},
		Data: data,
	}
	n.logger.Info("compacted log into a snapshot",
		"id", uint64(n.id), "index", index, "term", term, "bytes", len(data))
	return nil
}

// Snapshot returns the newest snapshot this node holds, or nil.
func (n *Node) Snapshot() *Snapshot { return n.snapshot }

// sendSnapshot ships the whole state machine to a follower whose next entry
// the leader no longer has.
func (n *Node) sendSnapshot(to NodeID) {
	p := n.progress[to]
	if p == nil {
		return
	}
	if n.snapshot.IsEmpty() {
		// The log has been compacted but no snapshot is available to send.
		// That should not happen -- compaction is what produces one -- so say
		// so rather than silently leaving the follower stuck.
		n.logger.Error("follower needs a snapshot but none is available",
			"id", uint64(n.id), "follower", uint64(to), "next", p.Next)
		return
	}

	p.PendingSnapshot = n.snapshot.Meta.Index
	n.logger.Info("sending snapshot",
		"id", uint64(n.id), "follower", uint64(to),
		"index", n.snapshot.Meta.Index, "bytes", len(n.snapshot.Data))
	n.send(Message{Type: MsgSnapshotReq, To: to, Snapshot: n.snapshot})
}

// handleSnapshot is the follower side of InstallSnapshot.
func (n *Node) handleSnapshot(m Message) {
	n.electionElapsed = 0
	n.lead = m.From

	if m.Snapshot.IsEmpty() {
		n.send(Message{Type: MsgSnapshotResp, To: m.From, Reject: true})
		return
	}

	// A snapshot that ends at or before our commit index tells us nothing we
	// have not already applied. Accepting it would throw away log entries we
	// still hold, so just report where we are.
	if m.Snapshot.Meta.Index <= n.log.Committed() {
		n.logger.Debug("ignoring a stale snapshot",
			"id", uint64(n.id), "snapshot", m.Snapshot.Meta.Index, "commit", n.log.Committed())
		n.send(Message{Type: MsgSnapshotResp, To: m.From, MatchIndex: n.log.Committed()})
		return
	}

	n.log.Restore(m.Snapshot.Meta)
	// The snapshot carries the configuration in force when it was taken, which
	// may well not be the one we booted with.
	n.cfg = m.Snapshot.Meta.Config.Clone()
	n.snapshot = m.Snapshot
	n.pendingSnapshot = m.Snapshot
	n.unstable = n.log.LastIndex() + 1

	n.logger.Info("installed snapshot from leader",
		"id", uint64(n.id), "leader", uint64(m.From),
		"index", m.Snapshot.Meta.Index, "term", m.Snapshot.Meta.Term)
	n.send(Message{Type: MsgSnapshotResp, To: m.From, MatchIndex: m.Snapshot.Meta.Index})
}

// handleSnapshotResponse is the leader side: the follower has caught up to the
// snapshot, so normal replication can resume from there.
func (n *Node) handleSnapshotResponse(m Message) {
	p := n.progress[m.From]
	if p == nil {
		return
	}
	p.RecentActive = true
	p.PendingSnapshot = 0

	if m.Reject {
		n.logger.Warn("follower rejected a snapshot", "id", uint64(n.id), "follower", uint64(m.From))
		return
	}
	if p.maybeUpdate(m.MatchIndex) {
		n.maybeCommit()
	}
	if p.Next <= n.log.LastIndex() {
		n.sendAppend(m.From)
	}
}

func (n *Node) hasPendingSnapshot() bool { return n.pendingSnapshot != nil }

func (n *Node) takePendingSnapshot() *Snapshot {
	snap := n.pendingSnapshot
	n.pendingSnapshot = nil
	return snap
}
