package raft

// Linearizable reads.
//
// Serving a read straight off the leader's state machine looks safe and is
// not. A leader that has been partitioned away does not know it yet: it can
// answer from state the rest of the cluster has already moved past, which is a
// stale read that no amount of retrying reveals. Routing reads through the log
// as entries would fix it, at the cost of a disk write and a full round of
// replication for every read.
//
// ReadIndex is the middle path, from section 6.4 of the dissertation. The
// leader notes the current commit index, confirms with a heartbeat round that
// it is still the leader *right now*, and then serves the read once the state
// machine has applied up to that index. No disk write, no log entry, and the
// answer provably includes everything committed before the read began.

// readRequest is one in-flight confirmation round.
type readRequest struct {
	id    uint64
	index uint64
	acks  map[NodeID]bool
}

// ReadIndex starts a linearizable read. The id is opaque and comes back on the
// matching ReadState once a quorum has confirmed this node still leads.
func (n *Node) ReadIndex(id uint64) error {
	if n.role != Leader {
		return ErrNotLeader
	}
	// The commit index is only meaningful once this leader has committed an
	// entry of its own term. Before that it may have inherited a commit index
	// it cannot yet vouch for, which is what the no-op entry exists to settle.
	if term, err := n.log.Term(n.log.Committed()); err != nil || term != n.term {
		return ErrReadIndexUnavailable
	}

	r := &readRequest{id: id, index: n.log.Committed(), acks: map[NodeID]bool{n.id: true}}
	if len(r.acks) >= n.cfg.Quorum() {
		// A single-voter cluster is its own quorum: nothing to confirm.
		n.readStates = append(n.readStates, ReadState{ID: id, Index: r.index})
		return nil
	}

	n.pendingReads = append(n.pendingReads, r)
	for _, id := range n.cfg.Voters() {
		if id == n.id {
			continue
		}
		commit := n.log.Committed()
		if p := n.progress[id]; p != nil && p.Match < commit {
			commit = p.Match
		}
		n.send(Message{Type: MsgHeartbeatReq, To: id, Commit: commit, ReadID: r.id})
	}
	return nil
}

// ackRead records a follower confirming the leader's authority for one read.
func (n *Node) ackRead(id uint64, from NodeID) {
	for i, r := range n.pendingReads {
		if r.id != id {
			continue
		}
		if !n.cfg.IsVoter(from) {
			return // a learner cannot confirm leadership
		}
		r.acks[from] = true
		if len(r.acks) < n.cfg.Quorum() {
			return
		}
		n.readStates = append(n.readStates, ReadState{ID: r.id, Index: r.index})
		n.pendingReads = append(n.pendingReads[:i], n.pendingReads[i+1:]...)
		return
	}
}

// dropPendingReads abandons every read in flight. A node that is no longer
// leader cannot finish confirming anything, and a read that would be served
// from a deposed leader is precisely the stale read this mechanism exists to
// prevent -- so the caller is failed and can retry against the real leader.
func (n *Node) dropPendingReads() {
	n.pendingReads = nil
	n.readStates = nil
}

func (n *Node) hasPendingReadStates() bool { return len(n.readStates) > 0 }

func (n *Node) takeReadStates() []ReadState {
	if len(n.readStates) == 0 {
		return nil
	}
	out := n.readStates
	n.readStates = nil
	return out
}
