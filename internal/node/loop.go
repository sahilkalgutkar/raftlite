package node

import (
	"fmt"
	"time"

	"github.com/sahilkalgutkar/raftlite/internal/raft"
)

// run is the event loop. Everything that touches the consensus state machine
// happens here, in this one goroutine, in this order.
func (n *Node) run() {
	defer close(n.doneCh)
	defer n.shutdown()

	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()

	if n.bootstrapPending {
		n.cfg.Logger.Info("bootstrapping: calling the first election", "id", uint64(n.cfg.ID))
		n.raft.Campaign()
	}

	for {
		if err := n.drainReady(); err != nil {
			n.fail(err)
			return
		}

		select {
		case <-n.stopCh:
			return

		case <-ticker.C:
			n.raft.Tick()

		case m := <-n.recvCh:
			if err := n.raft.Step(m); err != nil {
				n.cfg.Logger.Warn("rejected a message", "id", uint64(n.cfg.ID), "msg", m.String(), "err", err)
			}

		case p := <-n.propCh:
			n.startProposal(p)

		case r := <-n.readCh:
			n.startRead(r)
		}
	}
}

// startProposal appends a client's entry and records who is waiting for it.
func (n *Node) startProposal(p *proposal) {
	var (
		index uint64
		err   error
	)
	if p.conf != nil {
		index, err = n.raft.ProposeConfChange(*p.conf)
	} else {
		index, err = n.raft.Propose(p.entryType, p.data)
	}
	if err != nil {
		p.result <- proposalResult{err: err}
		return
	}
	p.term = n.raft.Term()
	n.waiters[index] = p
}

// startRead asks the algorithm to confirm this node still leads, and records
// the client waiting on the answer.
func (n *Node) startRead(r *readRequest) {
	n.nextReadID++
	id := n.nextReadID
	if err := n.raft.ReadIndex(id); err != nil {
		r.result <- err
		return
	}
	n.reads[id] = &readWaiter{result: r.result}
}

// drainReady performs the work the algorithm has queued up. The order is a
// safety requirement, not an optimisation:
//
//  1. Persist. A vote or an entry that went out on the wire but not to the
//     disk can be forgotten across a crash and then contradicted.
//  2. Send. Only once the state behind those messages is durable.
//  3. Apply. Only entries a quorum has committed reach the state machine.
func (n *Node) drainReady() error {
	for n.raft.HasReady() {
		rd := n.raft.Ready()

		if rd.Snapshot != nil {
			if err := n.installSnapshot(rd.Snapshot); err != nil {
				return err
			}
		}
		if err := n.store.Save(rd.HardState, rd.Entries); err != nil {
			return err
		}
		for _, m := range rd.Messages {
			n.tr.Send(m)
		}
		n.applyCommitted(rd.CommittedEntries)
		n.confirmReads(rd.ReadStates)
		n.releaseReads()

		n.reconcileMembership()
		n.publishStatus()
		if err := n.maybeSnapshot(); err != nil {
			return err
		}
		n.failStaleWaiters(rd.Role)
	}
	return nil
}

// installSnapshot replaces local state with an image from the leader.
func (n *Node) installSnapshot(snap *raft.Snapshot) error {
	if err := n.kv.Restore(snap.Data); err != nil {
		return fmt.Errorf("node: restore snapshot into the state machine: %w", err)
	}
	tail, err := n.raft.Log().From(snap.Meta.Index + 1)
	if err != nil {
		tail = nil
	}
	hs := raft.HardState{Term: n.raft.Term(), Commit: n.raft.Log().Committed()}
	if err := n.store.SaveSnapshot(*snap, hs, tail); err != nil {
		return err
	}
	n.appliedSinceSnapshot = 0
	n.cfg.Logger.Info("installed a snapshot from the leader",
		"id", uint64(n.cfg.ID), "index", snap.Meta.Index, "keys", n.kv.Len())
	return nil
}

// applyCommitted hands committed entries to the state machine and wakes up
// whoever was waiting on them.
func (n *Node) applyCommitted(entries []raft.Entry) {
	for _, e := range entries {
		res := n.kv.Apply(e)
		n.appliedSinceSnapshot++

		p, waiting := n.waiters[e.Index]
		if !waiting {
			continue
		}
		delete(n.waiters, e.Index)

		// Same index, different term: a new leader replaced our entry before
		// it committed. The client's write did not happen.
		if e.Term != p.term {
			p.result <- proposalResult{err: ErrLeadershipLost}
			continue
		}
		p.result <- proposalResult{res: res}
	}
}

// confirmReads records the index each confirmed read may be served at.
func (n *Node) confirmReads(states []raft.ReadState) {
	for _, rs := range states {
		if w, ok := n.reads[rs.ID]; ok {
			w.index = rs.Index
			w.confirmed = true
		}
	}
}

// releaseReads wakes up reads whose index the state machine has now reached.
func (n *Node) releaseReads() {
	if len(n.reads) == 0 {
		return
	}
	applied := n.raft.Log().Applied()
	for id, w := range n.reads {
		if w.confirmed && applied >= w.index {
			w.result <- nil
			delete(n.reads, id)
		}
	}
}

// failStaleWaiters releases clients still blocked on entries this node can no
// longer commit, which is what happens the moment it stops being leader.
// Leaving them to time out would be correct but needlessly slow.
func (n *Node) failStaleWaiters(role raft.Role) {
	if role == raft.Leader {
		return
	}
	for idx, p := range n.waiters {
		p.result <- proposalResult{err: ErrLeadershipLost}
		delete(n.waiters, idx)
	}
	// A read confirmed by a node that has since been deposed is exactly the
	// stale read ReadIndex exists to prevent, so these fail too and the client
	// retries against whoever is actually leading.
	for id, w := range n.reads {
		w.result <- ErrLeadershipLost
		delete(n.reads, id)
	}
}

// reconcileMembership keeps the transport's address book in step with the
// configuration the log has committed.
func (n *Node) reconcileMembership() {
	cfg := n.raft.Config()
	if cfg.Equal(n.knownConfig) {
		return
	}
	n.knownConfig = cfg
	n.tr.SetPeers(cfg.Members)
	n.cfg.Logger.Info("membership changed", "id", uint64(n.cfg.ID), "config", cfg.String())
}

// maybeSnapshot compacts the log once enough entries have piled up behind it.
func (n *Node) maybeSnapshot() error {
	if n.cfg.SnapshotThreshold == 0 || n.appliedSinceSnapshot < n.cfg.SnapshotThreshold {
		return nil
	}
	applied := n.raft.Log().Applied()
	if applied <= n.raft.Log().SnapshotIndex() {
		return nil
	}

	data, err := n.kv.Snapshot()
	if err != nil {
		return fmt.Errorf("node: snapshot the state machine: %w", err)
	}
	if err := n.raft.Compact(applied, data); err != nil {
		// Not fatal: the log simply stays longer than we wanted.
		n.cfg.Logger.Warn("could not compact the log", "id", uint64(n.cfg.ID), "index", applied, "err", err)
		n.appliedSinceSnapshot = 0
		return nil
	}
	tail, err := n.raft.Log().From(applied + 1)
	if err != nil {
		tail = nil
	}
	snap := n.raft.Snapshot()
	hs := raft.HardState{Term: n.raft.Term(), Commit: n.raft.Log().Committed()}
	if err := n.store.SaveSnapshot(*snap, hs, tail); err != nil {
		return err
	}
	n.appliedSinceSnapshot = 0
	return nil
}

func (n *Node) publishStatus() {
	st := n.raft.Status()
	n.status.Store(&st)
}

// fail records a fatal error. Anything that stops the node from making its
// state durable is fatal by definition: continuing to serve after a failed
// fsync means answering from state the node cannot promise to remember.
func (n *Node) fail(err error) {
	n.cfg.Logger.Error("node is stopping after an unrecoverable error",
		"id", uint64(n.cfg.ID), "err", err)
	wrapped := fmt.Errorf("node %d: %w", uint64(n.cfg.ID), err)
	n.runErr.Store(&wrapped)
}

func (n *Node) shutdown() {
	for idx, p := range n.waiters {
		p.result <- proposalResult{err: ErrStopped}
		delete(n.waiters, idx)
	}
	for id, w := range n.reads {
		w.result <- ErrStopped
		delete(n.reads, id)
	}
	if err := n.tr.Close(); err != nil {
		n.cfg.Logger.Warn("transport did not close cleanly", "id", uint64(n.cfg.ID), "err", err)
	}
	if err := n.store.Close(); err != nil {
		n.cfg.Logger.Warn("store did not close cleanly", "id", uint64(n.cfg.ID), "err", err)
	}
	n.cfg.Logger.Info("node stopped", "id", uint64(n.cfg.ID))
}
