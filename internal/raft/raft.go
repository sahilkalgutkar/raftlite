package raft

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
)

// Options configures a Node. Timeouts are counted in Tick calls rather than
// wall-clock durations, which is what lets the whole algorithm be driven
// deterministically from tests.
type Options struct {
	ID     NodeID
	Config Config

	// ElectionTicks is how many ticks without hearing from a leader before a
	// follower starts campaigning. Each node randomises its own timeout in
	// [ElectionTicks, 2*ElectionTicks) so split votes resolve quickly.
	ElectionTicks int
	// HeartbeatTicks is how often a leader announces itself. It must be
	// comfortably smaller than ElectionTicks.
	HeartbeatTicks int

	// HardState, Log and Snapshot are the state recovered from disk. All three
	// are zero for a node starting with nothing.
	HardState HardState
	Log       *Log
	Snapshot  *Snapshot

	// PreVoteDisabled turns off the pre-vote round. It exists so tests can
	// show what pre-vote is actually preventing.
	PreVoteDisabled bool

	// Rand seeds the election timeout jitter. Tests pass a fixed seed so an
	// election plays out identically on every run.
	Rand   *rand.Rand
	Logger *slog.Logger
}

func (o *Options) withDefaults() {
	if o.ElectionTicks <= 0 {
		o.ElectionTicks = 10
	}
	if o.HeartbeatTicks <= 0 {
		o.HeartbeatTicks = 1
	}
	if o.Log == nil {
		o.Log = NewLog()
	}
	if o.Rand == nil {
		o.Rand = rand.New(rand.NewPCG(uint64(o.ID), 0x9E3779B97F4A7C15))
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
}

// Node is one Raft server's consensus state. It is deliberately inert: it does
// no I/O and never blocks. Feed it time with Tick and messages with Step, then
// drain the work it wants done with Ready.
//
// Nothing in here is safe for concurrent use -- the runtime that owns a Node
// drives it from a single goroutine, which is far easier to reason about than
// a lock around every field.
type Node struct {
	id  NodeID
	cfg Config
	log *Log

	role Role
	term uint64
	vote NodeID
	lead NodeID

	// votes records the responses to the election currently in flight.
	votes map[NodeID]bool
	// progress is the leader's view of every follower. It is nil on anyone
	// who is not currently leading.
	progress map[NodeID]*Progress

	electionTicks    int
	heartbeatTicks   int
	electionTimeout  int // randomised per election
	electionElapsed  int
	heartbeatElapsed int

	preVote bool

	// msgs is the outbound queue drained by Ready.
	msgs []Message
	// unstable is the first log index not yet handed to the caller for
	// persistence.
	unstable uint64
	// prevHS is the last hard state reported, so Ready only reports changes.
	prevHS HardState

	// snapshot is the newest image this node holds, either taken locally or
	// received from the leader. A leader sends it to followers that have
	// fallen off the end of its retained log.
	snapshot *Snapshot
	// pendingSnapshot is a snapshot received from the leader that the runtime
	// has not yet persisted and handed to the state machine.
	pendingSnapshot *Snapshot

	// pendingConfIndex is the index of the most recent membership change. A
	// new one is refused until this index commits, which is what keeps
	// configurations one server apart.
	pendingConfIndex uint64

	rand   *rand.Rand
	logger *slog.Logger
}

// NewNode builds a node from its recovered state.
func NewNode(opts Options) *Node {
	opts.withDefaults()

	n := &Node{
		id:             opts.ID,
		cfg:            opts.Config.Clone(),
		log:            opts.Log,
		role:           Follower,
		term:           opts.HardState.Term,
		vote:           opts.HardState.Vote,
		lead:           None,
		votes:          make(map[NodeID]bool),
		electionTicks:  opts.ElectionTicks,
		heartbeatTicks: opts.HeartbeatTicks,
		preVote:        !opts.PreVoteDisabled,
		rand:           opts.Rand,
		logger:         opts.Logger,
	}
	if opts.Snapshot != nil && !opts.Snapshot.IsEmpty() {
		n.snapshot = opts.Snapshot
		if len(n.cfg.Members) == 0 {
			// A node restarting from a snapshot alone gets its membership from
			// the snapshot, which is the configuration that was in force when
			// the image was taken.
			n.cfg = opts.Snapshot.Meta.Config.Clone()
		}
	}
	if opts.HardState.Commit > 0 {
		n.log.CommitTo(opts.HardState.Commit)
	}
	n.unstable = n.log.LastIndex() + 1
	n.prevHS = n.hardState()
	n.resetElectionTimeout()
	return n
}

// ID returns this node's identity.
func (n *Node) ID() NodeID { return n.id }

// Role reports whether the node is a follower, candidate or leader.
func (n *Node) Role() Role { return n.role }

// Term is the current term as this node understands it.
func (n *Node) Term() uint64 { return n.term }

// Leader is the leader of the current term, or None if unknown.
func (n *Node) Leader() NodeID { return n.lead }

// Log exposes the replicated log for inspection.
func (n *Node) Log() *Log { return n.log }

// Config is the cluster configuration currently in force.
func (n *Node) Config() Config { return n.cfg.Clone() }

func (n *Node) hardState() HardState {
	return HardState{Term: n.term, Vote: n.vote, Commit: n.log.Committed()}
}

// resetElectionTimeout picks a fresh randomised timeout in
// [electionTicks, 2*electionTicks). Without the jitter, a cluster whose nodes
// tick in lockstep would split its vote every single election.
func (n *Node) resetElectionTimeout() {
	n.electionTimeout = n.electionTicks + n.rand.IntN(n.electionTicks)
}

// Tick advances the logical clock by one unit.
func (n *Node) Tick() {
	switch n.role {
	case Leader:
		n.heartbeatElapsed++
		n.electionElapsed++
		if n.electionElapsed >= n.electionTimeout {
			n.electionElapsed = 0
			n.checkQuorum()
		}
		if n.role == Leader && n.heartbeatElapsed >= n.heartbeatTicks {
			n.heartbeatElapsed = 0
			n.bcastHeartbeat()
		}
	default:
		n.electionElapsed++
		if n.promotable() && n.electionElapsed >= n.electionTimeout {
			n.Campaign()
		}
	}
}

// promotable reports whether this node is allowed to seek election: it must
// still be a voting member of the configuration it knows about. A learner, or
// a node that has been removed from the cluster, must never campaign.
func (n *Node) promotable() bool { return n.cfg.IsVoter(n.id) }

func (n *Node) send(m Message) {
	m.From = n.id
	if m.Term == 0 {
		m.Term = n.term
	}
	n.msgs = append(n.msgs, m)
}

func (n *Node) becomeFollower(term uint64, lead NodeID) {
	if term > n.term {
		n.term = term
		n.vote = None
	}
	n.role = Follower
	n.lead = lead
	n.progress = nil
	n.electionElapsed = 0
	n.resetElectionTimeout()
	n.logger.Debug("became follower", "id", uint64(n.id), "term", n.term, "leader", uint64(lead))
}

func (n *Node) becomePreCandidate() {
	// The term deliberately does not move: that is the whole point of the
	// pre-vote round.
	n.role = PreCandidate
	n.lead = None
	n.votes = map[NodeID]bool{n.id: true}
	n.electionElapsed = 0
	n.resetElectionTimeout()
	n.logger.Debug("became pre-candidate", "id", uint64(n.id), "term", n.term)
}

func (n *Node) becomeCandidate() {
	n.term++
	n.role = Candidate
	n.vote = n.id
	n.lead = None
	n.votes = map[NodeID]bool{n.id: true}
	n.electionElapsed = 0
	n.resetElectionTimeout()
	n.logger.Debug("became candidate", "id", uint64(n.id), "term", n.term)
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.lead = n.id
	n.heartbeatElapsed = 0
	n.electionElapsed = 0
	n.onBecomeLeader()
	n.logger.Info("became leader", "id", uint64(n.id), "term", n.term, "last_index", n.log.LastIndex())
}

// Campaign starts an election immediately instead of waiting for the election
// timeout. Bootstrapping a fresh cluster uses this so the first leader appears
// without a full timeout of dead air.
func (n *Node) Campaign() {
	if !n.promotable() {
		return
	}
	if n.preVote {
		n.becomePreCandidate()
		n.solicitVotes(true)
		return
	}
	n.becomeCandidate()
	n.solicitVotes(false)
}

// solicitVotes votes for itself and asks everyone else. A single-voter cluster
// therefore wins its election in the same call, with no messages at all.
func (n *Node) solicitVotes(preVote bool) {
	term := n.term
	if preVote {
		// A pre-vote asks about the term we would move to, without going there.
		term = n.term + 1
	}

	if won, decided := n.poll(n.id, true); decided && won {
		n.electionWon(preVote)
		return
	}

	for _, id := range n.cfg.Voters() {
		if id == n.id {
			continue
		}
		n.send(Message{
			Type:         MsgVoteReq,
			To:           id,
			Term:         term,
			PreVote:      preVote,
			LastLogIndex: n.log.LastIndex(),
			LastLogTerm:  n.log.LastTerm(),
		})
	}
}

// Step feeds one received message into the state machine.
func (n *Node) Step(m Message) error {
	if m.Type == MsgVoteReq && n.underLeaderLease(m.From) {
		n.logger.Debug("rejecting vote under leader lease",
			"id", uint64(n.id), "candidate", uint64(m.From), "leader", uint64(n.lead))
		n.send(Message{Type: MsgVoteResp, To: m.From, PreVote: m.PreVote, Reject: true})
		return nil
	}

	switch {
	case m.Term == 0: // local message, no term handling
	case m.Term > n.term:
		n.stepUpTerm(m)
	case m.Term < n.term:
		n.stepStaleTerm(m)
		return nil
	}

	switch n.role {
	case Leader:
		return n.stepLeader(m)
	case Candidate, PreCandidate:
		return n.stepCandidate(m)
	default:
		return n.stepFollower(m)
	}
}

// underLeaderLease reports whether we have heard from a leader recently enough
// that a vote request should be refused outright. A server that lost
// connectivity and is now trying to force a new term must not be able to
// interrupt a cluster that is working fine (Raft dissertation, section 4.2.3).
// The check runs before any term handling: adopting the candidate's term first
// would clear the very lease we are trying to defend.
func (n *Node) underLeaderLease(candidate NodeID) bool {
	return n.lead != None && n.lead != candidate && n.electionElapsed < n.electionTimeout
}

// stepUpTerm handles a message from a higher term. The two exceptions are what
// make pre-vote work: a pre-vote request must not drag us into a term that may
// never exist, and a granted pre-vote response carries the term the candidate
// asked about rather than a term anyone is actually in.
func (n *Node) stepUpTerm(m Message) {
	if m.Type == MsgVoteReq && m.PreVote {
		return
	}
	if m.Type == MsgVoteResp && m.PreVote && !m.Reject {
		return
	}
	lead := None
	if m.isRequestFromLeader() {
		lead = m.From
	}
	n.becomeFollower(m.Term, lead)
}

// stepStaleTerm answers a message from an older term. Responding rather than
// dropping it is what tells a deposed leader or a stale candidate to give up
// immediately instead of waiting out an election timeout.
func (n *Node) stepStaleTerm(m Message) {
	switch m.Type {
	case MsgHeartbeatReq:
		n.send(Message{Type: MsgHeartbeatResp, To: m.From, Reject: true})
	case MsgAppendReq:
		n.send(Message{Type: MsgAppendResp, To: m.From, Reject: true})
	case MsgSnapshotReq:
		n.send(Message{Type: MsgSnapshotResp, To: m.From, Reject: true})
	case MsgVoteReq:
		n.send(Message{Type: MsgVoteResp, To: m.From, PreVote: m.PreVote, Reject: true})
	default:
		n.logger.Debug("dropping stale message", "type", m.Type.String(), "from", uint64(m.From), "term", m.Term)
	}
}

func (n *Node) stepFollower(m Message) error {
	switch m.Type {
	case MsgVoteReq:
		n.handleVoteRequest(m)
	case MsgHeartbeatReq:
		n.handleHeartbeat(m)
	case MsgAppendReq:
		n.handleAppendRequest(m)
	case MsgSnapshotReq:
		n.handleSnapshot(m)
	}
	return nil
}

func (n *Node) stepCandidate(m Message) error {
	switch m.Type {
	case MsgVoteReq:
		n.handleVoteRequest(m)
	case MsgVoteResp:
		n.handleVoteResponse(m)
	case MsgHeartbeatReq:
		// Someone else won this term. Concede and follow them.
		n.becomeFollower(m.Term, m.From)
		n.handleHeartbeat(m)
	case MsgAppendReq:
		n.becomeFollower(m.Term, m.From)
		n.handleAppendRequest(m)
	case MsgSnapshotReq:
		n.becomeFollower(m.Term, m.From)
		n.handleSnapshot(m)
	}
	return nil
}

func (n *Node) stepLeader(m Message) error {
	switch m.Type {
	case MsgVoteReq:
		n.handleVoteRequest(m)
	case MsgHeartbeatResp:
		n.handleHeartbeatResponse(m)
	case MsgAppendResp:
		n.handleAppendResponse(m)
	case MsgSnapshotResp:
		n.handleSnapshotResponse(m)
	}
	return nil
}

func (n *Node) bcastHeartbeat() {
	for _, m := range n.cfg.Members {
		if m.ID == n.id {
			continue
		}
		n.sendHeartbeat(m.ID)
	}
}

// sendHeartbeat tells one follower the leader is still alive. The commit index
// rides along so a follower learns about newly committed entries without
// waiting for the next write -- clamped to what the leader knows that follower
// holds, because a follower must never be told to commit an entry it does not
// have.
func (n *Node) sendHeartbeat(to NodeID) {
	commit := n.log.Committed()
	if p := n.progress[to]; p != nil && p.Match < commit {
		commit = p.Match
	}
	n.send(Message{Type: MsgHeartbeatReq, To: to, Commit: commit})
}

// commitFromLeader advances the commit index toward what the leader reports,
// never past the end of our own log.
func (n *Node) commitFromLeader(leaderCommit uint64) {
	if leaderCommit > n.log.Committed() {
		n.log.CommitTo(leaderCommit)
	}
}

func (n *Node) handleHeartbeat(m Message) {
	n.electionElapsed = 0
	n.lead = m.From
	n.commitFromLeader(m.Commit)
	n.send(Message{Type: MsgHeartbeatResp, To: m.From})
}

// Ready collects everything the runtime has to do on this node's behalf and
// clears it. The caller must persist HardState, Entries and Snapshot before
// putting Messages on the wire -- that ordering is a Raft safety requirement,
// not an optimisation, since a vote or an entry that is sent but not durable
// can be forgotten across a crash and then contradicted.
type Ready struct {
	Role Role
	Term uint64
	Lead NodeID

	// HardState is non-nil only when it changed since the last Ready.
	HardState *HardState
	// Snapshot must be persisted and handed to the state machine, replacing
	// whatever it currently holds.
	Snapshot *Snapshot
	// Entries are new log entries to append to stable storage.
	Entries []Entry
	// Messages go out after the durable state above has landed.
	Messages []Message
	// CommittedEntries are entries the state machine should now apply.
	CommittedEntries []Entry
}

// IsEmpty reports whether there is nothing for the runtime to do.
func (r Ready) IsEmpty() bool {
	return r.HardState == nil && r.Snapshot == nil && len(r.Entries) == 0 &&
		len(r.Messages) == 0 && len(r.CommittedEntries) == 0
}

// HasReady reports whether a call to Ready would return any work.
func (n *Node) HasReady() bool {
	return len(n.msgs) > 0 ||
		n.log.LastIndex() >= n.unstable ||
		len(n.log.NextCommitted()) > 0 ||
		!n.hardState().Equal(n.prevHS) ||
		n.hasPendingSnapshot()
}

// Ready drains the pending work. Everything it returns is owned by the caller.
func (n *Node) Ready() Ready {
	rd := Ready{Role: n.role, Term: n.term, Lead: n.lead}

	if hs := n.hardState(); !hs.Equal(n.prevHS) {
		copied := hs
		rd.HardState = &copied
		n.prevHS = hs
	}

	rd.Snapshot = n.takePendingSnapshot()

	if last := n.log.LastIndex(); last >= n.unstable {
		if ents, err := n.log.Slice(n.unstable, last+1); err == nil {
			rd.Entries = ents
		}
		n.unstable = last + 1
	}

	rd.CommittedEntries = n.takeCommitted()

	rd.Messages = n.msgs
	n.msgs = nil
	return rd
}

// takeCommitted hands out the entries the state machine still owes, folding
// any configuration change among them into the live configuration first.
// Membership is applied here, inside the algorithm, because it changes what a
// quorum means for every decision that follows.
func (n *Node) takeCommitted() []Entry {
	ents := n.log.NextCommitted()
	if len(ents) == 0 {
		return nil
	}
	n.log.AppliedTo(ents[len(ents)-1].Index)
	for _, e := range ents {
		if e.Type == EntryConfChange {
			n.applyConfChangeEntry(e)
		}
	}
	return ents
}

// Status is a snapshot of a node's state for logging and the /status endpoint.
type Status struct {
	ID        NodeID
	Role      Role
	Term      uint64
	Leader    NodeID
	Commit    uint64
	Applied   uint64
	LastIndex uint64
	Snapshot  uint64
	Config    Config
	Progress  map[NodeID]Progress
}

// Status returns a consistent view of the node's state.
func (n *Node) Status() Status {
	s := Status{
		ID:        n.id,
		Role:      n.role,
		Term:      n.term,
		Leader:    n.lead,
		Commit:    n.log.Committed(),
		Applied:   n.log.Applied(),
		LastIndex: n.log.LastIndex(),
		Snapshot:  n.log.SnapshotIndex(),
		Config:    n.cfg.Clone(),
	}
	s.Progress = n.progressSnapshot()
	return s
}

func (n *Node) String() string {
	return fmt.Sprintf("node{id=%d role=%s term=%d lead=%d %s}",
		uint64(n.id), n.role, n.term, uint64(n.lead), n.log)
}
