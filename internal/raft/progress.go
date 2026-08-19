package raft

import "fmt"

// Progress is the leader's view of one follower: how much of the log it is
// known to hold, and what to send next.
//
// Match is a fact -- the follower acknowledged everything up to it. Next is a
// guess that starts optimistically at the end of the leader's log and walks
// backwards on rejection until the two logs agree. Keeping the fact and the
// guess in separate fields is what makes the repair loop terminate.
type Progress struct {
	// Match is the highest index the follower has confirmed storing.
	Match uint64
	// Next is the index of the next entry to send.
	Next uint64
	// Learner marks a replica that does not vote.
	Learner bool
	// RecentActive records whether this follower answered since the last
	// quorum check, which is how a leader notices it has been cut off.
	RecentActive bool
	// PendingSnapshot is the index of a snapshot in flight to this follower.
	// While it is set the leader stops sending entries, since they would just
	// be rejected until the snapshot lands.
	PendingSnapshot uint64
	// snapshotWait counts down the heartbeats a snapshot may stay outstanding
	// before the leader assumes it was lost and tries again. Without it, a
	// single dropped snapshot message strands that follower permanently: the
	// leader waits for an acknowledgement that is never coming, and refuses to
	// send anything else in the meantime.
	snapshotWait int
}

func (p *Progress) String() string {
	return fmt.Sprintf("{match=%d next=%d learner=%v active=%v}", p.Match, p.Next, p.Learner, p.RecentActive)
}

// maybeUpdate records an acknowledgement, reporting whether it told us
// anything new. Responses can arrive out of order, so an older acknowledgement
// must never drag Match backwards.
func (p *Progress) maybeUpdate(match uint64) bool {
	updated := false
	if p.Match < match {
		p.Match = match
		updated = true
	}
	if p.Next < match+1 {
		p.Next = match + 1
	}
	return updated
}

// maybeDecrTo backs Next up after a rejection, using the follower's hint.
// It returns false for a stale rejection that would move Next the wrong way.
func (p *Progress) maybeDecrTo(rejectedNext uint64) bool {
	if rejectedNext >= p.Next {
		return false
	}
	if rejectedNext < p.Match+1 {
		rejectedNext = p.Match + 1
	}
	if rejectedNext < 1 {
		rejectedNext = 1
	}
	p.Next = rejectedNext
	return true
}
