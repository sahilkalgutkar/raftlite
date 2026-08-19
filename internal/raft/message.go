package raft

import "fmt"

// MessageType identifies a protocol message. Heartbeats are a separate type
// from AppendEntries rather than "an AppendEntries with no entries": a
// heartbeat only proves liveness and carries a commit index, so it must not
// run the log matching check, and keeping the two apart means a heartbeat can
// never accidentally truncate a follower's log.
type MessageType uint8

const (
	// MsgVoteReq asks for a vote. With PreVote set it is a straw poll that
	// does not commit either side to a term change.
	MsgVoteReq MessageType = iota
	// MsgVoteResp answers a vote request.
	MsgVoteResp
	// MsgHeartbeatReq is the leader announcing it is still alive.
	MsgHeartbeatReq
	// MsgHeartbeatResp acknowledges a heartbeat.
	MsgHeartbeatResp
)

func (t MessageType) String() string {
	switch t {
	case MsgVoteReq:
		return "vote-req"
	case MsgVoteResp:
		return "vote-resp"
	case MsgHeartbeatReq:
		return "heartbeat-req"
	case MsgHeartbeatResp:
		return "heartbeat-resp"
	default:
		return fmt.Sprintf("msg(%d)", uint8(t))
	}
}

// Message is one protocol message. It is a single flat struct rather than a
// union per type because the transport layer then has exactly one thing to
// encode, and the consensus core can be tested by building literals.
type Message struct {
	Type MessageType
	From NodeID
	To   NodeID
	Term uint64

	// PreVote marks a vote request or response as part of the pre-vote round.
	PreVote bool

	// LastLogIndex and LastLogTerm carry the candidate's log position on a
	// vote request, which the voter compares against its own.
	LastLogIndex uint64
	LastLogTerm  uint64

	// Commit is how far the leader believes the log is committed. On a
	// heartbeat it is clamped to what the leader knows the follower has, so a
	// follower can never be told to commit an entry it does not hold.
	Commit uint64

	// Reject is set on any response that refuses the request.
	Reject bool
}

func (m Message) String() string {
	pre := ""
	if m.PreVote {
		pre = " pre"
	}
	rej := ""
	if m.Reject {
		rej = " reject"
	}
	return fmt.Sprintf("%s%s %d->%d term=%d%s", m.Type, pre, uint64(m.From), uint64(m.To), m.Term, rej)
}

// isRequestFromLeader reports whether receiving this message means the sender
// claims to be the leader of its term.
func (m Message) isRequestFromLeader() bool {
	return m.Type == MsgHeartbeatReq
}
