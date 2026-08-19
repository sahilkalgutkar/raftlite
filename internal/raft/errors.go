package raft

import "errors"

var (
	// ErrCompacted means the requested index is already covered by a snapshot,
	// so the entry itself no longer exists on this node.
	ErrCompacted = errors.New("raft: log index is compacted into a snapshot")
	// ErrUnavailable means the requested index is past the end of the log.
	ErrUnavailable = errors.New("raft: log index is not available yet")
	// ErrNotLeader is returned when a proposal reaches a node that cannot
	// accept writes.
	ErrNotLeader = errors.New("raft: node is not the leader")
	// ErrConfChangeInFlight is returned when a membership change is proposed
	// while a previous one is still uncommitted. Raft only stays safe if
	// configurations change one server at a time.
	ErrConfChangeInFlight = errors.New("raft: a configuration change is still in flight")
	// ErrInvalidConfChange is returned for a membership edit that would leave
	// the cluster in an unusable state, such as removing the last voter.
	ErrInvalidConfChange = errors.New("raft: invalid configuration change")
)
