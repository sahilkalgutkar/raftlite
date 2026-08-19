package raft

import "fmt"

// Log is the replicated log, held in memory, with everything below a snapshot
// boundary compacted away.
//
// The index arithmetic is the part that bites: entry indices are 1-based and
// global, but after compaction the slice no longer starts at index 1. I keep a
// single offset -- the index of the last entry the snapshot covers -- and every
// lookup goes through it, rather than scattering "index - something" all over
// the replication code.
type Log struct {
	entries []Entry // entries[i].Index == offset + 1 + i

	offset   uint64 // index the snapshot ends at; 0 when nothing is compacted
	snapTerm uint64 // term of the entry at offset

	committed uint64 // highest index known to be committed cluster-wide
	applied   uint64 // highest index handed to the state machine
}

// NewLog returns an empty log, as a brand new node starts life.
func NewLog() *Log { return &Log{} }

// NewLogFrom rebuilds a log from a snapshot boundary plus the entries that
// survived compaction -- the shape a node comes back in after a restart.
func NewLogFrom(snapIndex, snapTerm uint64, entries []Entry) *Log {
	l := &Log{
		offset:    snapIndex,
		snapTerm:  snapTerm,
		committed: snapIndex,
		applied:   snapIndex,
	}
	if len(entries) > 0 {
		l.entries = append([]Entry(nil), entries...)
	}
	return l
}

// FirstIndex is the lowest index still materialised in the log.
func (l *Log) FirstIndex() uint64 { return l.offset + 1 }

// LastIndex is the highest index in the log, snapshot included.
func (l *Log) LastIndex() uint64 { return l.offset + uint64(len(l.entries)) }

// SnapshotIndex is the index of the last entry folded into a snapshot.
func (l *Log) SnapshotIndex() uint64 { return l.offset }

// SnapshotTerm is the term of the entry at SnapshotIndex.
func (l *Log) SnapshotTerm() uint64 { return l.snapTerm }

// Committed is the highest index a quorum has durably stored.
func (l *Log) Committed() uint64 { return l.committed }

// Applied is the highest index the state machine has consumed.
func (l *Log) Applied() uint64 { return l.applied }

// Term returns the term of the entry at index i.
func (l *Log) Term(i uint64) (uint64, error) {
	switch {
	case i == l.offset:
		return l.snapTerm, nil
	case i < l.offset:
		return 0, ErrCompacted
	case i > l.LastIndex():
		return 0, ErrUnavailable
	default:
		return l.entries[i-l.offset-1].Term, nil
	}
}

// LastTerm is the term of the newest entry, which is what elections compare.
func (l *Log) LastTerm() uint64 {
	t, err := l.Term(l.LastIndex())
	if err != nil {
		return 0
	}
	return t
}

// Entry returns a single entry by index.
func (l *Log) Entry(i uint64) (Entry, error) {
	if i <= l.offset {
		return Entry{}, ErrCompacted
	}
	if i > l.LastIndex() {
		return Entry{}, ErrUnavailable
	}
	return l.entries[i-l.offset-1], nil
}

// Slice returns the entries in the half-open range [lo, hi).
func (l *Log) Slice(lo, hi uint64) ([]Entry, error) {
	if lo > hi {
		return nil, fmt.Errorf("raft: invalid slice range [%d,%d)", lo, hi)
	}
	if lo <= l.offset {
		return nil, ErrCompacted
	}
	if hi > l.LastIndex()+1 {
		return nil, ErrUnavailable
	}
	if lo == hi {
		return nil, nil
	}
	out := make([]Entry, hi-lo)
	copy(out, l.entries[lo-l.offset-1:hi-l.offset-1])
	return out, nil
}

// From returns every entry at or after lo.
func (l *Log) From(lo uint64) ([]Entry, error) { return l.Slice(lo, l.LastIndex()+1) }

// Append adds entries to the end of the log, stamping indices as it goes so
// callers only have to supply the term and payload. It returns the new last
// index.
func (l *Log) Append(entries ...Entry) uint64 {
	next := l.LastIndex() + 1
	for _, e := range entries {
		e.Index = next
		l.entries = append(l.entries, e)
		next++
	}
	return l.LastIndex()
}

// MatchTerm reports whether the log contains an entry at index i with the
// given term -- the log matching check an AppendEntries request performs.
func (l *Log) MatchTerm(i, term uint64) bool {
	t, err := l.Term(i)
	if err != nil {
		return false
	}
	return t == term
}

// MaybeAppend implements the follower side of AppendEntries: verify the
// previous entry matches, drop any prefix we already agree on, throw away the
// conflicting suffix, and append the rest. It returns the index of the last
// new entry and whether the request was accepted at all.
func (l *Log) MaybeAppend(prevIndex, prevTerm uint64, entries []Entry) (uint64, bool) {
	if !l.MatchTerm(prevIndex, prevTerm) {
		return 0, false
	}
	lastNew := prevIndex + uint64(len(entries))
	for i, e := range entries {
		idx := prevIndex + 1 + uint64(i)
		if l.MatchTerm(idx, e.Term) {
			continue // already have this exact entry
		}
		if idx <= l.LastIndex() {
			// Same index, different term: our tail is wrong, not theirs.
			l.truncateFrom(idx)
		}
		l.Append(entries[i:]...)
		break
	}
	return lastNew, true
}

// truncateFrom discards everything at or after index i.
func (l *Log) truncateFrom(i uint64) {
	if i <= l.offset+1 {
		l.entries = nil
		return
	}
	if i > l.LastIndex() {
		return
	}
	l.entries = l.entries[:i-l.offset-1]
}

// IsUpToDate implements the election restriction: a candidate only earns a
// vote if its log is at least as complete as the voter's. A longer log loses
// to a log with a higher final term, which is what keeps a committed entry
// from being erased by a node that missed it.
func (l *Log) IsUpToDate(lastIndex, lastTerm uint64) bool {
	own := l.LastTerm()
	if lastTerm != own {
		return lastTerm > own
	}
	return lastIndex >= l.LastIndex()
}

// CommitTo advances the commit index, never backwards and never past the end
// of the log.
func (l *Log) CommitTo(i uint64) bool {
	if i <= l.committed {
		return false
	}
	if i > l.LastIndex() {
		i = l.LastIndex()
	}
	if i <= l.committed {
		return false
	}
	l.committed = i
	return true
}

// AppliedTo records how far the state machine has consumed.
func (l *Log) AppliedTo(i uint64) {
	if i > l.applied {
		l.applied = i
	}
}

// NextCommitted returns the committed-but-unapplied entries, which is exactly
// the work the state machine still owes.
func (l *Log) NextCommitted() []Entry {
	if l.committed <= l.applied {
		return nil
	}
	ents, err := l.Slice(l.applied+1, l.committed+1)
	if err != nil {
		return nil
	}
	return ents
}

// Compact discards every entry at or before index i, which must already be
// applied and must still be present in the log.
func (l *Log) Compact(i uint64) error {
	if i <= l.offset {
		return ErrCompacted
	}
	if i > l.LastIndex() {
		return ErrUnavailable
	}
	term, err := l.Term(i)
	if err != nil {
		return err
	}
	l.entries = append([]Entry(nil), l.entries[i-l.offset:]...)
	l.offset = i
	l.snapTerm = term
	if l.committed < i {
		l.committed = i
	}
	if l.applied < i {
		l.applied = i
	}
	return nil
}

// Restore resets the log to a snapshot received from a leader, discarding
// whatever we had. A follower only gets here when it is so far behind that the
// leader no longer holds the entries it needs.
func (l *Log) Restore(meta SnapshotMeta) {
	l.entries = nil
	l.offset = meta.Index
	l.snapTerm = meta.Term
	l.committed = meta.Index
	l.applied = meta.Index
}

func (l *Log) String() string {
	return fmt.Sprintf("log{first=%d last=%d committed=%d applied=%d snap=%d/%d}",
		l.FirstIndex(), l.LastIndex(), l.committed, l.applied, l.offset, l.snapTerm)
}
