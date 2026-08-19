package raft

import (
	"errors"
	"testing"
)

// ents builds a log of entries whose terms are given in order, starting at
// index 1. It keeps the tests readable: entsLog(1, 1, 2) is a three-entry log.
func entsLog(terms ...uint64) *Log {
	l := NewLog()
	for _, t := range terms {
		l.Append(Entry{Term: t, Type: EntryNormal})
	}
	return l
}

func TestLogAppendStampsIndices(t *testing.T) {
	l := entsLog(1, 1, 2)
	if got := l.LastIndex(); got != 3 {
		t.Fatalf("LastIndex = %d, want 3", got)
	}
	if got := l.FirstIndex(); got != 1 {
		t.Fatalf("FirstIndex = %d, want 1", got)
	}
	for i := uint64(1); i <= 3; i++ {
		e, err := l.Entry(i)
		if err != nil {
			t.Fatalf("Entry(%d): %v", i, err)
		}
		if e.Index != i {
			t.Fatalf("entry at %d has Index %d", i, e.Index)
		}
	}
	if got := l.LastTerm(); got != 2 {
		t.Fatalf("LastTerm = %d, want 2", got)
	}
}

func TestLogEmpty(t *testing.T) {
	l := NewLog()
	if l.LastIndex() != 0 || l.LastTerm() != 0 {
		t.Fatalf("empty log: last=%d/%d", l.LastIndex(), l.LastTerm())
	}
	if term, err := l.Term(0); err != nil || term != 0 {
		t.Fatalf("Term(0) = %d, %v", term, err)
	}
	if _, err := l.Term(1); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Term(1) err = %v, want ErrUnavailable", err)
	}
	if ents := l.NextCommitted(); ents != nil {
		t.Fatalf("empty log has committed entries: %v", ents)
	}
}

func TestLogTermBoundaries(t *testing.T) {
	l := entsLog(1, 2, 3)
	if _, err := l.Term(4); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Term past end = %v", err)
	}
	if err := l.Compact(2); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if _, err := l.Term(1); !errors.Is(err, ErrCompacted) {
		t.Fatalf("Term below snapshot = %v, want ErrCompacted", err)
	}
	if term, err := l.Term(2); err != nil || term != 2 {
		t.Fatalf("Term at snapshot boundary = %d, %v", term, err)
	}
	if _, err := l.Entry(2); !errors.Is(err, ErrCompacted) {
		t.Fatalf("Entry at snapshot boundary should be compacted, got %v", err)
	}
	if _, err := l.Entry(9); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Entry past end = %v", err)
	}
}

func TestLogSlice(t *testing.T) {
	l := entsLog(1, 1, 2, 3)

	got, err := l.Slice(2, 4)
	if err != nil {
		t.Fatalf("Slice: %v", err)
	}
	if len(got) != 2 || got[0].Index != 2 || got[1].Index != 3 {
		t.Fatalf("Slice(2,4) = %v", got)
	}

	if got, err := l.Slice(3, 3); err != nil || got != nil {
		t.Fatalf("empty slice = %v, %v", got, err)
	}
	if _, err := l.Slice(4, 2); err == nil {
		t.Fatal("inverted range should error")
	}
	if _, err := l.Slice(0, 2); !errors.Is(err, ErrCompacted) {
		t.Fatalf("slice from 0 = %v", err)
	}
	if _, err := l.Slice(2, 99); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("slice past end = %v", err)
	}

	all, err := l.From(1)
	if err != nil || len(all) != 4 {
		t.Fatalf("From(1) = %v, %v", all, err)
	}

	// The returned slice must be a copy: mutating it cannot corrupt the log.
	all[0].Term = 99
	if e, _ := l.Entry(1); e.Term != 1 {
		t.Fatal("Slice handed out an aliased entry")
	}
}

func TestLogMaybeAppendMatchingPrefixIsIdempotent(t *testing.T) {
	l := entsLog(1, 1, 2, 2)
	// Replay of entries we already hold must not truncate the tail.
	last, ok := l.MaybeAppend(1, 1, []Entry{{Term: 1, Index: 2}, {Term: 2, Index: 3}})
	if !ok || last != 3 {
		t.Fatalf("MaybeAppend = %d, %v", last, ok)
	}
	if l.LastIndex() != 4 {
		t.Fatalf("duplicate append truncated the log to %d", l.LastIndex())
	}
}

func TestLogMaybeAppendRejectsUnmatchedPrev(t *testing.T) {
	l := entsLog(1, 1)
	if _, ok := l.MaybeAppend(2, 5, []Entry{{Term: 5}}); ok {
		t.Fatal("append with wrong prevTerm was accepted")
	}
	if _, ok := l.MaybeAppend(9, 1, []Entry{{Term: 1}}); ok {
		t.Fatal("append past the end of the log was accepted")
	}
	if l.LastIndex() != 2 {
		t.Fatalf("rejected append mutated the log: last=%d", l.LastIndex())
	}
}

func TestLogMaybeAppendTruncatesConflict(t *testing.T) {
	l := entsLog(1, 1, 2) // indices 1,2,3
	last, ok := l.MaybeAppend(1, 1, []Entry{{Term: 1}, {Term: 3}, {Term: 3}})
	if !ok || last != 4 {
		t.Fatalf("MaybeAppend = %d, %v", last, ok)
	}
	if l.LastIndex() != 4 {
		t.Fatalf("last index = %d, want 4", l.LastIndex())
	}
	if term, _ := l.Term(3); term != 3 {
		t.Fatalf("conflicting entry was not replaced: term at 3 = %d", term)
	}
}

func TestLogMaybeAppendExtendsFromEmpty(t *testing.T) {
	l := NewLog()
	last, ok := l.MaybeAppend(0, 0, []Entry{{Term: 1}, {Term: 1}})
	if !ok || last != 2 {
		t.Fatalf("MaybeAppend on empty log = %d, %v", last, ok)
	}
	if l.LastIndex() != 2 {
		t.Fatalf("last = %d", l.LastIndex())
	}
}

func TestLogTruncateWholeLog(t *testing.T) {
	l := entsLog(1, 1, 1)
	// A conflict at the very first index wipes the log clean before appending.
	last, ok := l.MaybeAppend(0, 0, []Entry{{Term: 4}})
	if !ok || last != 1 {
		t.Fatalf("MaybeAppend = %d, %v", last, ok)
	}
	if l.LastIndex() != 1 {
		t.Fatalf("last = %d, want 1", l.LastIndex())
	}
	if term, _ := l.Term(1); term != 4 {
		t.Fatalf("term at 1 = %d, want 4", term)
	}
}

func TestLogIsUpToDate(t *testing.T) {
	l := entsLog(1, 2, 2) // last index 3, last term 2

	cases := []struct {
		name      string
		index, tm uint64
		wantGrant bool
	}{
		{"identical log", 3, 2, true},
		{"longer log same term", 4, 2, true},
		{"shorter log same term", 2, 2, false},
		{"higher final term but shorter", 1, 3, true},
		{"lower final term but longer", 9, 1, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := l.IsUpToDate(tc.index, tc.tm); got != tc.wantGrant {
				t.Fatalf("IsUpToDate(%d,%d) = %v, want %v", tc.index, tc.tm, got, tc.wantGrant)
			}
		})
	}
}

func TestLogCommitAndApply(t *testing.T) {
	l := entsLog(1, 1, 1)

	if !l.CommitTo(2) {
		t.Fatal("CommitTo(2) reported no change")
	}
	if l.CommitTo(1) {
		t.Fatal("commit index moved backwards")
	}
	if !l.CommitTo(9) || l.Committed() != 3 {
		t.Fatalf("commit clamped wrong: %d", l.Committed())
	}
	if l.CommitTo(9) {
		t.Fatal("clamped commit reported a second change")
	}

	pending := l.NextCommitted()
	if len(pending) != 3 {
		t.Fatalf("NextCommitted = %d entries, want 3", len(pending))
	}
	l.AppliedTo(3)
	l.AppliedTo(1) // must not move backwards
	if l.Applied() != 3 {
		t.Fatalf("applied = %d", l.Applied())
	}
	if l.NextCommitted() != nil {
		t.Fatal("state machine still owes work after applying everything")
	}
}

func TestLogCompact(t *testing.T) {
	l := entsLog(1, 1, 2, 2)
	l.CommitTo(4)
	l.AppliedTo(4)

	if err := l.Compact(2); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if l.FirstIndex() != 3 || l.LastIndex() != 4 {
		t.Fatalf("after compact: first=%d last=%d", l.FirstIndex(), l.LastIndex())
	}
	if l.SnapshotIndex() != 2 || l.SnapshotTerm() != 1 {
		t.Fatalf("snapshot boundary = %d/%d", l.SnapshotIndex(), l.SnapshotTerm())
	}
	// Entries after the boundary survive with their original indices.
	e, err := l.Entry(3)
	if err != nil || e.Index != 3 || e.Term != 2 {
		t.Fatalf("Entry(3) = %v, %v", e, err)
	}
	if err := l.Compact(2); !errors.Is(err, ErrCompacted) {
		t.Fatalf("re-compact = %v", err)
	}
	if err := l.Compact(99); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("compact past end = %v", err)
	}
}

func TestLogCompactAdvancesCommitAndApplied(t *testing.T) {
	l := entsLog(1, 1, 1)
	if err := l.Compact(2); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if l.Committed() != 2 || l.Applied() != 2 {
		t.Fatalf("commit/applied = %d/%d, want 2/2", l.Committed(), l.Applied())
	}
}

func TestLogRestoreFromSnapshot(t *testing.T) {
	l := entsLog(1, 1, 1)
	l.Restore(SnapshotMeta{Index: 10, Term: 4})

	if l.FirstIndex() != 11 || l.LastIndex() != 10 {
		t.Fatalf("after restore: first=%d last=%d", l.FirstIndex(), l.LastIndex())
	}
	if l.Committed() != 10 || l.Applied() != 10 {
		t.Fatalf("commit/applied = %d/%d", l.Committed(), l.Applied())
	}
	if l.LastTerm() != 4 {
		t.Fatalf("last term = %d, want 4", l.LastTerm())
	}
	// New entries pick up numbering after the snapshot.
	if got := l.Append(Entry{Term: 5}); got != 11 {
		t.Fatalf("append after restore landed at %d", got)
	}
}

func TestNewLogFrom(t *testing.T) {
	l := NewLogFrom(5, 3, []Entry{{Term: 3, Index: 6}, {Term: 4, Index: 7}})
	if l.FirstIndex() != 6 || l.LastIndex() != 7 {
		t.Fatalf("first=%d last=%d", l.FirstIndex(), l.LastIndex())
	}
	if term, err := l.Term(7); err != nil || term != 4 {
		t.Fatalf("Term(7) = %d, %v", term, err)
	}
	if l.Committed() != 5 {
		t.Fatalf("committed = %d, want the snapshot index", l.Committed())
	}
	if s := l.String(); s == "" {
		t.Fatal("String() is empty")
	}
}

func TestLogTruncateFromNoop(t *testing.T) {
	l := entsLog(1, 1)
	l.truncateFrom(9) // past the end: nothing to do
	if l.LastIndex() != 2 {
		t.Fatalf("last = %d", l.LastIndex())
	}
}
