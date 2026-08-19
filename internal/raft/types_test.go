package raft

import "testing"

func TestRoleString(t *testing.T) {
	for role, want := range map[Role]string{
		Follower:     "follower",
		PreCandidate: "pre-candidate",
		Candidate:    "candidate",
		Leader:       "leader",
	} {
		if got := role.String(); got != want {
			t.Fatalf("Role(%d) = %q, want %q", role, got, want)
		}
	}
	if Role(9).String() == "" {
		t.Fatal("unknown role has no string form")
	}
}

func TestEntryTypeString(t *testing.T) {
	for typ, want := range map[EntryType]string{
		EntryNormal:     "normal",
		EntryNoOp:       "noop",
		EntryConfChange: "confchange",
	} {
		if got := typ.String(); got != want {
			t.Fatalf("EntryType(%d) = %q, want %q", typ, got, want)
		}
	}
	if EntryType(9).String() == "" {
		t.Fatal("unknown entry type has no string form")
	}
	if (Entry{Index: 1, Term: 2, Data: []byte("x")}).String() == "" {
		t.Fatal("entry has no string form")
	}
}

func TestHardState(t *testing.T) {
	if !(HardState{}).IsEmpty() {
		t.Fatal("zero hard state should be empty")
	}
	for _, hs := range []HardState{{Term: 1}, {Vote: 2}, {Commit: 3}} {
		if hs.IsEmpty() {
			t.Fatalf("%v reported empty", hs)
		}
	}
	a := HardState{Term: 2, Vote: 1, Commit: 5}
	if !a.Equal(HardState{Term: 2, Vote: 1, Commit: 5}) {
		t.Fatal("identical hard states compared unequal")
	}
	if a.Equal(HardState{Term: 3, Vote: 1, Commit: 5}) {
		t.Fatal("different terms compared equal")
	}
	if a.String() == "" {
		t.Fatal("hard state has no string form")
	}
}

func TestNodeIDString(t *testing.T) {
	if NodeID(7).String() != "node-7" {
		t.Fatalf("NodeID(7) = %q", NodeID(7).String())
	}
	if None != 0 {
		t.Fatal("None must be the zero NodeID")
	}
}

func TestSnapshotIsEmpty(t *testing.T) {
	var nilSnap *Snapshot
	if !nilSnap.IsEmpty() {
		t.Fatal("nil snapshot should be empty")
	}
	if !(&Snapshot{}).IsEmpty() {
		t.Fatal("zero snapshot should be empty")
	}
	if (&Snapshot{Meta: SnapshotMeta{Index: 1}}).IsEmpty() {
		t.Fatal("snapshot at index 1 is not empty")
	}
}
