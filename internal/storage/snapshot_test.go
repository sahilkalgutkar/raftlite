package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sahilkalgutkar/raftlite/internal/raft"
)

func sampleSnapshot(index, term uint64) raft.Snapshot {
	return raft.Snapshot{
		Meta: raft.SnapshotMeta{
			Index: index,
			Term:  term,
			Config: raft.NewConfig(
				raft.Member{ID: 1, Addr: "127.0.0.1:9001"},
				raft.Member{ID: 2, Addr: "127.0.0.1:9002"},
			),
		},
		Data: []byte("state machine image"),
	}
}

func TestSaveSnapshotCompactsTheLog(t *testing.T) {
	dir := t.TempDir()
	s, _ := openTemp(t, dir)

	if err := s.Save(&raft.HardState{Term: 3, Commit: 5}, entries(1, 1, 1, 2, 2, 3)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	walBefore, err := os.Stat(filepath.Join(dir, walName))
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}

	hs := raft.HardState{Term: 3, Vote: 1, Commit: 5}
	tail := entries(5, 3, 3) // entries 5 and 6 survive the snapshot at index 4
	if err := s.SaveSnapshot(sampleSnapshot(4, 2), hs, tail); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	walAfter, err := os.Stat(filepath.Join(dir, walName))
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if walAfter.Size() >= walBefore.Size() {
		t.Fatalf("log did not shrink: %d -> %d bytes", walBefore.Size(), walAfter.Size())
	}
	if s.LastIndex() != 6 {
		t.Fatalf("last index = %d, want 6", s.LastIndex())
	}
	_ = s.Close()

	_, state := openTemp(t, dir)
	if state.Snapshot == nil || state.Snapshot.Meta.Index != 4 || state.Snapshot.Meta.Term != 2 {
		t.Fatalf("recovered snapshot = %+v", state.Snapshot)
	}
	if string(state.Snapshot.Data) != "state machine image" {
		t.Fatalf("snapshot data = %q", state.Snapshot.Data)
	}
	if !state.Snapshot.Meta.Config.Equal(sampleSnapshot(4, 2).Meta.Config) {
		t.Fatalf("snapshot configuration = %v", state.Snapshot.Meta.Config)
	}
	if state.HardState != hs {
		t.Fatalf("hard state = %v, want %v", state.HardState, hs)
	}
	if len(state.Entries) != 2 || state.Entries[0].Index != 5 || state.Entries[1].Index != 6 {
		t.Fatalf("recovered entries = %v, want just 5 and 6", state.Entries)
	}
}

func TestAppendingAfterASnapshotStillRecovers(t *testing.T) {
	dir := t.TempDir()
	s, _ := openTemp(t, dir)
	if err := s.Save(nil, entries(1, 1, 1, 1)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s.SaveSnapshot(sampleSnapshot(3, 1), raft.HardState{Term: 1, Commit: 3}, entries(4, 1)); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := s.Save(nil, entries(5, 1, 1)); err != nil {
		t.Fatalf("Save after snapshot: %v", err)
	}
	_ = s.Close()

	_, state := openTemp(t, dir)
	if len(state.Entries) != 3 {
		t.Fatalf("recovered %v, want indices 4..6", state.Entries)
	}
	if state.Entries[0].Index != 4 || state.Entries[2].Index != 6 {
		t.Fatalf("recovered %v", state.Entries)
	}
}

func TestEntriesCoveredByASnapshotAreDroppedOnRecovery(t *testing.T) {
	// A crash between writing the snapshot and rewriting the log leaves
	// entries behind that the image already covers. Recovery drops them
	// rather than double-applying them.
	dir := t.TempDir()
	s, _ := openTemp(t, dir)
	if err := s.Save(&raft.HardState{Term: 1, Commit: 4}, entries(1, 1, 1, 1, 1)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	_ = s.Close()

	// Write the snapshot file by hand, without touching the log.
	snapDir := filepath.Join(dir, snapshotSubdir)
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	s2, _ := openTemp(t, dir)
	if err := s2.SaveSnapshot(sampleSnapshot(3, 1), raft.HardState{Term: 1, Commit: 4}, entries(1, 1, 1, 1, 1)); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	_ = s2.Close()

	_, state := openTemp(t, dir)
	for _, e := range state.Entries {
		if e.Index <= 3 {
			t.Fatalf("entry %d survived a snapshot covering it: %v", e.Index, state.Entries)
		}
	}
}

func TestOldSnapshotsArePruned(t *testing.T) {
	dir := t.TempDir()
	s, _ := openTemp(t, dir)

	for i := uint64(1); i <= keepSnapshots+3; i++ {
		if err := s.Save(nil, entries(i, 1)); err != nil {
			t.Fatalf("Save: %v", err)
		}
		if err := s.SaveSnapshot(sampleSnapshot(i, 1), raft.HardState{Term: 1, Commit: i}, nil); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}
	}

	names, err := snapshotNames(filepath.Join(dir, snapshotSubdir))
	if err != nil {
		t.Fatalf("snapshotNames: %v", err)
	}
	if len(names) != keepSnapshots {
		t.Fatalf("kept %d snapshots, want %d: %v", len(names), keepSnapshots, names)
	}
	// The names are fixed width and zero padded so lexical order is index
	// order; the survivors must be the newest ones.
	if !strings.HasPrefix(names[len(names)-1], "0000000000000006") {
		t.Fatalf("newest surviving snapshot is %s", names[len(names)-1])
	}
}

func TestACorruptNewestSnapshotFallsBackToAnOlderOne(t *testing.T) {
	dir := t.TempDir()
	s, _ := openTemp(t, dir)
	for _, idx := range []uint64{1, 2} {
		if err := s.Save(nil, entries(idx, 1)); err != nil {
			t.Fatalf("Save: %v", err)
		}
		if err := s.SaveSnapshot(sampleSnapshot(idx, 1), raft.HardState{Term: 1, Commit: idx}, nil); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}
	}
	_ = s.Close()

	snapDir := filepath.Join(dir, snapshotSubdir)
	names, _ := snapshotNames(snapDir)
	newest := filepath.Join(snapDir, names[len(names)-1])
	raw, err := os.ReadFile(newest)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	raw[len(raw)-1] ^= 0xFF // break the payload, and with it the checksum
	if err := os.WriteFile(newest, raw, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, state := openTemp(t, dir)
	if state.Snapshot == nil {
		t.Fatal("a single corrupt snapshot cost us every snapshot")
	}
	if state.Snapshot.Meta.Index != 1 {
		t.Fatalf("recovered snapshot at index %d, want the older intact one", state.Snapshot.Meta.Index)
	}
}

func TestEveryCorruptSnapshotLeavesNone(t *testing.T) {
	dir := t.TempDir()
	s, _ := openTemp(t, dir)
	if err := s.Save(nil, entries(1, 1)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s.SaveSnapshot(sampleSnapshot(1, 1), raft.HardState{Term: 1}, nil); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	_ = s.Close()

	snapDir := filepath.Join(dir, snapshotSubdir)
	names, _ := snapshotNames(snapDir)
	if err := os.WriteFile(filepath.Join(snapDir, names[0]), []byte("not a snapshot at all"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, state := openTemp(t, dir)
	if state.Snapshot != nil {
		t.Fatalf("a garbage file decoded into %+v", state.Snapshot)
	}
}

func TestSaveSnapshotRejectsAnEmptyImage(t *testing.T) {
	s, _ := openTemp(t, t.TempDir())
	if err := s.SaveSnapshot(raft.Snapshot{}, raft.HardState{}, nil); err == nil {
		t.Fatal("an empty snapshot was accepted")
	}
}

func TestNonSnapshotFilesAreIgnored(t *testing.T) {
	dir := t.TempDir()
	snapDir := filepath.Join(dir, snapshotSubdir)
	if err := os.MkdirAll(filepath.Join(snapDir, "a-directory"+snapshotSuffix), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(snapDir, "notes.txt"), []byte("hi"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	names, err := snapshotNames(snapDir)
	if err != nil {
		t.Fatalf("snapshotNames: %v", err)
	}
	if len(names) != 0 {
		t.Fatalf("picked up %v", names)
	}
	if _, state := openTemp(t, dir); state.Snapshot != nil {
		t.Fatalf("recovered a snapshot from nothing: %+v", state.Snapshot)
	}
}

func TestSnapshotNamesOnAMissingDirectory(t *testing.T) {
	names, err := snapshotNames(filepath.Join(t.TempDir(), "nope"))
	if err != nil || names != nil {
		t.Fatalf("snapshotNames = %v, %v", names, err)
	}
}

func TestWriteFileAtomicFailsOnAnUnwritablePath(t *testing.T) {
	if err := writeFileAtomic(filepath.Join(t.TempDir(), "missing", "f"), []byte("x"), true); err == nil {
		t.Fatal("writing under a missing directory succeeded")
	}
}

func TestDropThroughHelper(t *testing.T) {
	ents := entries(1, 1, 1, 1)
	if got := dropThrough(ents, 0); len(got) != 3 {
		t.Fatalf("dropping nothing changed the slice: %v", got)
	}
	if got := dropThrough(ents, 2); len(got) != 1 || got[0].Index != 3 {
		t.Fatalf("dropThrough(2) = %v", got)
	}
	if got := dropThrough(ents, 99); got != nil {
		t.Fatalf("dropping everything left %v", got)
	}
}
