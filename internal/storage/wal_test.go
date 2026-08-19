package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sahilkalgutkar/raftlite/internal/raft"
	"github.com/sahilkalgutkar/raftlite/internal/wire"
)

func openTemp(t *testing.T, dir string) (*Store, *State) {
	t.Helper()
	s, state, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open(%s): %v", dir, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, state
}

func entries(from uint64, terms ...uint64) []raft.Entry {
	out := make([]raft.Entry, 0, len(terms))
	for i, term := range terms {
		out = append(out, raft.Entry{Index: from + uint64(i), Term: term, Type: raft.EntryNormal})
	}
	return out
}

func TestEmptyDirectoryRecoversEmptyState(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "node1")
	s, state := openTemp(t, dir)

	if !state.HardState.IsEmpty() || len(state.Entries) != 0 {
		t.Fatalf("fresh store recovered %+v", state)
	}
	if s.LastIndex() != 0 || s.Dir() != dir {
		t.Fatalf("last index = %d, dir = %s", s.LastIndex(), s.Dir())
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("Open did not create the directory: %v", err)
	}
}

func TestSaveThenReopenRecoversEverything(t *testing.T) {
	dir := t.TempDir()
	s, _ := openTemp(t, dir)

	hs := raft.HardState{Term: 4, Vote: 2, Commit: 2}
	ents := entries(1, 3, 4, 4)
	if err := s.Save(&hs, ents); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, state := openTemp(t, dir)
	if state.HardState != hs {
		t.Fatalf("hard state = %v, want %v", state.HardState, hs)
	}
	if len(state.Entries) != 3 {
		t.Fatalf("recovered %d entries, want 3", len(state.Entries))
	}
	for i, want := range ents {
		if state.Entries[i].Index != want.Index || state.Entries[i].Term != want.Term {
			t.Fatalf("entry %d = %v, want %v", i, state.Entries[i], want)
		}
	}
}

func TestLatestHardStateWins(t *testing.T) {
	dir := t.TempDir()
	s, _ := openTemp(t, dir)

	for term := uint64(1); term <= 5; term++ {
		hs := raft.HardState{Term: term, Vote: raft.NodeID(term)}
		if err := s.Save(&hs, nil); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}
	_ = s.Close()

	_, state := openTemp(t, dir)
	if state.HardState.Term != 5 || state.HardState.Vote != 5 {
		t.Fatalf("recovered %v, want the newest hard state", state.HardState)
	}
}

func TestSaveIsAppendOnlyAcrossReopens(t *testing.T) {
	dir := t.TempDir()

	s, _ := openTemp(t, dir)
	if err := s.Save(nil, entries(1, 1, 1)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	_ = s.Close()

	// A reopened store must continue the log, not clobber it.
	s2, state := openTemp(t, dir)
	if s2.LastIndex() != 2 {
		t.Fatalf("last index after reopen = %d, want 2", s2.LastIndex())
	}
	if len(state.Entries) != 2 {
		t.Fatalf("recovered %d entries", len(state.Entries))
	}
	if err := s2.Save(nil, entries(3, 2)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	_ = s2.Close()

	_, final := openTemp(t, dir)
	if len(final.Entries) != 3 || final.Entries[2].Term != 2 {
		t.Fatalf("recovered %v", final.Entries)
	}
}

func TestOverwritingEntriesRecordsATruncation(t *testing.T) {
	dir := t.TempDir()
	s, _ := openTemp(t, dir)

	// A follower that accepted four entries from one leader...
	if err := s.Save(nil, entries(1, 1, 1, 1, 1)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	// ...then gets repaired by the next leader, which replaces indices 3 and 4.
	if err := s.Save(nil, entries(3, 2, 2)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	_ = s.Close()

	_, state := openTemp(t, dir)
	if len(state.Entries) != 4 {
		t.Fatalf("recovered %d entries, want 4", len(state.Entries))
	}
	for i, want := range []uint64{1, 1, 2, 2} {
		if state.Entries[i].Term != want {
			t.Fatalf("entry %d has term %d, want %d (%v)", i+1, state.Entries[i].Term, want, state.Entries)
		}
	}
	if state.Entries[3].Index != 4 {
		t.Fatalf("indices are wrong after repair: %v", state.Entries)
	}
}

func TestTruncationBackToTheStartOfTheLog(t *testing.T) {
	dir := t.TempDir()
	s, _ := openTemp(t, dir)
	if err := s.Save(nil, entries(1, 1, 1, 1)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s.Save(nil, entries(1, 9)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	_ = s.Close()

	_, state := openTemp(t, dir)
	if len(state.Entries) != 1 || state.Entries[0].Term != 9 {
		t.Fatalf("recovered %v, want a single term-9 entry", state.Entries)
	}
}

func TestCrashMidWriteDropsThePartialRecord(t *testing.T) {
	dir := t.TempDir()
	s, _ := openTemp(t, dir)
	if err := s.Save(&raft.HardState{Term: 2, Vote: 1}, entries(1, 1, 1)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	_ = s.Close()

	path := filepath.Join(dir, walName)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	// Simulate a power cut in the middle of appending a third entry: a valid
	// header followed by only part of its payload.
	partial := wire.AppendFrame(nil, []byte{recEntry, 3, 1, 0, 0, 0, 0, 0})
	if err := os.WriteFile(path, append(before, partial[:len(partial)-4]...), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	s2, state := openTemp(t, dir)
	if len(state.Entries) != 2 || state.HardState.Term != 2 {
		t.Fatalf("recovered %+v, want the two complete entries", state)
	}

	// The damaged tail must be gone from the file, so the next append lands
	// somewhere a replay can reach.
	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if after.Size() != int64(len(before)) {
		t.Fatalf("file is %d bytes, want it truncated back to %d", after.Size(), len(before))
	}
	if err := s2.Save(nil, entries(3, 1)); err != nil {
		t.Fatalf("Save after recovery: %v", err)
	}
	_ = s2.Close()

	_, final := openTemp(t, dir)
	if len(final.Entries) != 3 {
		t.Fatalf("recovered %d entries after appending post-recovery", len(final.Entries))
	}
}

func TestBitFlipTruncatesFromTheDamagedRecord(t *testing.T) {
	dir := t.TempDir()
	s, _ := openTemp(t, dir)
	if err := s.Save(nil, entries(1, 1, 1, 1, 1)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	_ = s.Close()

	path := filepath.Join(dir, walName)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	// Corrupt a byte inside the second record's payload. Its checksum fails,
	// and everything after it is treated as unreachable.
	raw[len(raw)/2] ^= 0xFF
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, state := openTemp(t, dir)
	if len(state.Entries) >= 4 {
		t.Fatalf("corruption did not stop replay: recovered %d entries", len(state.Entries))
	}
	for i, e := range state.Entries {
		if e.Index != uint64(i+1) {
			t.Fatalf("recovered entries are not a clean prefix: %v", state.Entries)
		}
	}
}

func TestUnknownRecordTypeStopsReplayCleanly(t *testing.T) {
	dir := t.TempDir()
	s, _ := openTemp(t, dir)
	if err := s.Save(nil, entries(1, 1)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	_ = s.Close()

	path := filepath.Join(dir, walName)
	raw, _ := os.ReadFile(path)
	// A record written by some future version this build does not understand.
	raw = wire.AppendFrame(raw, []byte{99, 1, 2, 3})
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, state := openTemp(t, dir)
	if len(state.Entries) != 1 {
		t.Fatalf("recovered %d entries, want the one that came before the unknown record", len(state.Entries))
	}
}

func TestEmptyRecordIsRejected(t *testing.T) {
	if err := applyRecord(&State{}, nil); err == nil {
		t.Fatal("an empty record was accepted")
	}
}

func TestSaveWithNothingToDoIsANoOp(t *testing.T) {
	dir := t.TempDir()
	s, _ := openTemp(t, dir)
	if err := s.Save(nil, nil); err != nil {
		t.Fatalf("Save: %v", err)
	}
	info, err := os.Stat(filepath.Join(dir, walName))
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("empty save wrote %d bytes", info.Size())
	}
}

func TestNoSyncStillPersists(t *testing.T) {
	dir := t.TempDir()
	s, _, err := Open(Options{Dir: dir, NoSync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Save(&raft.HardState{Term: 1}, entries(1, 1)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	_ = s.Close()

	_, state := openTemp(t, dir)
	if len(state.Entries) != 1 || state.HardState.Term != 1 {
		t.Fatalf("recovered %+v", state)
	}
}

func TestOpenRejectsABadDirectory(t *testing.T) {
	if _, _, err := Open(Options{}); err == nil {
		t.Fatal("Open with no directory succeeded")
	}

	// A path whose parent is a regular file cannot become a directory.
	file := filepath.Join(t.TempDir(), "afile")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, _, err := Open(Options{Dir: filepath.Join(file, "sub")}); err == nil {
		t.Fatal("Open under a regular file succeeded")
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	s, _ := openTemp(t, t.TempDir())
	if err := s.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestSaveAfterCloseFails(t *testing.T) {
	dir := t.TempDir()
	s, _, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	f := s.file
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Save(&raft.HardState{Term: 1}, nil); err == nil {
		t.Fatal("Save on a closed file succeeded")
	}
}

func TestTruncateFromHelper(t *testing.T) {
	ents := entries(1, 1, 1, 1)
	if got := truncateFrom(ents, 9); len(got) != 3 {
		t.Fatalf("truncating past the end changed the slice: %v", got)
	}
	if got := truncateFrom(ents, 1); len(got) != 0 {
		t.Fatalf("truncating from the head left %v", got)
	}
}

func TestOpenSurfacesAnUnreadableLog(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: permission bits do not apply")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, walName)
	if err := os.WriteFile(path, []byte("data"), 0o000); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, _, err := Open(Options{Dir: dir}); err == nil {
		t.Fatal("Open succeeded on a log it cannot read")
	}
}

func TestOpenSurfacesAnUnwritableLog(t *testing.T) {
	dir := t.TempDir()
	// A directory where the log file belongs: readable enough to replay
	// nothing, impossible to append to.
	if err := os.Mkdir(filepath.Join(dir, walName), 0o755); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	if _, _, err := Open(Options{Dir: dir}); err == nil {
		t.Fatal("Open succeeded on a log it cannot write")
	}
}
