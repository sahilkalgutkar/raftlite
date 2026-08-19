package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/sahilkalgutkar/raftlite/internal/raft"
	"github.com/sahilkalgutkar/raftlite/internal/wire"
)

const (
	snapshotSubdir = "snapshots"
	snapshotSuffix = ".snap"
	// keepSnapshots is how many images to retain. Keeping more than one costs
	// almost nothing and means a corrupt newest snapshot is survivable: the
	// node falls back to an older image and replays forward from there.
	keepSnapshots = 3
)

// SaveSnapshot makes a snapshot durable and compacts the log behind it.
//
// The ordering here is the whole point: the snapshot lands and is fsynced
// first, and only then is the log rewritten to drop the entries it covers. A
// crash between the two costs some redundant replay on restart; the reverse
// ordering would lose the entries permanently.
//
// tail is the entries the node still holds above the snapshot index, and hs
// the current hard state -- both are rewritten into the fresh log.
func (s *Store) SaveSnapshot(snap raft.Snapshot, hs raft.HardState, tail []raft.Entry) error {
	if snap.Meta.Index == 0 {
		return errors.New("storage: refusing to save an empty snapshot")
	}
	dir := filepath.Join(s.dir, snapshotSubdir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("storage: create snapshot dir: %w", err)
	}

	e := wire.NewEncoder(len(snap.Data) + 64)
	wire.EncodeSnapshot(e, snap)
	payload := wire.AppendFrame(nil, e.Result())

	name := fmt.Sprintf("%016d-%016d%s", snap.Meta.Index, snap.Meta.Term, snapshotSuffix)
	final := filepath.Join(dir, name)
	if err := writeFileAtomic(final, payload, s.noSync); err != nil {
		return err
	}

	if err := s.rewriteLog(hs, tail); err != nil {
		return err
	}
	s.pruneSnapshots(dir)

	s.logger.Info("saved snapshot and compacted the log",
		"index", snap.Meta.Index, "term", snap.Meta.Term, "bytes", len(snap.Data), "kept_entries", len(tail))
	return nil
}

// writeFileAtomic writes to a temporary file, syncs it, and renames it into
// place. A reader therefore only ever sees a complete file: rename is atomic,
// so there is no window where the snapshot exists but is half written.
func writeFileAtomic(path string, data []byte, noSync bool) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("storage: create temp file: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("storage: write temp file: %w", err)
	}
	if !noSync {
		if err := f.Sync(); err != nil {
			f.Close()
			os.Remove(tmp)
			return fmt.Errorf("storage: sync temp file: %w", err)
		}
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("storage: close temp file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("storage: rename temp file: %w", err)
	}
	return syncDir(filepath.Dir(path), noSync)
}

// syncDir flushes a directory entry, which is what actually makes a rename
// durable. Without it the file's contents survive a crash but the name
// pointing at them may not.
func syncDir(dir string, noSync bool) error {
	if noSync {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("storage: open dir for sync: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("storage: sync dir: %w", err)
	}
	return nil
}

// rewriteLog replaces the write-ahead log with a fresh one holding just the
// current hard state and the entries above the snapshot boundary.
func (s *Store) rewriteLog(hs raft.HardState, tail []raft.Entry) error {
	buf := []byte(nil)

	e := wire.NewEncoder(24)
	e.Byte(recHardState)
	wire.EncodeHardState(e, hs)
	buf = wire.AppendFrame(buf, e.Result())

	for _, ent := range tail {
		e := wire.NewEncoder(32 + len(ent.Data))
		e.Byte(recEntry)
		wire.EncodeEntry(e, ent)
		buf = wire.AppendFrame(buf, e.Result())
	}

	path := filepath.Join(s.dir, walName)
	if err := writeFileAtomic(path, buf, s.noSync); err != nil {
		return err
	}

	// Swap the open handle over to the file we just put in place.
	if err := s.file.Close(); err != nil {
		return fmt.Errorf("storage: close old wal: %w", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("storage: reopen wal: %w", err)
	}
	s.file = f

	s.lastIndex = 0
	if n := len(tail); n > 0 {
		s.lastIndex = tail[n-1].Index
	}
	return nil
}

// pruneSnapshots deletes all but the newest few images.
func (s *Store) pruneSnapshots(dir string) {
	names, err := snapshotNames(dir)
	if err != nil || len(names) <= keepSnapshots {
		return
	}
	for _, name := range names[:len(names)-keepSnapshots] {
		if err := os.Remove(filepath.Join(dir, name)); err != nil {
			s.logger.Warn("could not remove an old snapshot", "name", name, "err", err)
		}
	}
}

// snapshotNames returns the snapshot files in ascending order. The fixed-width
// zero-padded names mean lexical order is index order, so no parsing is needed
// just to sort them.
func snapshotNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("storage: read snapshot dir: %w", err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), snapshotSuffix) {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

// loadLatestSnapshot returns the newest snapshot that reads back intact.
// Trying older images on failure is why keepSnapshots is more than one: a
// snapshot that was being written during a crash should cost a little replay,
// not the whole node.
func loadLatestSnapshot(dir string, logger interface {
	Warn(string, ...any)
}) (*raft.Snapshot, error) {
	snapDir := filepath.Join(dir, snapshotSubdir)
	names, err := snapshotNames(snapDir)
	if err != nil {
		return nil, err
	}
	for i := len(names) - 1; i >= 0; i-- {
		path := filepath.Join(snapDir, names[i])
		snap, err := readSnapshot(path)
		if err == nil {
			return snap, nil
		}
		logger.Warn("skipping an unreadable snapshot", "path", path, "err", err)
	}
	return nil, nil
}

func readSnapshot(path string) (*raft.Snapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("storage: open snapshot: %w", err)
	}
	defer f.Close()

	payload, err := wire.ReadFrame(f, 0)
	if err != nil {
		return nil, fmt.Errorf("storage: read snapshot: %w", err)
	}
	d := wire.NewDecoder(payload)
	snap, err := wire.DecodeSnapshot(d)
	if err != nil {
		return nil, fmt.Errorf("storage: decode snapshot: %w", err)
	}
	return &snap, nil
}
