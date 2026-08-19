// Package storage is the durable half of a raftlite node: a write-ahead log of
// everything the consensus algorithm is not allowed to forget.
//
// Raft's safety argument rests on a promise about disks. A node that voted, or
// acknowledged an entry, must still remember doing so after a power cut --
// otherwise it can vote twice in one term, or acknowledge an entry and then
// deny holding it, and either one can lose a committed write. So the rule this
// package exists to enforce is simple: nothing goes on the wire until it is on
// the disk.
package storage

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/sahilkalgutkar/raftlite/internal/raft"
	"github.com/sahilkalgutkar/raftlite/internal/wire"
)

// Record types, written as the first byte of every framed record.
const (
	recHardState byte = 1
	recEntry     byte = 2
	// recTruncate marks everything from an index onward as discarded. A
	// follower repairing a diverged log needs this: the entries it is dropping
	// are already in the file, and the log is append-only, so the fact that
	// they are gone has to be recorded as its own event.
	recTruncate byte = 3
)

const walName = "raft.wal"

// Options configures a store.
type Options struct {
	// Dir is the directory the node owns. It is created if missing.
	Dir string
	// NoSync skips the fsync after every write. It makes tests and benchmarks
	// much faster and makes the durability guarantee a lie, so it is never on
	// by default.
	NoSync bool
	Logger *slog.Logger
}

// State is everything recovered from disk at startup.
type State struct {
	HardState raft.HardState
	Entries   []raft.Entry
}

// Store is a node's on-disk state.
type Store struct {
	dir    string
	file   *os.File
	buf    []byte
	noSync bool
	logger *slog.Logger

	lastIndex uint64
}

// Open recovers a node's durable state and returns a store ready to append to.
func Open(opts Options) (*Store, *State, error) {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.Dir == "" {
		return nil, nil, errors.New("storage: no directory configured")
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("storage: create dir: %w", err)
	}

	path := filepath.Join(opts.Dir, walName)
	state, goodBytes, err := replay(path, opts.Logger)
	if err != nil {
		return nil, nil, err
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("storage: open wal: %w", err)
	}
	// Drop anything after the last record that read back cleanly. A crash
	// mid-write leaves a partial record at the tail, and appending after it
	// would bury good data behind garbage forever.
	if err := f.Truncate(goodBytes); err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("storage: truncate wal: %w", err)
	}
	if _, err := f.Seek(goodBytes, io.SeekStart); err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("storage: seek wal: %w", err)
	}

	s := &Store{dir: opts.Dir, file: f, noSync: opts.NoSync, logger: opts.Logger}
	if n := len(state.Entries); n > 0 {
		s.lastIndex = state.Entries[n-1].Index
	}
	return s, state, nil
}

// Dir is the directory this store owns.
func (s *Store) Dir() string { return s.dir }

// LastIndex is the highest entry index currently on disk.
func (s *Store) LastIndex() uint64 { return s.lastIndex }

// Save appends a batch of durable state and syncs it once. Batching matters:
// an fsync per entry would put a disk round trip in the middle of every write,
// where one fsync per batch amortises it across everything the node learned in
// a single pass of its event loop.
func (s *Store) Save(hs *raft.HardState, entries []raft.Entry) error {
	if hs == nil && len(entries) == 0 {
		return nil
	}
	s.buf = s.buf[:0]

	if hs != nil {
		e := wire.NewEncoder(24)
		e.Byte(recHardState)
		wire.EncodeHardState(e, *hs)
		s.buf = wire.AppendFrame(s.buf, e.Result())
	}

	if len(entries) > 0 {
		// Overwriting existing indices means the log diverged and this node is
		// being repaired. Record the truncation before the replacements.
		if first := entries[0].Index; first <= s.lastIndex {
			e := wire.NewEncoder(16)
			e.Byte(recTruncate)
			e.Uvarint(first)
			s.buf = wire.AppendFrame(s.buf, e.Result())
		}
		for _, ent := range entries {
			e := wire.NewEncoder(32 + len(ent.Data))
			e.Byte(recEntry)
			wire.EncodeEntry(e, ent)
			s.buf = wire.AppendFrame(s.buf, e.Result())
		}
	}

	if _, err := s.file.Write(s.buf); err != nil {
		return fmt.Errorf("storage: write wal: %w", err)
	}
	if err := s.Sync(); err != nil {
		return err
	}

	if len(entries) > 0 {
		s.lastIndex = entries[len(entries)-1].Index
	}
	return nil
}

// Sync flushes the operating system's buffers down to the device.
func (s *Store) Sync() error {
	if s.noSync {
		return nil
	}
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("storage: sync wal: %w", err)
	}
	return nil
}

// Close releases the underlying file.
func (s *Store) Close() error {
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

// countingReader tracks how many bytes have been consumed, so replay can
// remember the offset just past the last record that verified.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// replay reads the log back into memory and reports how many bytes were
// trustworthy.
//
// Damage at the tail is expected -- that is what an interrupted write looks
// like -- so replay stops at the first record that fails to verify and reports
// the offset before it, rather than refusing to start. Damage in the middle is
// indistinguishable from damage at the tail from here, so it is handled the
// same way but logged loudly, since silently dropping the second half of a log
// is not something that should ever pass unnoticed.
func replay(path string, logger *slog.Logger) (*State, int64, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return &State{}, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("storage: open wal for replay: %w", err)
	}
	defer f.Close()

	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, 0, fmt.Errorf("storage: size wal: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("storage: rewind wal: %w", err)
	}

	cr := &countingReader{r: bufio.NewReader(f)}
	state := &State{}
	var good int64

	for {
		payload, err := wire.ReadFrame(cr, 0)
		if errors.Is(err, io.EOF) {
			break // clean end of file
		}
		if err != nil {
			logger.Warn("write-ahead log ends in a damaged record; discarding the tail",
				"path", path, "good_bytes", good, "file_bytes", size, "err", err)
			break
		}

		if err := applyRecord(state, payload); err != nil {
			logger.Warn("write-ahead log contains an unreadable record; discarding the tail",
				"path", path, "good_bytes", good, "err", err)
			break
		}
		good = cr.n
	}

	return state, good, nil
}

func applyRecord(state *State, payload []byte) error {
	if len(payload) == 0 {
		return errors.New("empty record")
	}
	d := wire.NewDecoder(payload)
	switch typ := d.Byte(); typ {
	case recHardState:
		hs := wire.DecodeHardState(d)
		if err := d.Err(); err != nil {
			return err
		}
		state.HardState = hs
	case recEntry:
		ent := wire.DecodeEntry(d)
		if err := d.Err(); err != nil {
			return err
		}
		state.Entries = append(state.Entries, ent)
	case recTruncate:
		from := d.Uvarint()
		if err := d.Err(); err != nil {
			return err
		}
		state.Entries = truncateFrom(state.Entries, from)
	default:
		return fmt.Errorf("unknown record type %d", typ)
	}
	return nil
}

func truncateFrom(ents []raft.Entry, from uint64) []raft.Entry {
	for i, e := range ents {
		if e.Index >= from {
			return ents[:i]
		}
	}
	return ents
}
