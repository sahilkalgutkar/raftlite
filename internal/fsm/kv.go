package fsm

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	"github.com/sahilkalgutkar/raftlite/internal/raft"
	"github.com/sahilkalgutkar/raftlite/internal/wire"
)

// Value is a stored value plus the revision it was written at.
type Value struct {
	Data     []byte
	Revision uint64
}

// Result is what applying one command produced. It goes back to whichever
// client was waiting on that log index.
type Result struct {
	// Value is the value after a successful write, or the current value after
	// a failed compare-and-swap, which saves the caller a follow-up read.
	Value []byte
	// Revision is the state machine's revision after the command.
	Revision uint64
	// Existed reports whether the key was present before the command ran.
	Existed bool
	// Swapped reports whether a compare-and-swap took effect.
	Swapped bool
	// Err describes a command that was rejected rather than one that failed to
	// replicate: a malformed payload, say. It is deliberately part of the
	// result rather than an error return, because every replica must produce
	// the same one.
	Err string
}

// KV is the state machine: a map, a revision counter, and nothing else.
//
// The mutex is not about consensus -- entries arrive from a single goroutine,
// already ordered. It is there because reads are served concurrently from HTTP
// handlers while that goroutine is applying.
type KV struct {
	mu       sync.RWMutex
	data     map[string]Value
	revision uint64
}

// NewKV returns an empty state machine.
func NewKV() *KV { return &KV{data: make(map[string]Value)} }

// Apply runs one committed log entry. Entries the state machine has no
// business interpreting -- the leader's no-op, membership changes -- are
// accepted and ignored, because the runtime hands over everything that commits
// and the alternative is teaching it which entries are which.
func (k *KV) Apply(e raft.Entry) Result {
	if e.Type != raft.EntryNormal {
		return Result{Revision: k.Revision()}
	}
	cmd, err := UnmarshalCommand(e.Data)
	if err != nil {
		// Every replica decodes the same bytes, so every replica records the
		// same failure. Refusing to apply it would be a divergence.
		return Result{Revision: k.Revision(), Err: err.Error()}
	}
	return k.applyCommand(cmd)
}

func (k *KV) applyCommand(cmd Command) Result {
	k.mu.Lock()
	defer k.mu.Unlock()

	current, existed := k.data[cmd.Key]
	res := Result{Existed: existed, Revision: k.revision}

	switch cmd.Op {
	case OpPut:
		k.revision++
		k.data[cmd.Key] = Value{Data: cmd.Value, Revision: k.revision}
		res.Revision = k.revision
		res.Value = cmd.Value

	case OpDelete:
		if !existed {
			return res
		}
		k.revision++
		delete(k.data, cmd.Key)
		res.Revision = k.revision
		res.Value = current.Data

	case OpCAS:
		if existed != cmd.ExpectExists || (existed && !bytes.Equal(current.Data, cmd.Expected)) {
			// Hand back what is actually there: the caller almost always wants
			// it, and a second round trip to fetch it could read a newer value.
			res.Value = current.Data
			return res
		}
		k.revision++
		k.data[cmd.Key] = Value{Data: cmd.Value, Revision: k.revision}
		res.Revision = k.revision
		res.Value = cmd.Value
		res.Swapped = true

	default:
		res.Err = fmt.Sprintf("fsm: unknown op %d", uint8(cmd.Op))
	}
	return res
}

// Get reads a key. It does not go through the log, so on a follower it can be
// stale; the HTTP layer is what decides whether a given read is allowed to be.
func (k *KV) Get(key string) (Value, bool) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	v, ok := k.data[key]
	return v, ok
}

// Len is the number of keys held.
func (k *KV) Len() int {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return len(k.data)
}

// Revision is the state machine's current revision.
func (k *KV) Revision() uint64 {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.revision
}

// Keys returns every key, sorted.
func (k *KV) Keys() []string {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.sortedKeysLocked()
}

func (k *KV) sortedKeysLocked() []string {
	keys := make([]string, 0, len(k.data))
	for key := range k.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// Snapshot serialises the whole state machine.
//
// Keys are written in sorted order, which is the detail that makes this
// worth its own note: Go randomises map iteration, so encoding in map order
// would give two replicas byte-different snapshots of identical state. That
// would not be wrong exactly, but it makes the images incomparable and turns
// any "did these two nodes agree?" check into a full semantic diff.
func (k *KV) Snapshot() ([]byte, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	e := wire.NewEncoder(64 * (len(k.data) + 1))
	e.Uvarint(k.revision)
	e.Uvarint(uint64(len(k.data)))
	for _, key := range k.sortedKeysLocked() {
		v := k.data[key]
		e.Text(key)
		e.Bytes(v.Data)
		e.Uvarint(v.Revision)
	}
	return e.Result(), nil
}

// minEntryBytes is the smallest a snapshot record can be: an empty key, an
// empty value and a one-byte revision.
const minEntryBytes = 3

// Restore replaces the state machine's contents with a snapshot. It is
// all-or-nothing: a snapshot that fails to decode leaves the previous state
// untouched rather than a half-loaded mixture of the two.
func (k *KV) Restore(data []byte) error {
	d := wire.NewDecoder(data)
	revision := d.Uvarint()
	count := d.Count(minEntryBytes)
	if err := d.Err(); err != nil {
		return fmt.Errorf("fsm: restore: %w", err)
	}

	next := make(map[string]Value, count)
	for i := 0; i < count; i++ {
		key := d.Text()
		value := d.Bytes()
		rev := d.Uvarint()
		if err := d.Err(); err != nil {
			return fmt.Errorf("fsm: restore entry %d: %w", i, err)
		}
		next[key] = Value{Data: value, Revision: rev}
	}

	k.mu.Lock()
	defer k.mu.Unlock()
	k.data = next
	k.revision = revision
	return nil
}
