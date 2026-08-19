// Package fsm is the replicated state machine raftlite puts behind the
// consensus layer: an ordinary key-value map, plus the encoding that turns a
// client's write into a log entry.
//
// The one rule that matters here is determinism. Every replica applies the
// same entries in the same order, so applying them has to produce byte-
// identical state everywhere -- no map iteration order leaking into a
// snapshot, no timestamps, no randomness, no "skip it if it looks stale".
// Anything that varies between replicas turns into a divergence that consensus
// cannot see and cannot fix.
package fsm

import (
	"errors"
	"fmt"

	"github.com/sahilkalgutkar/raftlite/internal/wire"
)

// Op is the kind of mutation a command performs.
type Op uint8

const (
	// OpPut sets a key unconditionally.
	OpPut Op = iota
	// OpDelete removes a key.
	OpDelete
	// OpCAS sets a key only if its current value matches what the caller
	// expected. This is what lets clients build anything stronger than
	// last-writer-wins on top -- locks, leases, counters -- without the state
	// machine knowing about any of them.
	OpCAS
)

func (o Op) String() string {
	switch o {
	case OpPut:
		return "put"
	case OpDelete:
		return "delete"
	case OpCAS:
		return "cas"
	default:
		return fmt.Sprintf("op(%d)", uint8(o))
	}
}

// ErrMalformedCommand is returned when a command cannot be decoded.
var ErrMalformedCommand = errors.New("fsm: malformed command")

// Command is one mutation, as it travels through the log.
type Command struct {
	Op    Op
	Key   string
	Value []byte

	// Expected and ExpectExists are the comparison half of a compare-and-swap.
	// ExpectExists false means "only if the key is absent", which is how a
	// caller expresses create-if-not-exists.
	Expected     []byte
	ExpectExists bool
}

func (c Command) String() string {
	return fmt.Sprintf("%s %q (%d bytes)", c.Op, c.Key, len(c.Value))
}

// Marshal encodes a command for storage in a log entry.
func (c Command) Marshal() []byte {
	e := wire.NewEncoder(32 + len(c.Key) + len(c.Value) + len(c.Expected))
	e.Byte(byte(c.Op))
	e.Text(c.Key)
	e.Bytes(c.Value)
	e.Bool(c.ExpectExists)
	e.Bytes(c.Expected)
	return e.Result()
}

// UnmarshalCommand decodes a command written by Marshal.
func UnmarshalCommand(b []byte) (Command, error) {
	d := wire.NewDecoder(b)
	c := Command{
		Op:  Op(d.Byte()),
		Key: d.Text(),
	}
	c.Value = d.Bytes()
	c.ExpectExists = d.Bool()
	c.Expected = d.Bytes()
	if err := d.Err(); err != nil {
		return Command{}, fmt.Errorf("%w: %v", ErrMalformedCommand, err)
	}
	if c.Op > OpCAS {
		return Command{}, fmt.Errorf("%w: unknown op %d", ErrMalformedCommand, uint8(c.Op))
	}
	return c, nil
}

// Put builds an unconditional write.
func Put(key string, value []byte) Command {
	return Command{Op: OpPut, Key: key, Value: value}
}

// Delete builds a removal.
func Delete(key string) Command { return Command{Op: OpDelete, Key: key} }

// CompareAndSwap builds a conditional write. Pass exists=false to mean "only
// if the key does not exist yet".
func CompareAndSwap(key string, expected, value []byte, exists bool) Command {
	return Command{Op: OpCAS, Key: key, Value: value, Expected: expected, ExpectExists: exists}
}
