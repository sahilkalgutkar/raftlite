package fsm

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/sahilkalgutkar/raftlite/internal/raft"
)

func entry(index uint64, cmd Command) raft.Entry {
	return raft.Entry{Index: index, Term: 1, Type: raft.EntryNormal, Data: cmd.Marshal()}
}

func TestPutAndGet(t *testing.T) {
	kv := NewKV()

	res := kv.Apply(entry(1, Put("alpha", []byte("one"))))
	if res.Existed || res.Revision != 1 || string(res.Value) != "one" {
		t.Fatalf("put result = %+v", res)
	}

	v, ok := kv.Get("alpha")
	if !ok || string(v.Data) != "one" || v.Revision != 1 {
		t.Fatalf("get = %+v, %v", v, ok)
	}
	if _, ok := kv.Get("missing"); ok {
		t.Fatal("a key that was never written came back")
	}
	if kv.Len() != 1 || kv.Revision() != 1 {
		t.Fatalf("len=%d revision=%d", kv.Len(), kv.Revision())
	}

	// Overwriting bumps the revision and reports that the key was there.
	res = kv.Apply(entry(2, Put("alpha", []byte("two"))))
	if !res.Existed || res.Revision != 2 {
		t.Fatalf("overwrite result = %+v", res)
	}
	if kv.Len() != 1 {
		t.Fatalf("overwrite grew the map to %d keys", kv.Len())
	}
}

func TestDelete(t *testing.T) {
	kv := NewKV()
	kv.Apply(entry(1, Put("k", []byte("v"))))

	res := kv.Apply(entry(2, Delete("k")))
	if !res.Existed || res.Revision != 2 || string(res.Value) != "v" {
		t.Fatalf("delete result = %+v", res)
	}
	if _, ok := kv.Get("k"); ok {
		t.Fatal("key survived deletion")
	}

	// Deleting something absent is a no-op, and must not move the revision --
	// otherwise two replicas that saw the same log would report different
	// revisions for identical state.
	res = kv.Apply(entry(3, Delete("k")))
	if res.Existed || res.Revision != 2 {
		t.Fatalf("redundant delete = %+v", res)
	}
}

func TestCompareAndSwap(t *testing.T) {
	kv := NewKV()

	t.Run("create if absent", func(t *testing.T) {
		res := kv.Apply(entry(1, CompareAndSwap("lock", nil, []byte("held by A"), false)))
		if !res.Swapped || string(res.Value) != "held by A" {
			t.Fatalf("result = %+v", res)
		}
	})

	t.Run("create fails once it exists", func(t *testing.T) {
		res := kv.Apply(entry(2, CompareAndSwap("lock", nil, []byte("held by B"), false)))
		if res.Swapped {
			t.Fatal("two callers both created the same key")
		}
		if string(res.Value) != "held by A" {
			t.Fatalf("failed swap did not report the current value: %+v", res)
		}
		if kv.Revision() != 1 {
			t.Fatalf("a failed swap moved the revision to %d", kv.Revision())
		}
	})

	t.Run("swap on a matching value", func(t *testing.T) {
		res := kv.Apply(entry(3, CompareAndSwap("lock", []byte("held by A"), []byte("held by B"), true)))
		if !res.Swapped || string(res.Value) != "held by B" {
			t.Fatalf("result = %+v", res)
		}
	})

	t.Run("swap on a stale value fails", func(t *testing.T) {
		res := kv.Apply(entry(4, CompareAndSwap("lock", []byte("held by A"), []byte("held by C"), true)))
		if res.Swapped {
			t.Fatal("a stale comparison succeeded")
		}
		if string(res.Value) != "held by B" {
			t.Fatalf("current value = %q", res.Value)
		}
	})

	t.Run("expecting an absent key that exists", func(t *testing.T) {
		res := kv.Apply(entry(5, CompareAndSwap("nothing", []byte("x"), []byte("y"), true)))
		if res.Swapped {
			t.Fatal("swapped against a key that does not exist")
		}
	})
}

func TestNonNormalEntriesAreIgnored(t *testing.T) {
	kv := NewKV()
	kv.Apply(entry(1, Put("k", []byte("v"))))

	for _, typ := range []raft.EntryType{raft.EntryNoOp, raft.EntryConfChange} {
		res := kv.Apply(raft.Entry{Index: 2, Type: typ, Data: []byte("not a command")})
		if res.Err != "" {
			t.Fatalf("%s entry produced an error: %s", typ, res.Err)
		}
		if res.Revision != 1 {
			t.Fatalf("%s entry moved the revision to %d", typ, res.Revision)
		}
	}
	if kv.Len() != 1 {
		t.Fatalf("bookkeeping entries changed the state machine")
	}
}

func TestMalformedCommandFailsIdentically(t *testing.T) {
	// A payload that cannot decode has to produce the same outcome on every
	// replica. Recording the failure keeps them identical; skipping it on some
	// nodes and not others would not.
	a, b := NewKV(), NewKV()
	bad := raft.Entry{Index: 1, Type: raft.EntryNormal, Data: []byte{0xFF}}

	ra, rb := a.Apply(bad), b.Apply(bad)
	if ra.Err == "" || ra.Err != rb.Err {
		t.Fatalf("errors differ: %q vs %q", ra.Err, rb.Err)
	}
	if a.Len() != 0 || b.Len() != 0 {
		t.Fatal("a malformed command mutated state")
	}
}

func TestUnknownOpIsRecordedNotApplied(t *testing.T) {
	kv := NewKV()
	res := kv.applyCommand(Command{Op: Op(99), Key: "k"})
	if res.Err == "" {
		t.Fatal("an unknown op was applied silently")
	}
	if kv.Len() != 0 {
		t.Fatal("an unknown op mutated state")
	}
	if Op(99).String() == "" {
		t.Fatal("unknown op has no string form")
	}
}

func TestCommandRoundTrip(t *testing.T) {
	cases := []Command{
		Put("k", []byte("v")),
		Put("", nil),
		Delete("gone"),
		CompareAndSwap("lock", []byte("old"), []byte("new"), true),
		CompareAndSwap("lock", nil, []byte("new"), false),
	}
	for _, want := range cases {
		got, err := UnmarshalCommand(want.Marshal())
		if err != nil {
			t.Fatalf("UnmarshalCommand(%v): %v", want, err)
		}
		if got.Op != want.Op || got.Key != want.Key ||
			!bytes.Equal(got.Value, want.Value) ||
			!bytes.Equal(got.Expected, want.Expected) ||
			got.ExpectExists != want.ExpectExists {
			t.Fatalf("round trip: %+v != %+v", got, want)
		}
		if want.String() == "" {
			t.Fatal("command has no string form")
		}
	}
}

func TestUnmarshalCommandRejectsGarbage(t *testing.T) {
	full := Put("key", []byte("value")).Marshal()
	for cut := 0; cut < len(full); cut++ {
		if _, err := UnmarshalCommand(full[:cut]); err == nil {
			t.Fatalf("truncating to %d of %d bytes decoded cleanly", cut, len(full))
		}
	}
	bad := Command{Op: Op(200), Key: "k"}.Marshal()
	if _, err := UnmarshalCommand(bad); !errors.Is(err, ErrMalformedCommand) {
		t.Fatalf("unknown op = %v, want ErrMalformedCommand", err)
	}
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	kv := NewKV()
	for i := 0; i < 50; i++ {
		kv.Apply(entry(uint64(i+1), Put(fmt.Sprintf("key-%02d", i), []byte(fmt.Sprintf("value-%d", i)))))
	}
	kv.Apply(entry(100, Delete("key-07")))

	data, err := kv.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	restored := NewKV()
	if err := restored.Restore(data); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if restored.Len() != kv.Len() || restored.Revision() != kv.Revision() {
		t.Fatalf("restored len/revision = %d/%d, want %d/%d",
			restored.Len(), restored.Revision(), kv.Len(), kv.Revision())
	}
	for _, key := range kv.Keys() {
		want, _ := kv.Get(key)
		got, ok := restored.Get(key)
		if !ok || !bytes.Equal(got.Data, want.Data) || got.Revision != want.Revision {
			t.Fatalf("key %q restored as %+v, want %+v", key, got, want)
		}
	}
	if _, ok := restored.Get("key-07"); ok {
		t.Fatal("a deleted key came back through the snapshot")
	}
}

func TestSnapshotsAreByteIdenticalForIdenticalState(t *testing.T) {
	// Go randomises map iteration, so encoding in map order would give two
	// replicas different bytes for the same state. Sorted keys are what make
	// "did these nodes agree?" a byte comparison instead of a semantic diff.
	build := func(order []int) *KV {
		kv := NewKV()
		idx := uint64(0)
		for _, i := range order {
			idx++
			kv.Apply(entry(idx, Put(fmt.Sprintf("key-%03d", i), []byte("v"))))
		}
		return kv
	}
	forward := make([]int, 100)
	for i := range forward {
		forward[i] = i
	}
	shuffled := append([]int(nil), forward...)
	rng := rand.New(rand.NewPCG(1, 2))
	rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

	a, _ := build(forward).Snapshot()
	for i := 0; i < 5; i++ {
		b, _ := build(forward).Snapshot()
		if !bytes.Equal(a, b) {
			t.Fatal("two snapshots of identical state differ")
		}
	}

	// Written in a different order the revisions differ, so the bytes may too;
	// what must hold is that the same snapshot always encodes the same way.
	c, _ := build(shuffled).Snapshot()
	d, _ := build(shuffled).Snapshot()
	if !bytes.Equal(c, d) {
		t.Fatal("snapshot encoding is not stable")
	}
}

func TestReplicasApplyingTheSameLogConverge(t *testing.T) {
	// The property the whole system exists to provide, checked at the state
	// machine level: same entries, same order, byte-identical result.
	log := []raft.Entry{
		entry(1, Put("a", []byte("1"))),
		entry(2, Put("b", []byte("2"))),
		{Index: 3, Type: raft.EntryNoOp},
		entry(4, CompareAndSwap("a", []byte("1"), []byte("3"), true)),
		entry(5, Delete("b")),
		entry(6, CompareAndSwap("c", nil, []byte("4"), false)),
		entry(7, CompareAndSwap("c", []byte("wrong"), []byte("5"), true)),
	}

	var snaps [][]byte
	for replica := 0; replica < 3; replica++ {
		kv := NewKV()
		for _, e := range log {
			kv.Apply(e)
		}
		s, err := kv.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		snaps = append(snaps, s)
	}
	for i := 1; i < len(snaps); i++ {
		if !bytes.Equal(snaps[0], snaps[i]) {
			t.Fatalf("replica %d diverged from replica 0", i)
		}
	}
}

func TestRestoreIsAllOrNothing(t *testing.T) {
	kv := NewKV()
	kv.Apply(entry(1, Put("keep", []byte("me"))))

	good, _ := kv.Snapshot()
	for cut := 1; cut < len(good); cut++ {
		if err := kv.Restore(good[:cut]); err == nil {
			continue // a shorter prefix can legitimately be a valid smaller snapshot
		}
		// A failed restore must leave the previous contents intact.
		if v, ok := kv.Get("keep"); !ok || string(v.Data) != "me" {
			t.Fatalf("a failed restore at %d bytes damaged the state machine", cut)
		}
	}
}

func TestRestoreRejectsAnImplausibleCount(t *testing.T) {
	kv := NewKV()
	if err := kv.Restore([]byte{0x01, 0xFF, 0xFF, 0xFF, 0x7F}); err == nil {
		t.Fatal("a snapshot claiming a huge key count was accepted")
	}
	if err := kv.Restore(nil); err == nil {
		t.Fatal("an empty snapshot was accepted")
	}
}

func TestEmptySnapshotRestores(t *testing.T) {
	empty, err := NewKV().Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	kv := NewKV()
	kv.Apply(entry(1, Put("k", []byte("v"))))
	if err := kv.Restore(empty); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if kv.Len() != 0 || kv.Revision() != 0 {
		t.Fatalf("restoring an empty snapshot left len=%d revision=%d", kv.Len(), kv.Revision())
	}
}

func TestConcurrentReadsDuringApply(t *testing.T) {
	// Entries arrive from one goroutine, but HTTP handlers read while that is
	// happening. Run under -race, this is the test that says the lock is
	// actually doing its job.
	kv := NewKV()
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			kv.Apply(entry(uint64(i+1), Put(fmt.Sprintf("key-%d", i%10), []byte("v"))))
		}
	}()

	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				kv.Get(fmt.Sprintf("key-%d", i%10))
				kv.Len()
				kv.Revision()
				if i%50 == 0 {
					kv.Keys()
					if _, err := kv.Snapshot(); err != nil {
						t.Errorf("Snapshot: %v", err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()

	if kv.Len() != 10 {
		t.Fatalf("len = %d, want 10", kv.Len())
	}
}
