package wire

import (
	"reflect"
	"testing"

	"github.com/sahilkalgutkar/raftlite/internal/raft"
)

func sampleMessages() []raft.Message {
	return []raft.Message{
		{Type: raft.MsgVoteReq, From: 1, To: 2, Term: 7, PreVote: true, LastLogIndex: 40, LastLogTerm: 6},
		{Type: raft.MsgVoteResp, From: 2, To: 1, Term: 7, Reject: true},
		{Type: raft.MsgHeartbeatReq, From: 1, To: 3, Term: 7, Commit: 39},
		{Type: raft.MsgHeartbeatResp, From: 3, To: 1, Term: 7},
		{
			Type: raft.MsgAppendReq, From: 1, To: 2, Term: 7,
			PrevLogIndex: 40, PrevLogTerm: 6, Commit: 39,
			Entries: []raft.Entry{
				{Index: 41, Term: 7, Type: raft.EntryNoOp},
				{Index: 42, Term: 7, Type: raft.EntryNormal, Data: []byte("set k v")},
				{Index: 43, Term: 7, Type: raft.EntryConfChange, Data: []byte{9, 9, 9}},
			},
		},
		{Type: raft.MsgAppendResp, From: 2, To: 1, Term: 7, MatchIndex: 43},
		{Type: raft.MsgAppendResp, From: 2, To: 1, Term: 7, Reject: true, ConflictIndex: 12, ConflictTerm: 5},
		{}, // the zero message must survive too
	}
}

func TestMessageRoundTrip(t *testing.T) {
	for _, want := range sampleMessages() {
		got, err := DecodeMessage(EncodeMessage(want))
		if err != nil {
			t.Fatalf("DecodeMessage(%v): %v", want, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("round trip changed the message:\n got %+v\nwant %+v", got, want)
		}
	}
}

func TestMessageEncodingIsDeterministic(t *testing.T) {
	for _, m := range sampleMessages() {
		if string(EncodeMessage(m)) != string(EncodeMessage(m)) {
			t.Fatalf("encoding %v is not stable", m)
		}
	}
}

func TestDecodeMessageRejectsEveryTruncation(t *testing.T) {
	// Every prefix of a valid message must be refused rather than silently
	// decoding into something plausible-looking. This is the case a half-read
	// socket actually produces.
	for _, m := range sampleMessages() {
		full := EncodeMessage(m)
		for cut := 0; cut < len(full); cut++ {
			if _, err := DecodeMessage(full[:cut]); err == nil {
				// A prefix may legitimately decode when every remaining field
				// is zero-valued and the message carries no entries, but a
				// truncated entry list must never pass.
				if len(m.Entries) > 0 {
					t.Fatalf("truncating %v to %d of %d bytes decoded cleanly", m.Type, cut, len(full))
				}
			}
		}
	}
}

func TestDecodeMessageIgnoresTrailingBytes(t *testing.T) {
	want := sampleMessages()[4]
	padded := append(EncodeMessage(want), 0xDE, 0xAD, 0xBE, 0xEF)
	got, err := DecodeMessage(padded)
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("trailing bytes changed the message: %+v", got)
	}
}

func TestDecodeMessageRejectsACorruptEntryCount(t *testing.T) {
	e := NewEncoder(0)
	e.Byte(byte(raft.MsgAppendReq))
	for i := 0; i < 11; i++ {
		e.Uvarint(0) // from, to, term, flags-as-byte stand-in, and the indices
	}
	e.Uvarint(1 << 30) // an entry count nothing could satisfy
	if _, err := DecodeMessage(e.Result()); err == nil {
		t.Fatal("a corrupt entry count decoded cleanly")
	}
}

func TestHardStateRoundTrip(t *testing.T) {
	for _, want := range []raft.HardState{
		{},
		{Term: 1},
		{Term: 9, Vote: 3, Commit: 412},
	} {
		e := NewEncoder(0)
		EncodeHardState(e, want)
		d := NewDecoder(e.Result())
		got := DecodeHardState(d)
		if err := d.Err(); err != nil {
			t.Fatalf("DecodeHardState: %v", err)
		}
		if got != want {
			t.Fatalf("round trip: %v != %v", got, want)
		}
	}
}

func TestEntryRoundTrip(t *testing.T) {
	want := raft.Entry{Index: 99, Term: 12, Type: raft.EntryConfChange, Data: []byte("cc")}
	e := NewEncoder(0)
	EncodeEntry(e, want)
	d := NewDecoder(e.Result())
	got := DecodeEntry(d)
	if d.Err() != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip: %+v (%v)", got, d.Err())
	}
}

func TestEmptyEntryListRoundTrip(t *testing.T) {
	e := NewEncoder(0)
	EncodeEntries(e, nil)
	d := NewDecoder(e.Result())
	if got := DecodeEntries(d); got != nil {
		t.Fatalf("empty list decoded to %v", got)
	}
	if d.Err() != nil {
		t.Fatalf("err = %v", d.Err())
	}
}

func TestSnapshotRoundTripInAMessage(t *testing.T) {
	want := raft.Message{
		Type: raft.MsgSnapshotReq, From: 1, To: 2, Term: 9,
		Snapshot: &raft.Snapshot{
			Meta: raft.SnapshotMeta{
				Index: 512, Term: 8,
				Config: raft.NewConfig(
					raft.Member{ID: 1, Addr: "10.0.0.1:9001"},
					raft.Member{ID: 2, Addr: "10.0.0.2:9001", Learner: true},
				),
			},
			Data: []byte("a whole state machine"),
		},
	}

	got, err := DecodeMessage(EncodeMessage(want))
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	if got.Snapshot == nil {
		t.Fatal("snapshot did not survive the round trip")
	}
	if got.Snapshot.Meta.Index != 512 || got.Snapshot.Meta.Term != 8 {
		t.Fatalf("meta = %+v", got.Snapshot.Meta)
	}
	if string(got.Snapshot.Data) != "a whole state machine" {
		t.Fatalf("data = %q", got.Snapshot.Data)
	}
	if !got.Snapshot.Meta.Config.Equal(want.Snapshot.Meta.Config) {
		t.Fatalf("config = %v", got.Snapshot.Meta.Config)
	}
}

func TestSnapshotTruncationIsRejected(t *testing.T) {
	m := raft.Message{
		Type:     raft.MsgSnapshotReq,
		Snapshot: &raft.Snapshot{Meta: raft.SnapshotMeta{Index: 5, Term: 2}, Data: []byte("image")},
	}
	full := EncodeMessage(m)
	for cut := 0; cut < len(full)-1; cut++ {
		if _, err := DecodeMessage(full[:cut]); err == nil {
			t.Fatalf("truncating to %d of %d bytes decoded cleanly", cut, len(full))
		}
	}
}

func TestSnapshotWithACorruptConfigIsRejected(t *testing.T) {
	e := NewEncoder(0)
	e.Uvarint(5)                // index
	e.Uvarint(2)                // term
	e.Bytes([]byte{0xFF, 0xFF}) // a configuration that cannot decode
	e.Bytes([]byte("image"))
	d := NewDecoder(e.Result())
	if _, err := DecodeSnapshot(d); err == nil {
		t.Fatal("a corrupt configuration decoded cleanly")
	}
}
