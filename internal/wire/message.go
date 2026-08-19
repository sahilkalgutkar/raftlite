package wire

import "github.com/sahilkalgutkar/raftlite/internal/raft"

// Field order is fixed and shared by both ends. Adding a field means appending
// it here and bumping the frame version -- decoders ignore trailing bytes, so
// a newer sender stays readable by an older receiver for anything additive.

// EncodeEntry appends one log entry.
func EncodeEntry(e *Encoder, ent raft.Entry) {
	e.Uvarint(ent.Index)
	e.Uvarint(ent.Term)
	e.Byte(byte(ent.Type))
	e.Bytes(ent.Data)
}

// DecodeEntry reads one log entry.
func DecodeEntry(d *Decoder) raft.Entry {
	return raft.Entry{
		Index: d.Uvarint(),
		Term:  d.Uvarint(),
		Type:  raft.EntryType(d.Byte()),
		Data:  d.Bytes(),
	}
}

// minEntryBytes is the smallest an entry can possibly encode to: three
// single-byte varints plus a zero-length payload. It lets the decoder reject a
// corrupt entry count before allocating for it.
const minEntryBytes = 4

// EncodeEntries appends a length-prefixed run of entries.
func EncodeEntries(e *Encoder, ents []raft.Entry) {
	e.Uvarint(uint64(len(ents)))
	for _, ent := range ents {
		EncodeEntry(e, ent)
	}
}

// DecodeEntries reads a run of entries written by EncodeEntries.
func DecodeEntries(d *Decoder) []raft.Entry {
	n := d.Count(minEntryBytes)
	if n == 0 {
		return nil
	}
	out := make([]raft.Entry, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, DecodeEntry(d))
		if d.Err() != nil {
			return nil
		}
	}
	return out
}

// Message flag bits, packed into a single byte rather than spending a byte per
// boolean.
const (
	flagPreVote = 1 << iota
	flagReject
)

// EncodeMessage serialises a protocol message.
func EncodeMessage(m raft.Message) []byte {
	e := NewEncoder(64 + 32*len(m.Entries))
	e.Byte(byte(m.Type))
	e.Uvarint(uint64(m.From))
	e.Uvarint(uint64(m.To))
	e.Uvarint(m.Term)

	var flags byte
	if m.PreVote {
		flags |= flagPreVote
	}
	if m.Reject {
		flags |= flagReject
	}
	e.Byte(flags)

	e.Uvarint(m.LastLogIndex)
	e.Uvarint(m.LastLogTerm)
	e.Uvarint(m.Commit)
	e.Uvarint(m.PrevLogIndex)
	e.Uvarint(m.PrevLogTerm)
	e.Uvarint(m.MatchIndex)
	e.Uvarint(m.ConflictIndex)
	e.Uvarint(m.ConflictTerm)
	EncodeEntries(e, m.Entries)
	return e.Result()
}

// DecodeMessage parses a message. Trailing bytes are ignored so a node running
// a newer build can talk to an older one for purely additive changes.
func DecodeMessage(b []byte) (raft.Message, error) {
	d := NewDecoder(b)

	m := raft.Message{Type: raft.MessageType(d.Byte())}
	m.From = raft.NodeID(d.Uvarint())
	m.To = raft.NodeID(d.Uvarint())
	m.Term = d.Uvarint()

	flags := d.Byte()
	m.PreVote = flags&flagPreVote != 0
	m.Reject = flags&flagReject != 0

	m.LastLogIndex = d.Uvarint()
	m.LastLogTerm = d.Uvarint()
	m.Commit = d.Uvarint()
	m.PrevLogIndex = d.Uvarint()
	m.PrevLogTerm = d.Uvarint()
	m.MatchIndex = d.Uvarint()
	m.ConflictIndex = d.Uvarint()
	m.ConflictTerm = d.Uvarint()
	m.Entries = DecodeEntries(d)

	if err := d.Err(); err != nil {
		return raft.Message{}, err
	}
	return m, nil
}

// EncodeHardState serialises the three fields that must survive a crash.
func EncodeHardState(e *Encoder, hs raft.HardState) {
	e.Uvarint(hs.Term)
	e.Uvarint(uint64(hs.Vote))
	e.Uvarint(hs.Commit)
}

// DecodeHardState reads a hard state.
func DecodeHardState(d *Decoder) raft.HardState {
	return raft.HardState{
		Term:   d.Uvarint(),
		Vote:   raft.NodeID(d.Uvarint()),
		Commit: d.Uvarint(),
	}
}
