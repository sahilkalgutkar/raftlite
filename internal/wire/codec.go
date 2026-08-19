// Package wire is raftlite's binary encoding: the format nodes speak to each
// other over TCP and the format the write-ahead log stores on disk.
//
// I wrote the codec by hand rather than reaching for protobuf or gob. The
// consensus protocol is a closed set of messages that I control end to end, so
// a code generator buys very little, and having the byte layout be something I
// can read and reason about is worth a lot when a follower rejects a frame at
// three in the morning. It also means one dependency-free package covers both
// the network and the log.
package wire

import (
	"encoding/binary"
	"errors"
	"math"
)

var (
	// ErrTruncated means the buffer ended in the middle of a value. It is the
	// expected outcome of reading a partially written record at the tail of a
	// log that a crash cut short.
	ErrTruncated = errors.New("wire: buffer ended mid-value")
	// ErrOverflow means a varint claimed a length no sane message would have.
	ErrOverflow = errors.New("wire: length is implausibly large")
)

// maxByteSlice caps any single length-prefixed field. Without it, a corrupt
// length header turns into an enormous allocation, which is a trivially
// remote-triggerable way to kill a node.
const maxByteSlice = 64 << 20 // 64 MiB

// Encoder builds a byte buffer. It never fails: encoding is total, and every
// error in this package belongs to the decoding side.
type Encoder struct {
	buf []byte
}

// NewEncoder returns an encoder with room for size bytes already reserved.
func NewEncoder(size int) *Encoder { return &Encoder{buf: make([]byte, 0, size)} }

// Uvarint appends a variable-length unsigned integer. Most of the numbers in
// this protocol are small indices and terms, so varints cut a heartbeat down
// to a handful of bytes.
func (e *Encoder) Uvarint(v uint64) { e.buf = binary.AppendUvarint(e.buf, v) }

// Byte appends a single byte.
func (e *Encoder) Byte(b byte) { e.buf = append(e.buf, b) }

// Bool appends a boolean as one byte.
func (e *Encoder) Bool(b bool) {
	if b {
		e.buf = append(e.buf, 1)
		return
	}
	e.buf = append(e.buf, 0)
}

// Bytes appends a length-prefixed byte slice.
func (e *Encoder) Bytes(b []byte) {
	e.Uvarint(uint64(len(b)))
	e.buf = append(e.buf, b...)
}

// Text appends a length-prefixed string.
func (e *Encoder) Text(s string) {
	e.Uvarint(uint64(len(s)))
	e.buf = append(e.buf, s...)
}

// Result returns the encoded bytes.
func (e *Encoder) Result() []byte { return e.buf }

// Len is how many bytes have been written so far.
func (e *Encoder) Len() int { return len(e.buf) }

// Decoder reads values back out of a buffer.
//
// It uses sticky errors: the first failure is recorded and every later call is
// a no-op returning a zero value. That keeps the decode functions readable as
// a straight list of fields, with one error check at the end, instead of an
// `if err != nil` after every single field.
type Decoder struct {
	buf []byte
	err error
}

// NewDecoder wraps a buffer for reading.
func NewDecoder(b []byte) *Decoder { return &Decoder{buf: b} }

// Err returns the first error hit, if any.
func (d *Decoder) Err() error { return d.err }

// Remaining returns the bytes not yet consumed.
func (d *Decoder) Remaining() []byte { return d.buf }

// Done reports whether the buffer is fully consumed and error-free.
func (d *Decoder) Done() bool { return d.err == nil && len(d.buf) == 0 }

func (d *Decoder) fail(err error) {
	if d.err == nil {
		d.err = err
	}
}

// Uvarint reads a variable-length unsigned integer.
func (d *Decoder) Uvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.buf)
	switch {
	case n == 0:
		d.fail(ErrTruncated)
		return 0
	case n < 0:
		d.fail(ErrOverflow)
		return 0
	}
	d.buf = d.buf[n:]
	return v
}

// Byte reads a single byte.
func (d *Decoder) Byte() byte {
	if d.err != nil {
		return 0
	}
	if len(d.buf) < 1 {
		d.fail(ErrTruncated)
		return 0
	}
	b := d.buf[0]
	d.buf = d.buf[1:]
	return b
}

// Bool reads a boolean written by Encoder.Bool.
func (d *Decoder) Bool() bool { return d.Byte() == 1 }

// Bytes reads a length-prefixed byte slice. The result is a copy, so a decoded
// message never aliases the network or disk buffer it came from.
func (d *Decoder) Bytes() []byte {
	n := d.Uvarint()
	if d.err != nil {
		return nil
	}
	if n > maxByteSlice {
		d.fail(ErrOverflow)
		return nil
	}
	if uint64(len(d.buf)) < n {
		d.fail(ErrTruncated)
		return nil
	}
	if n == 0 {
		d.buf = d.buf[0:]
		return nil
	}
	out := make([]byte, n)
	copy(out, d.buf[:n])
	d.buf = d.buf[n:]
	return out
}

// Text reads a length-prefixed string. It is not called String because that
// signature would make Decoder look like a fmt.Stringer to every linter and
// every caller reading it.
func (d *Decoder) Text() string {
	b := d.Bytes()
	if b == nil {
		return ""
	}
	return string(b)
}

// Count reads a collection length, rejecting anything that could not possibly
// fit in the remaining buffer. One byte of corruption in a length header would
// otherwise ask for a slice of four billion elements before we ever discover
// the data is not there.
func (d *Decoder) Count(minBytesPerItem int) int {
	n := d.Uvarint()
	if d.err != nil {
		return 0
	}
	if n > math.MaxInt32 || (minBytesPerItem > 0 && n > uint64(len(d.buf)/minBytesPerItem)+1) {
		d.fail(ErrOverflow)
		return 0
	}
	return int(n)
}
