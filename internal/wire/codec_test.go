package wire

import (
	"errors"
	"testing"
)

func TestPrimitiveRoundTrip(t *testing.T) {
	e := NewEncoder(0)
	e.Uvarint(0)
	e.Uvarint(1)
	e.Uvarint(1 << 62)
	e.Byte(0xAB)
	e.Bool(true)
	e.Bool(false)
	e.Bytes([]byte("payload"))
	e.Bytes(nil)
	e.String("hello")
	e.String("")
	if e.Len() == 0 {
		t.Fatal("encoder wrote nothing")
	}

	d := NewDecoder(e.Result())
	if got := d.Uvarint(); got != 0 {
		t.Fatalf("uvarint = %d", got)
	}
	if got := d.Uvarint(); got != 1 {
		t.Fatalf("uvarint = %d", got)
	}
	if got := d.Uvarint(); got != 1<<62 {
		t.Fatalf("uvarint = %d", got)
	}
	if got := d.Byte(); got != 0xAB {
		t.Fatalf("byte = %#x", got)
	}
	if !d.Bool() || d.Bool() {
		t.Fatal("bools did not round trip")
	}
	if got := string(d.Bytes()); got != "payload" {
		t.Fatalf("bytes = %q", got)
	}
	if got := d.Bytes(); got != nil {
		t.Fatalf("empty bytes decoded to %v", got)
	}
	if got := d.String(); got != "hello" {
		t.Fatalf("string = %q", got)
	}
	if got := d.String(); got != "" {
		t.Fatalf("empty string = %q", got)
	}
	if !d.Done() {
		t.Fatalf("decoder has %d bytes left over: %v", len(d.Remaining()), d.Err())
	}
}

func TestDecodedBytesAreACopy(t *testing.T) {
	e := NewEncoder(0)
	e.Bytes([]byte("mutable"))
	buf := e.Result()

	d := NewDecoder(buf)
	got := d.Bytes()

	// Scribble over the source buffer the way a reused network read buffer
	// would. The decoded value must not change.
	for i := range buf {
		buf[i] = 0
	}
	if string(got) != "mutable" {
		t.Fatalf("decoded value aliased its source buffer: %q", got)
	}
}

func TestDecoderErrorsAreSticky(t *testing.T) {
	d := NewDecoder(nil)
	if got := d.Uvarint(); got != 0 {
		t.Fatalf("uvarint on empty buffer = %d", got)
	}
	if !errors.Is(d.Err(), ErrTruncated) {
		t.Fatalf("err = %v, want ErrTruncated", d.Err())
	}
	// Every subsequent call is a no-op and the first error is preserved.
	d.Byte()
	d.Bytes()
	d.String()
	d.Bool()
	d.Count(1)
	if !errors.Is(d.Err(), ErrTruncated) {
		t.Fatalf("err = %v, want the original ErrTruncated", d.Err())
	}
	if d.Done() {
		t.Fatal("a failed decoder reported Done")
	}
}

func TestDecoderRejectsImplausibleLengths(t *testing.T) {
	t.Run("varint overflow", func(t *testing.T) {
		// Ten 0x80 bytes is a varint that never terminates in 64 bits.
		bad := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02}
		d := NewDecoder(bad)
		d.Uvarint()
		if !errors.Is(d.Err(), ErrOverflow) {
			t.Fatalf("err = %v, want ErrOverflow", d.Err())
		}
	})

	t.Run("byte slice longer than the buffer", func(t *testing.T) {
		e := NewEncoder(0)
		e.Uvarint(1000) // claims 1000 bytes, supplies none
		d := NewDecoder(e.Result())
		if got := d.Bytes(); got != nil {
			t.Fatalf("decoded %d bytes out of thin air", len(got))
		}
		if !errors.Is(d.Err(), ErrTruncated) {
			t.Fatalf("err = %v", d.Err())
		}
	})

	t.Run("byte slice beyond the hard cap", func(t *testing.T) {
		e := NewEncoder(0)
		e.Uvarint(maxByteSlice + 1)
		d := NewDecoder(e.Result())
		d.Bytes()
		if !errors.Is(d.Err(), ErrOverflow) {
			t.Fatalf("err = %v, want ErrOverflow", d.Err())
		}
	})

	t.Run("collection count that cannot fit", func(t *testing.T) {
		e := NewEncoder(0)
		e.Uvarint(1 << 40) // a billion entries in a ten byte buffer
		d := NewDecoder(e.Result())
		d.Count(4)
		if !errors.Is(d.Err(), ErrOverflow) {
			t.Fatalf("err = %v, want ErrOverflow", d.Err())
		}
	})
}
