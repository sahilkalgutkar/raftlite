package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	payloads := [][]byte{nil, []byte("x"), bytes.Repeat([]byte("abc"), 1000)}

	var buf bytes.Buffer
	for _, p := range payloads {
		if err := WriteFrame(&buf, p); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
	}
	// All three frames share one stream: the length prefix is what lets the
	// reader split them apart again.
	for i, want := range payloads {
		got, err := ReadFrame(&buf, 0)
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("frame %d = %q, want %q", i, got, want)
		}
	}
	if _, err := ReadFrame(&buf, 0); !errors.Is(err, io.EOF) {
		t.Fatalf("end of stream = %v, want io.EOF", err)
	}
}

func TestReadFrameRejectsCorruption(t *testing.T) {
	build := func() []byte { return AppendFrame(nil, []byte("hello raftlite")) }

	t.Run("flipped payload byte", func(t *testing.T) {
		f := build()
		f[headerSize+2] ^= 0xFF
		if _, err := ReadFrame(bytes.NewReader(f), 0); !errors.Is(err, ErrChecksum) {
			t.Fatalf("err = %v, want ErrChecksum", err)
		}
	})

	t.Run("bad magic", func(t *testing.T) {
		f := build()
		f[0] = 0
		if _, err := ReadFrame(bytes.NewReader(f), 0); !errors.Is(err, ErrBadMagic) {
			t.Fatalf("err = %v, want ErrBadMagic", err)
		}
	})

	t.Run("unsupported version", func(t *testing.T) {
		f := build()
		f[2] = 99
		if _, err := ReadFrame(bytes.NewReader(f), 0); !errors.Is(err, ErrBadVersion) {
			t.Fatalf("err = %v, want ErrBadVersion", err)
		}
	})

	t.Run("oversized length header", func(t *testing.T) {
		f := build()
		binary.BigEndian.PutUint32(f[4:8], 1<<30)
		if _, err := ReadFrame(bytes.NewReader(f), 1024); !errors.Is(err, ErrFrameTooLarge) {
			t.Fatalf("err = %v, want ErrFrameTooLarge", err)
		}
	})

	t.Run("truncated header", func(t *testing.T) {
		f := build()
		if _, err := ReadFrame(bytes.NewReader(f[:5]), 0); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("err = %v, want io.ErrUnexpectedEOF", err)
		}
	})

	t.Run("truncated payload", func(t *testing.T) {
		f := build()
		if _, err := ReadFrame(bytes.NewReader(f[:len(f)-3]), 0); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("err = %v, want io.ErrUnexpectedEOF", err)
		}
	})
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("socket closed") }

func TestWriteFrameSurfacesWriteErrors(t *testing.T) {
	if err := WriteFrame(failWriter{}, []byte("x")); err == nil {
		t.Fatal("a failing writer produced no error")
	}
}

func TestChecksumIsStable(t *testing.T) {
	if Checksum([]byte("raftlite")) != Checksum([]byte("raftlite")) {
		t.Fatal("checksum is not deterministic")
	}
	if Checksum([]byte("a")) == Checksum([]byte("b")) {
		t.Fatal("checksum collides on trivially different inputs")
	}
}
