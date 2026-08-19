package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// Frame layout, 12 bytes of header followed by the payload:
//
//	0..1   magic     "RL"
//	2      version
//	3      reserved (zero)
//	4..7   payload length, big endian
//	8..11  CRC32 (Castagnoli) of the payload
//
// The length prefix is what makes a TCP stream splittable back into messages,
// and the checksum is what makes a torn write at the end of the log
// distinguishable from a message that really did arrive intact.
const (
	headerSize    = 12
	frameMagic    = 0x524C // "RL"
	frameVersion  = 1
	DefaultMaxLen = 64 << 20
)

var (
	// ErrBadMagic means the stream is not carrying raftlite frames at all.
	ErrBadMagic = errors.New("wire: bad frame magic")
	// ErrBadVersion means the peer speaks a frame version we do not.
	ErrBadVersion = errors.New("wire: unsupported frame version")
	// ErrChecksum means the payload did not survive its trip intact.
	ErrChecksum = errors.New("wire: payload checksum mismatch")
	// ErrFrameTooLarge means the header claims more bytes than we will accept.
	ErrFrameTooLarge = errors.New("wire: frame exceeds the size limit")
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Checksum returns the CRC used by frames and by log records.
func Checksum(payload []byte) uint32 { return crc32.Checksum(payload, crcTable) }

// AppendFrame appends a framed payload to dst, which lets a caller build up
// several frames in one buffer and issue a single write.
func AppendFrame(dst []byte, payload []byte) []byte {
	var header [headerSize]byte
	binary.BigEndian.PutUint16(header[0:2], frameMagic)
	header[2] = frameVersion
	header[3] = 0
	binary.BigEndian.PutUint32(header[4:8], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[8:12], Checksum(payload))

	dst = append(dst, header[:]...)
	return append(dst, payload...)
}

// WriteFrame writes one framed payload.
func WriteFrame(w io.Writer, payload []byte) error {
	if _, err := w.Write(AppendFrame(nil, payload)); err != nil {
		return fmt.Errorf("wire: write frame: %w", err)
	}
	return nil
}

// ReadFrame reads one framed payload, verifying magic, version, size and
// checksum before returning it. A clean end of stream comes back as io.EOF; a
// stream that stops midway through a frame comes back as io.ErrUnexpectedEOF,
// and the caller can tell the difference between a peer hanging up politely
// and one dying mid-message.
func ReadFrame(r io.Reader, maxLen uint32) ([]byte, error) {
	if maxLen == 0 {
		maxLen = DefaultMaxLen
	}

	var header [headerSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	if got := binary.BigEndian.Uint16(header[0:2]); got != frameMagic {
		return nil, fmt.Errorf("%w: got %#04x", ErrBadMagic, got)
	}
	if header[2] != frameVersion {
		return nil, fmt.Errorf("%w: got %d, support %d", ErrBadVersion, header[2], frameVersion)
	}

	length := binary.BigEndian.Uint32(header[4:8])
	if length > maxLen {
		return nil, fmt.Errorf("%w: %d bytes > %d", ErrFrameTooLarge, length, maxLen)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	if want := binary.BigEndian.Uint32(header[8:12]); Checksum(payload) != want {
		return nil, ErrChecksum
	}
	return payload, nil
}
