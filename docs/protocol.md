# Wire protocol

Nodes speak a hand-written binary protocol over TCP. The same encoding is used
for records in the write-ahead log, which is why the codec is its own package
rather than living inside the transport.

I wrote it by hand rather than reaching for protobuf. The protocol is a closed
set of messages I control at both ends, so a code generator buys very little,
while being able to read the byte layout directly is worth a lot when a peer
starts rejecting frames.

## Framing

Every message is one frame: a 12-byte header followed by the payload.

```
 0      1      2      3      4                    8                   12
 +------+------+------+------+--------------------+--------------------+
 |   magic     | ver  | rsvd |   payload length   |     CRC32-C        |
 +------+------+------+------+--------------------+--------------------+
 |                          payload                                    |
 +---------------------------------------------------------------------+
```

- **magic** — `0x524C` ("RL"). Catches a stream that is not carrying raftlite
  frames at all.
- **ver** — frame version, currently 1.
- **length** — payload bytes, big endian, capped at 64 MiB by the reader.
- **CRC32-C** — Castagnoli checksum of the payload.

The length prefix is what makes a TCP stream splittable back into messages. The
checksum is what makes a torn write at the end of the log distinguishable from a
record that really did land intact.

A stream that ends cleanly surfaces as `io.EOF`; one that dies mid-frame
surfaces as `io.ErrUnexpectedEOF`. The transport uses the difference to tell a
peer hanging up politely from a peer crashing.

## Message payload

Fields are encoded in a fixed order. Integers are unsigned varints, since terms
and indices are almost always small and a heartbeat compresses to a handful of
bytes. Booleans are packed into a single flags byte.

| Field | Type | Used by |
| --- | --- | --- |
| type | byte | all |
| from, to | varint | all |
| term | varint | all |
| flags | byte | pre-vote, reject |
| lastLogIndex, lastLogTerm | varint | vote requests |
| commit | varint | heartbeats, appends |
| prevLogIndex, prevLogTerm | varint | appends |
| matchIndex | varint | append and snapshot responses |
| conflictIndex, conflictTerm | varint | append rejections |
| readID | varint | heartbeats carrying a linearizable read |
| entries | count + records | appends |
| snapshot | flag + image | install snapshot |

Decoders ignore trailing bytes, so a purely additive change stays readable by an
older build. Adding a field means appending it here and bumping the frame
version if the change is not additive.

There is a test that walks the message struct by reflection and fails if any
field is zero in its fixture or comes back zero after a round trip. That exists
because a field was once added to the algorithm and not to the codec: every
in-process test passed, and the feature failed the moment two machines had to
talk.

## Message types

| Type | Direction | Purpose |
| --- | --- | --- |
| `vote-req` / `vote-resp` | candidate ↔ voter | Elections, in both pre-vote and real rounds |
| `heartbeat-req` / `heartbeat-resp` | leader ↔ follower | Liveness, commit index, and read confirmation |
| `append-req` / `append-resp` | leader ↔ follower | Log replication and repair |
| `snapshot-req` / `snapshot-resp` | leader ↔ follower | Catching up a replica past the retained log |

Heartbeats are a separate type from appends rather than "an append with no
entries" so that a heartbeat never runs the log matching check and can never
truncate a follower's log.

## Log records

The write-ahead log reuses the frame format, with a one-byte record type at the
start of each payload:

| Type | Contents |
| --- | --- |
| 1 | Hard state: term, vote, commit index |
| 2 | One log entry |
| 3 | A truncation: discard everything from this index onward |

Snapshot files are a single frame containing the index, term, cluster
configuration and the state machine image.
