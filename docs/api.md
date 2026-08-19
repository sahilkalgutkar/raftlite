# HTTP API

Every node serves the same API. Writes and linearizable reads are answered by
the leader; a follower that receives one replies `307 Temporary Redirect` with
the leader's address rather than proxying, so the client learns where the work
is happening.

It is `307` specifically, not `302`, because the method and body have to survive
the redirect — otherwise every `PUT` arrives at the leader as a `GET`.

## Keys

### `PUT /kv/{key}`

The request body is the value, byte for byte. Values are capped at 1 MiB: every
write is replicated to and held in memory by every node, so an unbounded value
is an unbounded cluster-wide commitment any client could make.

| Query | Effect |
| --- | --- |
| *(none)* | Unconditional write |
| `?prev=<value>` | Write only if the key currently holds `<value>` |
| `?prev_exists=false` | Write only if the key does not exist |

```bash
curl -X PUT --data 'hello' http://127.0.0.1:8001/kv/greeting
# {"key":"greeting","revision":1,"existed":false}

curl -X PUT --data 'held-by-a' 'http://127.0.0.1:8001/kv/lock?prev_exists=false'
# {"key":"lock","revision":2,"existed":false,"swapped":true}
```

A failed comparison returns `409` along with the value that is actually stored,
because the caller needs it to retry and a separate fetch could read something
newer:

```json
{"error":"compare-and-swap failed","key":"lock","current":"held-by-a","existed":true,"revision":2}
```

### `GET /kv/{key}`

Returns the raw value bytes, so what you stored is exactly what you get back and
binary payloads need no encoding.

| Query | Effect |
| --- | --- |
| *(none)* or `?consistency=linearizable` | Confirms leadership first; reflects every write committed before the call |
| `?consistency=stale` | Answered from local state by any node; may be behind |

Response headers: `X-Raft-Revision`, `X-Raft-Consistency`.

`404` if the key does not exist.

### `DELETE /kv/{key}`

`200` with the revision, or `404` if the key was not there.

## Cluster

### `GET /status`

```json
{
  "id": 1, "role": "leader", "term": 3, "leader": 1,
  "leader_url": "127.0.0.1:8001",
  "commit_index": 847, "applied_index": 847,
  "last_index": 847, "snapshot_index": 500, "keys": 120,
  "members": [{"id": 1, "addr": "...", "client_addr": "...", "learner": false}],
  "followers": [{"id": 2, "match_index": 847, "next_index": 848, "recently_active": true}]
}
```

`followers` is only populated on the leader, and is the first thing worth
looking at when a cluster is misbehaving: it shows exactly who is behind and by
how much.

### `GET /members`, `POST /members`, `POST /members/{id}/promote`, `DELETE /members/{id}`

```bash
curl -X POST -d '{"id":4,"addr":"10.0.0.4:9001","client_addr":"10.0.0.4:8001","voting":false}' \
  http://127.0.0.1:8001/members
curl -X POST http://127.0.0.1:8001/members/4/promote
curl -X DELETE http://127.0.0.1:8001/members/4
```

Each returns the resulting member list. A change is refused with `409` while a
previous one is still uncommitted — configurations only stay safe if they change
one server at a time.

### `GET /healthz`

`200` while the node is running, `503` once it has stopped.

### `GET /metrics`

Prometheus text format. Served by every node regardless of role.

| Metric | Type | Meaning |
| --- | --- | --- |
| `raftlite_is_leader` | gauge | 1 on the leader |
| `raftlite_term` | gauge | Current term |
| `raftlite_commit_index`, `raftlite_applied_index`, `raftlite_last_index`, `raftlite_snapshot_index` | gauge | Log positions |
| `raftlite_keys`, `raftlite_members`, `raftlite_voters` | gauge | State machine and configuration size |
| `raftlite_followers_behind` | gauge | Replicas trailing the leader's log |
| `raftlite_proposals_total`, `raftlite_proposals_failed_total` | counter | Writes submitted and refused |
| `raftlite_linearizable_reads_total`, `..._failed_total` | counter | Reads served and refused |
| `raftlite_entries_applied_total` | counter | Entries handed to the state machine |
| `raftlite_leader_changes_total` | counter | Leadership changes this node observed |
| `raftlite_snapshots_taken_total`, `raftlite_snapshots_installed_total` | counter | Compaction and catch-up |
| `raftlite_config_changes_total` | counter | Membership changes applied |
| `raftlite_transport_messages_{sent,received,dropped}_total` | counter | Peer traffic |

## Status codes

| Code | Meaning |
| --- | --- |
| `200` | Done |
| `307` | Not the leader; the `Location` and `X-Raft-Leader` headers name it |
| `400` | Malformed request, or a membership change that could never apply |
| `404` | No such key |
| `409` | Compare-and-swap failed, or a membership change is already in flight |
| `503` | No leader, or this node has stopped |
| `504` | The request outlived `--request-timeout` waiting on consensus |
