# Running a cluster

## Sizing

Three nodes tolerate one failure; five tolerate two. Even sizes buy nothing — a
four node cluster needs three to agree, the same as five, while having more
machines that can fail.

Learners do not count toward a quorum, so a read replica or a member catching up
never makes writes harder.

## Adding a server

Start it with `--join` pointed at any existing member:

```bash
raftlited --id 4 --raft-addr 10.0.0.4:9001 --http-addr 10.0.0.4:8001 \
  --data-dir /var/lib/raftlite --join 10.0.0.1:8001
```

It asks that node for the membership, starts up, and announces itself as a
**learner**: it replicates the log without being counted in any quorum, so a
cold replica cannot stall writes while it catches up.

Watch it converge:

```bash
raftctl --endpoints 10.0.0.1:8001 status
```

When its `match_index` is close to the leader's `last_index`, promote it:

```bash
raftctl --endpoints 10.0.0.1:8001 member-promote 4
```

Promoting too early is not unsafe, only slow: writes then wait for a replica
that is still streaming history.

## Removing a server

```bash
raftctl --endpoints 10.0.0.1:8001 member-remove 4
```

Remove it from the configuration **before** shutting the process down. A server
that is switched off while still a member is a member that never answers, and
the cluster keeps counting it toward the quorum it needs.

Removing the leader is allowed: it serves until that entry commits, then steps
down and the rest elect a replacement.

## Replacing a server

Two steps, not one — configurations only stay safe while they differ by a single
server:

```bash
raftctl --endpoints ... member-add 5 10.0.0.5:9001 10.0.0.5:8001
# wait for it to catch up, then promote
raftctl --endpoints ... member-promote 5
raftctl --endpoints ... member-remove 4
```

## Rolling restarts

Restart one node at a time and wait for it to rejoin before taking the next
down. `raftctl status` shows every endpoint separately, which is what makes
"has it actually caught up?" answerable rather than assumed.

The cluster stays writable throughout as long as a quorum is up, which for
three nodes means never taking down two at once.

## Backups

A node's data directory is the whole of its state. With the process stopped, a
copy of the directory is a complete backup. On a running node, copy the newest
file from `snapshots/` — it is written atomically and is a consistent image at
its index, though it will not include writes since.

To restore, put the directory back and start the server with the same `--id`.

## What to alert on

| Signal | Why |
| --- | --- |
| `raftlite_is_leader` summing to something other than 1 | No leader, or a partition |
| `raftlite_leader_changes_total` rising steadily | Elections are churning; usually a network or timeout problem |
| `raftlite_followers_behind` above zero for long | A replica is not keeping up, and the cluster's failure tolerance is lower than it looks |
| `raftlite_proposals_failed_total` rising | Writes are being refused, usually because leadership keeps moving |
| `raftlite_transport_messages_dropped_total` rising | A peer is unreachable or too slow to drain its queue |
| A process that exited | A node stops on any error that prevents it from making state durable, and that is deliberate |

## Tuning

`--tick-interval` and `--election-ticks` decide failover time: the default is
one to two seconds. On a lossy or high-latency network, raise `--election-ticks`
before you raise the tick interval — jitter is what stops split votes, and there
is more of it in more ticks.

`--snapshot-threshold` trades log length against snapshot cost. Lower it if
restarts replay for too long; raise it if snapshotting is showing up in write
latency.

`--unsafe-no-fsync` makes writes much faster and the durability guarantee
untrue. It is for benchmarks.
