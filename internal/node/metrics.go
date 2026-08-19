package node

import (
	"github.com/sahilkalgutkar/raftlite/internal/metrics"
	"github.com/sahilkalgutkar/raftlite/internal/raft"
	"github.com/sahilkalgutkar/raftlite/internal/transport"
)

// nodeMetrics is everything a running node exports.
//
// Counters are incremented where the event happens; gauges are read at scrape
// time from whatever already knows the answer. Mirroring the term or the
// commit index into a gauge on every change would create a second source of
// truth that can drift; asking the algorithm when someone scrapes cannot.
type nodeMetrics struct {
	proposals       *metrics.Counter
	proposalsFailed *metrics.Counter
	reads           *metrics.Counter
	readsFailed     *metrics.Counter
	entriesApplied  *metrics.Counter
	leaderChanges   *metrics.Counter
	snapshotsTaken  *metrics.Counter
	snapshotsLoaded *metrics.Counter
	configChanges   *metrics.Counter
}

// statsSource is the optional interface a transport can satisfy to have its
// counters exported alongside the node's.
type statsSource interface {
	Stats() transport.StatsSnapshot
}

func (n *Node) registerMetrics(reg *metrics.Registry) {
	labels := metrics.Labels{"node": n.cfg.ID.String()}

	n.metrics = &nodeMetrics{
		proposals:       reg.Counter("raftlite_proposals_total", "Writes submitted to this node.", labels),
		proposalsFailed: reg.Counter("raftlite_proposals_failed_total", "Writes this node could not commit.", labels),
		reads:           reg.Counter("raftlite_linearizable_reads_total", "Linearizable reads served.", labels),
		readsFailed:     reg.Counter("raftlite_linearizable_reads_failed_total", "Linearizable reads refused or abandoned.", labels),
		entriesApplied:  reg.Counter("raftlite_entries_applied_total", "Log entries handed to the state machine.", labels),
		leaderChanges:   reg.Counter("raftlite_leader_changes_total", "Times this node observed a new leader.", labels),
		snapshotsTaken:  reg.Counter("raftlite_snapshots_taken_total", "Snapshots this node produced.", labels),
		snapshotsLoaded: reg.Counter("raftlite_snapshots_installed_total", "Snapshots this node installed from a leader.", labels),
		configChanges:   reg.Counter("raftlite_config_changes_total", "Membership changes applied.", labels),
	}

	reg.GaugeFunc("raftlite_term", "Current Raft term.", labels,
		func() float64 { return float64(n.Status().Term) })
	reg.GaugeFunc("raftlite_is_leader", "1 when this node is the leader.", labels, func() float64 {
		if n.IsLeader() {
			return 1
		}
		return 0
	})
	reg.GaugeFunc("raftlite_commit_index", "Highest committed log index.", labels,
		func() float64 { return float64(n.Status().Commit) })
	reg.GaugeFunc("raftlite_applied_index", "Highest applied log index.", labels,
		func() float64 { return float64(n.Status().Applied) })
	reg.GaugeFunc("raftlite_last_index", "Highest log index stored.", labels,
		func() float64 { return float64(n.Status().LastIndex) })
	reg.GaugeFunc("raftlite_snapshot_index", "Log index the newest snapshot covers.", labels,
		func() float64 { return float64(n.Status().Snapshot) })
	reg.GaugeFunc("raftlite_keys", "Keys held by the state machine.", labels,
		func() float64 { return float64(n.kv.Len()) })
	reg.GaugeFunc("raftlite_members", "Servers in the current configuration.", labels,
		func() float64 { return float64(len(n.Status().Config.Members)) })
	reg.GaugeFunc("raftlite_voters", "Voting servers in the current configuration.", labels,
		func() float64 { return float64(len(n.Status().Config.Voters())) })

	// A follower that is falling behind is the thing worth alerting on, and
	// only the leader can see it.
	reg.GaugeFunc("raftlite_followers_behind",
		"Followers whose match index trails the leader's log, as seen by this node.", labels,
		func() float64 {
			st := n.Status()
			if st.Role != raft.Leader {
				return 0
			}
			behind := 0
			for id, p := range st.Progress {
				if id != st.ID && p.Match < st.LastIndex {
					behind++
				}
			}
			return float64(behind)
		})

	if src, ok := n.tr.(statsSource); ok {
		reg.CounterFunc("raftlite_transport_messages_sent_total", "Messages queued for peers.", labels,
			func() float64 { return float64(src.Stats().Sent) })
		reg.CounterFunc("raftlite_transport_messages_received_total", "Messages received from peers.", labels,
			func() float64 { return float64(src.Stats().Received) })
		reg.CounterFunc("raftlite_transport_messages_dropped_total",
			"Messages dropped because a peer was unreachable or its queue was full.", labels,
			func() float64 { return float64(src.Stats().Dropped) })
		reg.GaugeFunc("raftlite_transport_connections", "Open inbound peer connections.", labels,
			func() float64 { return float64(src.Stats().Connections) })
	}
}

// observeLeader records leadership changes as they are noticed.
func (n *Node) observeLeader(lead raft.NodeID) {
	if lead == raft.None || lead == n.lastSeenLeader {
		return
	}
	n.lastSeenLeader = lead
	n.metrics.leaderChanges.Inc()
}
