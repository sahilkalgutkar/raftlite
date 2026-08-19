package server

import (
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseFlagsMinimalSingleNode(t *testing.T) {
	cfg, err := ParseFlags([]string{"--id", "1", "--bootstrap"}, io.Discard)
	if err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}
	if cfg.ID != 1 || !cfg.Bootstrap {
		t.Fatalf("cfg = %+v", cfg)
	}
	// A lone bootstrapping server is its own one-node cluster.
	if len(cfg.Peers) != 1 || cfg.Peers[0].ID != 1 {
		t.Fatalf("peers = %v", cfg.Peers)
	}
	if cfg.Peers[0].Addr != cfg.RaftAddr || cfg.Peers[0].ClientAddr != cfg.HTTPAddr {
		t.Fatalf("implied peer = %+v", cfg.Peers[0])
	}
	if cfg.DataDir != filepath.Join("data", "node1") {
		t.Fatalf("data dir = %q", cfg.DataDir)
	}
	if cfg.TickInterval != 100*time.Millisecond || cfg.ElectionTicks != 10 {
		t.Fatalf("timing defaults = %v/%d", cfg.TickInterval, cfg.ElectionTicks)
	}
}

func TestParseFlagsFullCluster(t *testing.T) {
	cfg, err := ParseFlags([]string{
		"--id", "2",
		"--raft-addr", "10.0.0.2:9001",
		"--http-addr", "10.0.0.2:8001",
		"--data-dir", "/var/lib/raftlite",
		"--peer", "1,10.0.0.1:9001,10.0.0.1:8001",
		"--peer", "2,10.0.0.2:9001,10.0.0.2:8001",
		"--peer", "3,10.0.0.3:9001,10.0.0.3:8001",
		"--snapshot-threshold", "50",
		"--log-level", "debug",
	}, io.Discard)
	if err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}
	if len(cfg.Peers) != 3 {
		t.Fatalf("peers = %v", cfg.Peers)
	}
	if cfg.Peers[0].ClientAddr != "10.0.0.1:8001" {
		t.Fatalf("peer 1 = %+v", cfg.Peers[0])
	}
	if cfg.SnapshotThreshold != 50 || cfg.DataDir != "/var/lib/raftlite" {
		t.Fatalf("cfg = %+v", cfg)
	}
	if cfg.Logger == nil {
		t.Fatal("no logger was configured")
	}
}

func TestParseFlagsRejectsBadInput(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"no id", []string{"--bootstrap"}, "--id is required"},
		{"zero id", []string{"--id", "0", "--bootstrap"}, "--id is required"},
		{"bootstrap and join", []string{"--id", "1", "--bootstrap", "--join", "127.0.0.1:8001"}, "mutually exclusive"},
		{"no peers and no mode", []string{"--id", "1"}, "--bootstrap or --join"},
		{"peer list missing self", []string{"--id", "9", "--peer", "1,a:1,b:1"}, "does not include this server"},
		{"peer with too few fields", []string{"--id", "1", "--peer", "1,a:1"}, "expected id,raft-addr,http-addr"},
		{"peer with a bad id", []string{"--id", "1", "--peer", "x,a:1,b:1"}, "positive integer"},
		{"peer with a zero id", []string{"--id", "1", "--peer", "0,a:1,b:1"}, "positive integer"},
		{"peer with an empty address", []string{"--id", "1", "--peer", "1,,b:1"}, "needs both"},
		{"duplicate peer", []string{"--id", "1", "--peer", "1,a:1,b:1", "--peer", "1,c:1,d:1"}, "listed twice"},
		{"unknown log level", []string{"--id", "1", "--bootstrap", "--log-level", "shout"}, "unknown log level"},
		{"unknown flag", []string{"--id", "1", "--nope"}, "flag provided but not defined"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseFlags(tc.args, io.Discard)
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %q, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestParseFlagsAcceptsEveryLogLevel(t *testing.T) {
	for _, level := range []string{"debug", "info", "warn", "warning", "error", "ERROR"} {
		if _, err := ParseFlags([]string{"--id", "1", "--bootstrap", "--log-level", level}, io.Discard); err != nil {
			t.Fatalf("log level %q: %v", level, err)
		}
	}
}

func TestParseFlagsJoinNeedsNoPeers(t *testing.T) {
	cfg, err := ParseFlags([]string{"--id", "4", "--join", "127.0.0.1:8001"}, io.Discard)
	if err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}
	if len(cfg.Peers) != 0 || cfg.JoinTarget != "127.0.0.1:8001" {
		t.Fatalf("cfg = %+v", cfg)
	}
}

func TestPeerListFormatting(t *testing.T) {
	var p peerList
	if err := p.Set("1,10.0.0.1:9001,10.0.0.1:8001"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got := p.String(); got != "1,10.0.0.1:9001,10.0.0.1:8001" {
		t.Fatalf("String = %q", got)
	}
}
