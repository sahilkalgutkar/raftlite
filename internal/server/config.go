// Package server wires a raftlite node, its HTTP API and its lifecycle into
// something a process manager can run.
//
// Everything except argument parsing and signal handling lives here rather
// than in main, so the daemon can be started, driven and shut down inside a
// test instead of only by launching a binary and hoping.
package server

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sahilkalgutkar/raftlite/internal/raft"
)

// Config is one server's settings.
type Config struct {
	ID         raft.NodeID
	RaftAddr   string
	HTTPAddr   string
	DataDir    string
	Peers      []raft.Member
	Bootstrap  bool
	JoinTarget string

	TickInterval      time.Duration
	ElectionTicks     int
	HeartbeatTicks    int
	SnapshotThreshold uint64
	RequestTimeout    time.Duration
	NoSync            bool

	LogLevel string
	Logger   *slog.Logger
}

// peerList collects repeated --peer flags.
type peerList []raft.Member

func (p *peerList) String() string {
	parts := make([]string, 0, len(*p))
	for _, m := range *p {
		parts = append(parts, fmt.Sprintf("%d,%s,%s", uint64(m.ID), m.Addr, m.ClientAddr))
	}
	return strings.Join(parts, " ")
}

// Set parses one "id,raftAddr,clientAddr" triple.
func (p *peerList) Set(v string) error {
	fields := strings.Split(v, ",")
	if len(fields) != 3 {
		return fmt.Errorf("expected id,raft-addr,http-addr but got %q", v)
	}
	id, err := strconv.ParseUint(strings.TrimSpace(fields[0]), 10, 64)
	if err != nil || id == 0 {
		return fmt.Errorf("peer id must be a positive integer, got %q", fields[0])
	}
	raftAddr := strings.TrimSpace(fields[1])
	clientAddr := strings.TrimSpace(fields[2])
	if raftAddr == "" || clientAddr == "" {
		return fmt.Errorf("peer %d needs both a raft address and an http address", id)
	}
	for _, existing := range *p {
		if existing.ID == raft.NodeID(id) {
			return fmt.Errorf("peer %d is listed twice", id)
		}
	}
	*p = append(*p, raft.Member{ID: raft.NodeID(id), Addr: raftAddr, ClientAddr: clientAddr})
	return nil
}

// ParseFlags turns command line arguments into a Config. It takes the argument
// slice rather than reading os.Args so the parsing can be tested directly.
func ParseFlags(args []string, stderr io.Writer) (Config, error) {
	fs := flag.NewFlagSet("raftlited", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		cfg   Config
		id    uint64
		peers peerList
	)
	fs.Uint64Var(&id, "id", 0, "this server's unique id (required)")
	fs.StringVar(&cfg.RaftAddr, "raft-addr", "127.0.0.1:9001", "address peers connect to")
	fs.StringVar(&cfg.HTTPAddr, "http-addr", "127.0.0.1:8001", "address clients connect to")
	fs.StringVar(&cfg.DataDir, "data-dir", "", "directory for the log and snapshots (default ./data/node<id>)")
	fs.Var(&peers, "peer", "cluster member as id,raft-addr,http-addr (repeat for each member)")
	fs.BoolVar(&cfg.Bootstrap, "bootstrap", false, "call the first election; exactly one founding member should set this")
	fs.StringVar(&cfg.JoinTarget, "join", "", "http address of any member of an existing cluster to join")
	fs.DurationVar(&cfg.TickInterval, "tick-interval", 100*time.Millisecond, "logical clock period")
	fs.IntVar(&cfg.ElectionTicks, "election-ticks", 10, "ticks without a leader before campaigning")
	fs.IntVar(&cfg.HeartbeatTicks, "heartbeat-ticks", 1, "ticks between leader heartbeats")
	fs.Uint64Var(&cfg.SnapshotThreshold, "snapshot-threshold", 1000, "applied entries between snapshots")
	fs.DurationVar(&cfg.RequestTimeout, "request-timeout", 5*time.Second, "how long an API request may wait on consensus")
	fs.BoolVar(&cfg.NoSync, "unsafe-no-fsync", false, "skip fsync; much faster and not crash safe")
	fs.StringVar(&cfg.LogLevel, "log-level", "info", "debug, info, warn or error")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if id == 0 {
		return Config{}, errors.New("--id is required and must be greater than zero")
	}
	cfg.ID = raft.NodeID(id)
	cfg.Peers = peers

	if cfg.DataDir == "" {
		cfg.DataDir = filepath.Join("data", fmt.Sprintf("node%d", id))
	}
	if cfg.Bootstrap && cfg.JoinTarget != "" {
		return Config{}, errors.New("--bootstrap and --join are mutually exclusive")
	}
	if cfg.JoinTarget == "" && len(cfg.Peers) == 0 {
		// A lone server with no peers is a perfectly valid one-node cluster,
		// but it has to be told it is one rather than sitting there waiting
		// for members that will never be configured.
		if !cfg.Bootstrap {
			return Config{}, errors.New("a server with no --peer entries must pass --bootstrap or --join")
		}
		cfg.Peers = []raft.Member{{ID: cfg.ID, Addr: cfg.RaftAddr, ClientAddr: cfg.HTTPAddr}}
	}
	if cfg.JoinTarget == "" && !containsID(cfg.Peers, cfg.ID) {
		return Config{}, fmt.Errorf("--peer list does not include this server (id %d)", id)
	}

	level, err := parseLevel(cfg.LogLevel)
	if err != nil {
		return Config{}, err
	}
	cfg.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	return cfg, nil
}

func containsID(members []raft.Member, id raft.NodeID) bool {
	for _, m := range members {
		if m.ID == id {
			return true
		}
	}
	return false
}

func parseLevel(name string) (slog.Level, error) {
	switch strings.ToLower(name) {
	case "debug":
		return slog.LevelDebug, nil
	case "info", "":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown log level %q", name)
	}
}
