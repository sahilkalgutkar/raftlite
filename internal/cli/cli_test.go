package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sahilkalgutkar/raftlite/internal/raft"
	"github.com/sahilkalgutkar/raftlite/internal/server"
)

// realCluster starts genuine servers on loopback ports. The CLI's whole job is
// to talk to a running cluster, so testing it against a mock would mostly test
// the mock.
type realCluster struct {
	t         *testing.T
	instances []*server.Instance
	httpAddrs []string
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func startCluster(t *testing.T, size int) *realCluster {
	t.Helper()
	raftAddrs := make([]string, size)
	httpAddrs := make([]string, size)
	var peers []raft.Member
	for i := 0; i < size; i++ {
		raftAddrs[i] = freeAddr(t)
		httpAddrs[i] = freeAddr(t)
		peers = append(peers, raft.Member{
			ID: raft.NodeID(i + 1), Addr: raftAddrs[i], ClientAddr: httpAddrs[i],
		})
	}

	c := &realCluster{t: t, httpAddrs: httpAddrs}
	for i := 0; i < size; i++ {
		inst, err := server.Start(server.Config{
			ID:                raft.NodeID(i + 1),
			RaftAddr:          raftAddrs[i],
			HTTPAddr:          httpAddrs[i],
			DataDir:           filepath.Join(t.TempDir(), fmt.Sprintf("node%d", i+1)),
			Peers:             peers,
			Bootstrap:         i == 0,
			TickInterval:      10 * time.Millisecond,
			ElectionTicks:     8,
			HeartbeatTicks:    1,
			SnapshotThreshold: 1 << 30,
			RequestTimeout:    3 * time.Second,
			NoSync:            true,
			Logger:            slog.New(slog.DiscardHandler),
		})
		if err != nil {
			t.Fatalf("start node %d: %v", i+1, err)
		}
		c.instances = append(c.instances, inst)
	}
	t.Cleanup(func() {
		for _, inst := range c.instances {
			_ = inst.Shutdown(t.Context())
		}
	})
	c.waitForLeader()
	return c
}

func (c *realCluster) waitForLeader() {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, inst := range c.instances {
			if inst.Node().IsLeader() {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatal("no leader emerged")
}

func (c *realCluster) endpoints() string { return strings.Join(c.httpAddrs, ",") }

// run invokes raftctl exactly as the binary does and returns its output.
func run(t *testing.T, stdin string, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := Run(args, strings.NewReader(stdin), &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func TestPutGetDelete(t *testing.T) {
	c := startCluster(t, 3)
	ep := "--endpoints=" + c.endpoints()

	code, out, errOut := run(t, "", ep, "put", "greeting", "hello raftlite")
	if code != 0 {
		t.Fatalf("put = %d: %s%s", code, out, errOut)
	}
	if !strings.Contains(out, "wrote greeting at revision") {
		t.Fatalf("put output = %q", out)
	}

	code, out, errOut = run(t, "", ep, "get", "greeting")
	if code != 0 || strings.TrimSpace(out) != "hello raftlite" {
		t.Fatalf("get = %d %q %q", code, out, errOut)
	}

	// Overwriting reports that the key was already there.
	_, out, _ = run(t, "", ep, "put", "greeting", "hello again")
	if !strings.Contains(out, "updated greeting") {
		t.Fatalf("overwrite output = %q", out)
	}

	code, out, _ = run(t, "", ep, "del", "greeting")
	if code != 0 || !strings.Contains(out, "deleted greeting") {
		t.Fatalf("del = %d %q", code, out)
	}
	code, _, errOut = run(t, "", ep, "get", "greeting")
	if code == 0 || !strings.Contains(errOut, "not found") {
		t.Fatalf("get after delete = %d %q", code, errOut)
	}
	if code, _, errOut = run(t, "", ep, "del", "greeting"); code == 0 || !strings.Contains(errOut, "not found") {
		t.Fatalf("second delete = %d %q", code, errOut)
	}
}

func TestPutFromStdin(t *testing.T) {
	c := startCluster(t, 1)
	ep := "--endpoints=" + c.endpoints()

	payload := "a value\nwith newlines\n"
	if code, out, errOut := run(t, payload, ep, "put", "piped", "-"); code != 0 {
		t.Fatalf("put = %d: %s%s", code, out, errOut)
	}
	code, out, _ := run(t, "", ep, "get", "piped")
	if code != 0 || out != payload+"\n" {
		t.Fatalf("get = %d %q", code, out)
	}
}

func TestConditionalWrites(t *testing.T) {
	c := startCluster(t, 1)
	ep := "--endpoints=" + c.endpoints()

	if code, _, errOut := run(t, "", ep, "--absent", "put", "lock", "held-by-a"); code != 0 {
		t.Fatalf("create = %d: %s", code, errOut)
	}
	code, _, errOut := run(t, "", ep, "--absent", "put", "lock", "held-by-b")
	if code == 0 {
		t.Fatal("a second create succeeded")
	}
	// The error tells the operator what is actually stored, which is what they
	// need in order to decide what to do next.
	if !strings.Contains(errOut, "held-by-a") {
		t.Fatalf("error did not report the current value: %q", errOut)
	}

	if code, _, errOut := run(t, "", ep, "--prev", "held-by-a", "put", "lock", "held-by-b"); code != 0 {
		t.Fatalf("swap = %d: %s", code, errOut)
	}
	if code, _, _ := run(t, "", ep, "--prev", "held-by-a", "put", "lock", "held-by-c"); code == 0 {
		t.Fatal("a stale swap succeeded")
	}
	if code, _, errOut := run(t, "", ep, "--absent", "--prev", "x", "put", "k", "v"); code == 0 {
		t.Fatalf("contradictory conditions were accepted: %s", errOut)
	}
}

func TestStaleReadHitsWhicheverNodeAnswers(t *testing.T) {
	c := startCluster(t, 3)
	ep := "--endpoints=" + c.endpoints()

	if code, _, errOut := run(t, "", ep, "put", "k", "v"); code != 0 {
		t.Fatalf("put = %d: %s", code, errOut)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if code, out, _ := run(t, "", ep, "--stale", "get", "k"); code == 0 && strings.TrimSpace(out) == "v" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("a stale read never succeeded")
}

func TestStatusShowsEveryEndpoint(t *testing.T) {
	c := startCluster(t, 3)
	ep := "--endpoints=" + c.endpoints()

	code, out, errOut := run(t, "", ep, "status")
	if code != 0 {
		t.Fatalf("status = %d: %s", code, errOut)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 4 {
		t.Fatalf("status printed %d lines, want a header and three rows:\n%s", len(lines), out)
	}
	if !strings.Contains(lines[0], "ENDPOINT") || !strings.Contains(lines[0], "SNAPSHOT") {
		t.Fatalf("header = %q", lines[0])
	}
	if !strings.Contains(out, "leader") {
		t.Fatalf("no leader in the table:\n%s", out)
	}
	// Two of three must be followers.
	if strings.Count(out, "follower") != 2 {
		t.Fatalf("expected two followers:\n%s", out)
	}
}

func TestStatusMarksUnreachableEndpoints(t *testing.T) {
	c := startCluster(t, 1)
	ep := "--endpoints=" + c.endpoints() + "," + freeAddr(t)

	code, out, _ := run(t, "", ep, "status")
	if code != 0 {
		t.Fatalf("status = %d", code)
	}
	if !strings.Contains(out, "unreachable") {
		t.Fatalf("a dead endpoint was not marked unreachable:\n%s", out)
	}
}

func TestStatusWithNothingRunning(t *testing.T) {
	code, out, errOut := run(t, "", "--endpoints="+freeAddr(t), "status")
	if code == 0 {
		t.Fatalf("status against nothing succeeded: %s", out)
	}
	if !strings.Contains(errOut, "no endpoint answered") {
		t.Fatalf("stderr = %q", errOut)
	}
}

func TestMembershipCommands(t *testing.T) {
	c := startCluster(t, 3)
	ep := "--endpoints=" + c.endpoints()

	code, out, errOut := run(t, "", ep, "members")
	if code != 0 {
		t.Fatalf("members = %d: %s", code, errOut)
	}
	if !strings.Contains(out, "RAFT ADDRESS") || strings.Count(out, "voter") != 3 {
		t.Fatalf("members output:\n%s", out)
	}

	code, out, errOut = run(t, "", ep, "member-add", "4", "127.0.0.1:9999", "127.0.0.1:9998")
	if code != 0 {
		t.Fatalf("member-add = %d: %s", code, errOut)
	}
	if !strings.Contains(out, "added 4 as a learner") || !strings.Contains(out, "learner") {
		t.Fatalf("member-add output:\n%s", out)
	}

	if code, out, errOut = run(t, "", ep, "member-promote", "4"); code != 0 {
		t.Fatalf("member-promote = %d: %s%s", code, out, errOut)
	}
	if strings.Count(out, "voter") != 4 {
		t.Fatalf("after promotion:\n%s", out)
	}

	if code, out, errOut = run(t, "", ep, "member-remove", "4"); code != 0 {
		t.Fatalf("member-remove = %d: %s%s", code, out, errOut)
	}
	if strings.Contains(out, "9999") {
		t.Fatalf("removed member still listed:\n%s", out)
	}
}

func TestJSONOutput(t *testing.T) {
	c := startCluster(t, 1)
	ep := "--endpoints=" + c.endpoints()

	if code, _, errOut := run(t, "", ep, "put", "k", "v"); code != 0 {
		t.Fatalf("put = %d: %s", code, errOut)
	}

	code, out, _ := run(t, "", ep, "--json", "get", "k")
	if code != 0 {
		t.Fatalf("get = %d", code)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decoding %q: %v", out, err)
	}
	if got["value"] != "v" || got["consistency"] != "linearizable" {
		t.Fatalf("json = %v", got)
	}

	for _, args := range [][]string{
		{ep, "--json", "put", "k", "v2"},
		{ep, "--json", "status"},
		{ep, "--json", "members"},
		{ep, "--json", "del", "k"},
	} {
		code, out, errOut := run(t, "", args...)
		if code != 0 {
			t.Fatalf("%v = %d: %s", args, code, errOut)
		}
		if !json.Valid([]byte(out)) {
			t.Fatalf("%v produced invalid json:\n%s", args, out)
		}
	}
}

func TestArgumentErrors(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"no command", []string{}, "Usage"},
		{"unknown command", []string{"frobnicate"}, "unknown command"},
		{"put without a value", []string{"put", "k"}, "takes a key and a value"},
		{"get with two keys", []string{"get", "a", "b"}, "exactly one key"},
		{"del with no key", []string{"del"}, "exactly one key"},
		{"member-add with too few arguments", []string{"member-add", "4"}, "an id, a raft address"},
		{"member-add with a bad id", []string{"member-add", "x", "a", "b"}, "positive integer"},
		{"member-promote with no id", []string{"member-promote"}, "exactly one member id"},
		{"member-remove with a zero id", []string{"member-remove", "0"}, "positive integer"},
		{"empty endpoints", []string{"--endpoints=", "status"}, "no endpoints"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, out, errOut := run(t, "", tc.args...)
			if code == 0 {
				t.Fatalf("expected a failure, got 0: %s", out)
			}
			if !strings.Contains(errOut, tc.want) {
				t.Fatalf("stderr = %q, want it to mention %q", errOut, tc.want)
			}
		})
	}
}

func TestUnparseableFlags(t *testing.T) {
	if code, _, _ := run(t, "", "--nonsense", "status"); code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
}

func TestNewClientNormalisesEndpoints(t *testing.T) {
	c, err := NewClient([]string{" http://127.0.0.1:8001/ ", "", "127.0.0.1:8002"}, 0)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	got := c.Endpoints()
	if len(got) != 2 || got[0] != "127.0.0.1:8001" || got[1] != "127.0.0.1:8002" {
		t.Fatalf("endpoints = %v", got)
	}
	if _, err := NewClient(nil, time.Second); err == nil {
		t.Fatal("NewClient accepted an empty endpoint list")
	}
}

func TestClientFallsOverToTheNextEndpoint(t *testing.T) {
	c := startCluster(t, 3)
	// A dead endpoint first: the client must move past it rather than fail.
	ep := "--endpoints=" + freeAddr(t) + "," + c.endpoints()

	if code, out, errOut := run(t, "", ep, "put", "k", "v"); code != 0 {
		t.Fatalf("put = %d: %s%s", code, out, errOut)
	}
	if code, out, _ := run(t, "", ep, "get", "k"); code != 0 || strings.TrimSpace(out) != "v" {
		t.Fatalf("get = %d %q", code, out)
	}
}

func TestKeysWithAwkwardCharacters(t *testing.T) {
	c := startCluster(t, 1)
	ep := "--endpoints=" + c.endpoints()

	for _, key := range []string{"with space", "with/slash", "with%percent", "with?question"} {
		if code, _, errOut := run(t, "", ep, "put", key, "v"); code != 0 {
			t.Fatalf("put %q = %d: %s", key, code, errOut)
		}
		code, out, errOut := run(t, "", ep, "get", key)
		if code != 0 || strings.TrimSpace(out) != "v" {
			t.Fatalf("get %q = %d %q %q", key, code, out, errOut)
		}
	}
}
