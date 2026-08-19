package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// freeAddr reserves a loopback port and immediately releases it. Peers have to
// be configured with real addresses before the servers exist, so the ports
// cannot simply be zero.
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

func quietLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

func testConfig(t *testing.T, id uint64, raftAddr, httpAddr string) Config {
	t.Helper()
	return Config{
		ID:                nodeID(id),
		RaftAddr:          raftAddr,
		HTTPAddr:          httpAddr,
		DataDir:           filepath.Join(t.TempDir(), fmt.Sprintf("node%d", id)),
		TickInterval:      10 * time.Millisecond,
		ElectionTicks:     8,
		HeartbeatTicks:    1,
		SnapshotThreshold: 1 << 30,
		RequestTimeout:    3 * time.Second,
		NoSync:            true,
		Logger:            quietLogger(),
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func request(t *testing.T, method, url, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(out)
}

func TestSingleServerServesOverRealSockets(t *testing.T) {
	cfg := testConfig(t, 1, freeAddr(t), freeAddr(t))
	cfg.Bootstrap = true
	cfg.Peers = []member{{ID: nodeID(1), Addr: cfg.RaftAddr, ClientAddr: cfg.HTTPAddr}}

	inst, err := Start(cfg)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer inst.Shutdown(context.Background())

	base := "http://" + inst.HTTPAddr()
	waitFor(t, "the node to become leader", func() bool { return inst.Node().IsLeader() })

	if code, body := request(t, http.MethodPut, base+"/kv/k", "v"); code != http.StatusOK {
		t.Fatalf("PUT = %d: %s", code, body)
	}
	if code, body := request(t, http.MethodGet, base+"/kv/k", ""); code != http.StatusOK || body != "v" {
		t.Fatalf("GET = %d %q", code, body)
	}
	if code, _ := request(t, http.MethodGet, base+"/healthz", ""); code != http.StatusOK {
		t.Fatalf("healthz = %d", code)
	}
}

func TestThreeServersOverRealSockets(t *testing.T) {
	raftAddrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	httpAddrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}

	var peers []member
	for i := 0; i < 3; i++ {
		peers = append(peers, member{ID: nodeID(uint64(i + 1)), Addr: raftAddrs[i], ClientAddr: httpAddrs[i]})
	}

	var instances []*Instance
	for i := 0; i < 3; i++ {
		cfg := testConfig(t, uint64(i+1), raftAddrs[i], httpAddrs[i])
		cfg.Peers = peers
		cfg.Bootstrap = i == 0
		inst, err := Start(cfg)
		if err != nil {
			t.Fatalf("Start node %d: %v", i+1, err)
		}
		instances = append(instances, inst)
	}
	defer func() {
		for _, inst := range instances {
			_ = inst.Shutdown(context.Background())
		}
	}()

	var leader *Instance
	waitFor(t, "a leader", func() bool {
		for _, inst := range instances {
			if inst.Node().IsLeader() {
				leader = inst
				return true
			}
		}
		return false
	})

	base := "http://" + leader.HTTPAddr()
	for i := 0; i < 10; i++ {
		if code, body := request(t, http.MethodPut, fmt.Sprintf("%s/kv/key-%d", base, i), "v"); code != http.StatusOK {
			t.Fatalf("PUT %d = %d: %s", i, code, body)
		}
	}
	waitFor(t, "every node to apply every write", func() bool {
		for _, inst := range instances {
			if inst.Node().Store().Len() != 10 {
				return false
			}
		}
		return true
	})

	// A write sent to a follower is redirected and still lands.
	for _, inst := range instances {
		if inst == leader {
			continue
		}
		code, body := request(t, http.MethodPut, "http://"+inst.HTTPAddr()+"/kv/via-follower", "v")
		if code != http.StatusOK {
			t.Fatalf("write via a follower = %d: %s", code, body)
		}
		break
	}
}

func TestAServerJoinsARunningCluster(t *testing.T) {
	// The founding member.
	cfg := testConfig(t, 1, freeAddr(t), freeAddr(t))
	cfg.Bootstrap = true
	cfg.Peers = []member{{ID: nodeID(1), Addr: cfg.RaftAddr, ClientAddr: cfg.HTTPAddr}}

	first, err := Start(cfg)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer first.Shutdown(context.Background())
	waitFor(t, "the founding member to lead", func() bool { return first.Node().IsLeader() })

	base := "http://" + first.HTTPAddr()
	if code, body := request(t, http.MethodPut, base+"/kv/existing", "data"); code != http.StatusOK {
		t.Fatalf("PUT = %d: %s", code, body)
	}

	// A second server that knows only one address: it discovers the rest and
	// announces itself.
	joinCfg := testConfig(t, 2, freeAddr(t), freeAddr(t))
	joinCfg.JoinTarget = first.HTTPAddr()

	second, err := Start(joinCfg)
	if err != nil {
		t.Fatalf("Start joining server: %v", err)
	}
	defer second.Shutdown(context.Background())

	waitFor(t, "the new server to replicate the existing data", func() bool {
		v, ok := second.Node().Store().Get("existing")
		return ok && string(v.Data) == "data"
	})
	waitFor(t, "the leader to list both members", func() bool {
		return len(first.Node().Members()) == 2
	})

	// It joined as a learner, so it is not counted in any quorum yet.
	if first.Node().Status().Config.IsVoter(2) {
		t.Fatal("a joining server became a voter without being promoted")
	}
	if code, body := request(t, http.MethodPost, base+"/members/2/promote", ""); code != http.StatusOK {
		t.Fatalf("promote = %d: %s", code, body)
	}
	waitFor(t, "the promotion to take effect", func() bool {
		return first.Node().Status().Config.IsVoter(2)
	})
}

func TestJoinFailuresAreReported(t *testing.T) {
	cfg := testConfig(t, 2, freeAddr(t), freeAddr(t))
	cfg.JoinTarget = freeAddr(t) // nothing is listening there

	if _, err := Start(cfg); err == nil {
		t.Fatal("Start succeeded against a cluster that does not exist")
	}
}

func TestJoiningWithAnIDAlreadyInUseIsRefused(t *testing.T) {
	cfg := testConfig(t, 1, freeAddr(t), freeAddr(t))
	cfg.Bootstrap = true
	cfg.Peers = []member{{ID: nodeID(1), Addr: cfg.RaftAddr, ClientAddr: cfg.HTTPAddr}}

	first, err := Start(cfg)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer first.Shutdown(context.Background())
	waitFor(t, "a leader", func() bool { return first.Node().IsLeader() })

	clash := testConfig(t, 1, freeAddr(t), freeAddr(t))
	clash.JoinTarget = first.HTTPAddr()
	_, err = Start(clash)
	if err == nil {
		t.Fatal("a server joined with an id that was already taken")
	}
	if !strings.Contains(err.Error(), "already a member") {
		t.Fatalf("err = %v", err)
	}
}

func TestStartRejectsAnUnusableClientAddress(t *testing.T) {
	cfg := testConfig(t, 1, freeAddr(t), "256.256.256.256:99999")
	cfg.Bootstrap = true
	cfg.Peers = []member{{ID: nodeID(1), Addr: cfg.RaftAddr, ClientAddr: cfg.HTTPAddr}}

	if _, err := Start(cfg); err == nil {
		t.Fatal("Start accepted an address it cannot bind")
	}
}

func TestStartRejectsAnUnusablePeerAddress(t *testing.T) {
	cfg := testConfig(t, 1, "256.256.256.256:99999", freeAddr(t))
	cfg.Bootstrap = true
	cfg.Peers = []member{{ID: nodeID(1), Addr: cfg.RaftAddr, ClientAddr: cfg.HTTPAddr}}

	if _, err := Start(cfg); err == nil {
		t.Fatal("Start accepted a peer address it cannot bind")
	}
}

func TestRunStopsWhenTheContextIsCancelled(t *testing.T) {
	cfg := testConfig(t, 1, freeAddr(t), freeAddr(t))
	cfg.Bootstrap = true
	cfg.Peers = []member{{ID: nodeID(1), Addr: cfg.RaftAddr, ClientAddr: cfg.HTTPAddr}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg) }()

	waitFor(t, "the server to bind", func() bool {
		conn, err := net.DialTimeout("tcp", cfg.HTTPAddr, 100*time.Millisecond)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	})

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not return after the context was cancelled")
	}
}

func TestShutdownIsSafeTwice(t *testing.T) {
	cfg := testConfig(t, 1, freeAddr(t), freeAddr(t))
	cfg.Bootstrap = true
	cfg.Peers = []member{{ID: nodeID(1), Addr: cfg.RaftAddr, ClientAddr: cfg.HTTPAddr}}

	inst, err := Start(cfg)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := inst.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := inst.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
}

func TestStartWithoutALoggerStillWorks(t *testing.T) {
	cfg := testConfig(t, 1, freeAddr(t), freeAddr(t))
	cfg.Bootstrap = true
	cfg.Logger = nil
	cfg.Peers = []member{{ID: nodeID(1), Addr: cfg.RaftAddr, ClientAddr: cfg.HTTPAddr}}

	inst, err := Start(cfg)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer inst.Shutdown(context.Background())
	waitFor(t, "a leader", func() bool { return inst.Node().IsLeader() })
}

func TestLinearizableReadsWorkOverRealSockets(t *testing.T) {
	// A regression test with a specific history. Linearizable reads passed
	// every in-process test and then timed out against three real binaries,
	// because the read identifier was never encoded on the wire and the
	// in-memory mesh hands message structs over directly. Anything that
	// depends on a field surviving the codec has to be exercised against real
	// sockets at least once.
	raftAddrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	httpAddrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}

	var peers []member
	for i := 0; i < 3; i++ {
		peers = append(peers, member{ID: nodeID(uint64(i + 1)), Addr: raftAddrs[i], ClientAddr: httpAddrs[i]})
	}

	var instances []*Instance
	for i := 0; i < 3; i++ {
		cfg := testConfig(t, uint64(i+1), raftAddrs[i], httpAddrs[i])
		cfg.Peers = peers
		cfg.Bootstrap = i == 0
		inst, err := Start(cfg)
		if err != nil {
			t.Fatalf("Start node %d: %v", i+1, err)
		}
		instances = append(instances, inst)
	}
	defer func() {
		for _, inst := range instances {
			_ = inst.Shutdown(context.Background())
		}
	}()

	var leader *Instance
	waitFor(t, "a leader", func() bool {
		for _, inst := range instances {
			if inst.Node().IsLeader() {
				leader = inst
				return true
			}
		}
		return false
	})

	base := "http://" + leader.HTTPAddr()
	if code, body := request(t, http.MethodPut, base+"/kv/k", "v"); code != http.StatusOK {
		t.Fatalf("PUT = %d: %s", code, body)
	}

	// The default consistency is linearizable, so this is the path that hung.
	code, body := request(t, http.MethodGet, base+"/kv/k", "")
	if code != http.StatusOK || body != "v" {
		t.Fatalf("linearizable GET = %d %q", code, body)
	}

	// And directly, without the HTTP layer in the way.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := leader.Node().LinearizableRead(ctx); err != nil {
		t.Fatalf("LinearizableRead: %v", err)
	}
}
