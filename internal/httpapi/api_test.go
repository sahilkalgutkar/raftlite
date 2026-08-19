package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sahilkalgutkar/raftlite/internal/node"
	"github.com/sahilkalgutkar/raftlite/internal/raft"
	"github.com/sahilkalgutkar/raftlite/internal/transport"
)

// harness is a cluster of nodes, each with a real HTTP server in front of it.
type harness struct {
	t       *testing.T
	mesh    *transport.Mesh
	nodes   map[raft.NodeID]*node.Node
	servers map[raft.NodeID]*httptest.Server
	ids     []raft.NodeID
}

func startHarness(t *testing.T, size int) *harness {
	t.Helper()
	h := &harness{
		t:       t,
		mesh:    transport.NewMesh(),
		nodes:   make(map[raft.NodeID]*node.Node),
		servers: make(map[raft.NodeID]*httptest.Server),
	}

	// Each node's HTTP server has to exist before the cluster does, because
	// the client address is part of the replicated configuration.
	listeners := make(map[raft.NodeID]*httptest.Server, size)
	var peers []raft.Member
	for i := 1; i <= size; i++ {
		id := raft.NodeID(i)
		h.ids = append(h.ids, id)
		srv := httptest.NewUnstartedServer(nil)
		listeners[id] = srv
		peers = append(peers, raft.Member{
			ID:         id,
			Addr:       fmt.Sprintf("mem://%d", i),
			ClientAddr: srv.Listener.Addr().String(),
		})
	}

	for _, id := range h.ids {
		n, err := node.Start(node.Config{
			ID:                id,
			Addr:              fmt.Sprintf("mem://%d", uint64(id)),
			Dir:               filepath.Join(t.TempDir(), fmt.Sprintf("node%d", uint64(id))),
			Peers:             peers,
			Bootstrap:         id == 1,
			TickInterval:      5 * time.Millisecond,
			ElectionTicks:     8,
			HeartbeatTicks:    1,
			SnapshotThreshold: 1 << 30,
			NewTransport:      h.mesh.Factory(id, fmt.Sprintf("mem://%d", uint64(id))),
			NoSync:            true,
		})
		if err != nil {
			t.Fatalf("start node %d: %v", uint64(id), err)
		}
		h.nodes[id] = n

		srv := listeners[id]
		srv.Config.Handler = New(Options{Node: n, RequestTimeout: 3 * time.Second}).Handler()
		srv.Start()
		h.servers[id] = srv
	}

	t.Cleanup(func() {
		for _, srv := range h.servers {
			srv.Close()
		}
		for _, n := range h.nodes {
			_ = n.Stop()
		}
	})
	return h
}

// leaderURL waits for a leader and returns its base URL.
func (h *harness) leaderURL() string {
	h.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range h.ids {
			if h.nodes[id].IsLeader() {
				return h.servers[id].URL
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	h.t.Fatal("no leader emerged")
	return ""
}

func (h *harness) followerURL() string {
	h.t.Helper()
	h.leaderURL()
	for _, id := range h.ids {
		if !h.nodes[id].IsLeader() {
			return h.servers[id].URL
		}
	}
	h.t.Fatal("every node claims to be leader")
	return ""
}

// do issues a request with redirects followed, the way an ordinary client
// behaves.
func do(t *testing.T, method, url string, body string) (*http.Response, string) {
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
	return resp, string(out)
}

// doNoRedirect issues a request without following redirects, so a test can see
// the 307 itself.
func doNoRedirect(t *testing.T, method, url string, body string) (*http.Response, string) {
	t.Helper()
	client := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp, string(out)
}

func TestPutGetDeleteRoundTrip(t *testing.T) {
	h := startHarness(t, 1)
	base := h.leaderURL()

	resp, body := do(t, http.MethodPut, base+"/kv/greeting", "hello raftlite")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT = %d: %s", resp.StatusCode, body)
	}
	var res resultBody
	if err := json.Unmarshal([]byte(body), &res); err != nil {
		t.Fatalf("decoding %q: %v", body, err)
	}
	if res.Key != "greeting" || res.Revision == 0 {
		t.Fatalf("put result = %+v", res)
	}

	resp, body = do(t, http.MethodGet, base+"/kv/greeting", "")
	if resp.StatusCode != http.StatusOK || body != "hello raftlite" {
		t.Fatalf("GET = %d %q", resp.StatusCode, body)
	}
	if got := resp.Header.Get("X-Raft-Revision"); got != "1" {
		t.Fatalf("revision header = %q", got)
	}
	if got := resp.Header.Get("X-Raft-Consistency"); got != "linearizable" {
		t.Fatalf("consistency header = %q", got)
	}

	resp, body = do(t, http.MethodDelete, base+"/kv/greeting", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE = %d: %s", resp.StatusCode, body)
	}
	if resp, _ = do(t, http.MethodGet, base+"/kv/greeting", ""); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after delete = %d", resp.StatusCode)
	}
	if resp, _ = do(t, http.MethodDelete, base+"/kv/greeting", ""); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("second DELETE = %d", resp.StatusCode)
	}
}

func TestValuesAreReturnedByteForByte(t *testing.T) {
	h := startHarness(t, 1)
	base := h.leaderURL()

	payload := string([]byte{0x00, 0x01, 0xFF, 0xFE, '\n', '"', '\\'})
	if resp, body := do(t, http.MethodPut, base+"/kv/binary", payload); resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT = %d: %s", resp.StatusCode, body)
	}
	resp, body := do(t, http.MethodGet, base+"/kv/binary", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET = %d", resp.StatusCode)
	}
	if body != payload {
		t.Fatalf("value came back as %q, want %q", body, payload)
	}
}

func TestCompareAndSwapOverHTTP(t *testing.T) {
	h := startHarness(t, 1)
	base := h.leaderURL()

	// Create only if absent.
	resp, body := do(t, http.MethodPut, base+"/kv/lock?prev_exists=false", "held-by-a")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create = %d: %s", resp.StatusCode, body)
	}

	// A second creator loses, and is told what is actually there.
	resp, body = do(t, http.MethodPut, base+"/kv/lock?prev_exists=false", "held-by-b")
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("second create = %d: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "held-by-a") {
		t.Fatalf("conflict did not report the current value: %s", body)
	}

	// Swapping on the right value succeeds.
	resp, body = do(t, http.MethodPut, base+"/kv/lock?prev=held-by-a", "held-by-b")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("swap = %d: %s", resp.StatusCode, body)
	}
	// Swapping on a stale value does not.
	resp, _ = do(t, http.MethodPut, base+"/kv/lock?prev=held-by-a", "held-by-c")
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("stale swap = %d", resp.StatusCode)
	}
}

func TestBadRequestsAreRejected(t *testing.T) {
	h := startHarness(t, 1)
	base := h.leaderURL()

	cases := []struct {
		name, method, url, body string
		want                    int
	}{
		{"contradictory conditions", http.MethodPut, base + "/kv/k?prev=x&prev_exists=false", "v", http.StatusBadRequest},
		{"unsupported prev_exists", http.MethodPut, base + "/kv/k?prev_exists=true", "v", http.StatusBadRequest},
		{"unknown consistency", http.MethodGet, base + "/kv/k?consistency=eventual", "", http.StatusBadRequest},
		{"oversized value", http.MethodPut, base + "/kv/k", strings.Repeat("x", maxValueBytes+1), http.StatusBadRequest},
		{"member id is not a number", http.MethodDelete, base + "/members/abc", "", http.StatusBadRequest},
		{"member id zero", http.MethodDelete, base + "/members/0", "", http.StatusBadRequest},
		{"malformed member json", http.MethodPost, base + "/members", "{", http.StatusBadRequest},
		{"member without an address", http.MethodPost, base + "/members", `{"id":9}`, http.StatusBadRequest},
		{"promote a non-member", http.MethodPost, base + "/members/42/promote", "", http.StatusBadRequest},
		{"remove a non-member", http.MethodDelete, base + "/members/42", "", http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, body := do(t, tc.method, tc.url, tc.body)
			if resp.StatusCode != tc.want {
				t.Fatalf("status = %d, want %d: %s", resp.StatusCode, tc.want, body)
			}
		})
	}
}

func TestFollowerRedirectsWritesToTheLeader(t *testing.T) {
	h := startHarness(t, 3)
	leader := h.leaderURL()
	follower := h.followerURL()

	// Seen raw, the follower answers 307 and names the leader, so a client
	// learns the topology instead of just being served silently.
	resp, _ := doNoRedirect(t, http.MethodPut, follower+"/kv/k", "v")
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d, want 307", resp.StatusCode)
	}
	location := resp.Header.Get("Location")
	if !strings.Contains(location, strings.TrimPrefix(leader, "http://")) {
		t.Fatalf("redirected to %q, leader is at %q", location, leader)
	}
	if resp.Header.Get("X-Raft-Leader") == "" || resp.Header.Get("X-Raft-Leader-ID") == "" {
		t.Fatalf("redirect carried no leader headers: %v", resp.Header)
	}

	// An ordinary client follows it, and the body survives -- which is the
	// reason it is a 307 and not a 302.
	resp, body := do(t, http.MethodPut, follower+"/kv/k", "v")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("following the redirect = %d: %s", resp.StatusCode, body)
	}
	resp, body = do(t, http.MethodGet, leader+"/kv/k", "")
	if resp.StatusCode != http.StatusOK || body != "v" {
		t.Fatalf("GET = %d %q", resp.StatusCode, body)
	}
}

func TestStaleReadsAreServedByAnyNode(t *testing.T) {
	h := startHarness(t, 3)
	leader := h.leaderURL()
	follower := h.followerURL()

	if resp, body := do(t, http.MethodPut, leader+"/kv/k", "v"); resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT = %d: %s", resp.StatusCode, body)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, body := doNoRedirect(t, http.MethodGet, follower+"/kv/k?consistency=stale", "")
		if resp.StatusCode == http.StatusOK && body == "v" {
			if got := resp.Header.Get("X-Raft-Consistency"); got != "stale" {
				t.Fatalf("consistency header = %q", got)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("a stale read never succeeded on a follower")
}

func TestLinearizableReadOnAFollowerRedirects(t *testing.T) {
	h := startHarness(t, 3)
	h.leaderURL()
	follower := h.followerURL()

	resp, _ := doNoRedirect(t, http.MethodGet, follower+"/kv/whatever", "")
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d, want a redirect to the leader", resp.StatusCode)
	}
}

func TestStatusAndMembers(t *testing.T) {
	h := startHarness(t, 3)
	base := h.leaderURL()

	if resp, body := do(t, http.MethodPut, base+"/kv/k", "v"); resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT = %d: %s", resp.StatusCode, body)
	}

	resp, body := do(t, http.MethodGet, base+"/status", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /status = %d", resp.StatusCode)
	}
	var st statusBody
	if err := json.Unmarshal([]byte(body), &st); err != nil {
		t.Fatalf("decoding %q: %v", body, err)
	}
	if st.Role != "leader" || st.Term == 0 || st.Keys != 1 {
		t.Fatalf("status = %+v", st)
	}
	if len(st.Members) != 3 {
		t.Fatalf("members = %v", st.Members)
	}
	// The leader's view of who is behind is the first thing worth seeing when
	// a cluster misbehaves.
	if len(st.Followers) != 3 {
		t.Fatalf("followers = %v", st.Followers)
	}

	resp, body = do(t, http.MethodGet, base+"/members", "")
	if resp.StatusCode != http.StatusOK || !strings.Contains(body, "client_addr") {
		t.Fatalf("GET /members = %d %s", resp.StatusCode, body)
	}
}

func TestMembershipOverHTTP(t *testing.T) {
	h := startHarness(t, 3)
	base := h.leaderURL()

	// Add a fourth server as a learner. Nothing is actually running there, but
	// the configuration change itself must commit and replicate.
	body, _ := json.Marshal(addMemberRequest{ID: 4, Addr: "mem://4", ClientAddr: "127.0.0.1:1", Voting: false})
	resp, out := do(t, http.MethodPost, base+"/members", string(body))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /members = %d: %s", resp.StatusCode, out)
	}
	if !strings.Contains(out, `"id":4`) {
		t.Fatalf("member list after add: %s", out)
	}

	resp, out = do(t, http.MethodPost, base+"/members/4/promote", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("promote = %d: %s", resp.StatusCode, out)
	}

	resp, out = do(t, http.MethodDelete, base+"/members/4", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("remove = %d: %s", resp.StatusCode, out)
	}
	if strings.Contains(out, `"id":4`) {
		t.Fatalf("member 4 survived removal: %s", out)
	}
}

func TestHealth(t *testing.T) {
	h := startHarness(t, 1)
	base := h.leaderURL()

	resp, body := do(t, http.MethodGet, base+"/healthz", "")
	if resp.StatusCode != http.StatusOK || !strings.Contains(body, "ok") {
		t.Fatalf("healthz = %d %s", resp.StatusCode, body)
	}

	_ = h.nodes[1].Stop()
	resp, _ = do(t, http.MethodGet, base+"/healthz", "")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("healthz on a stopped node = %d", resp.StatusCode)
	}
}

func TestWritesFailWhileThereIsNoLeader(t *testing.T) {
	h := startHarness(t, 3)
	base := h.leaderURL()

	// Cut every node off from every other. Nobody can lead, and the honest
	// answer is that the cluster cannot serve this request.
	h.mesh.Isolate(h.ids...)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, _ := doNoRedirect(t, http.MethodPut, base+"/kv/k", "v")
		if resp.StatusCode == http.StatusServiceUnavailable {
			h.mesh.Heal()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.mesh.Heal()
	t.Fatal("a leaderless cluster never reported itself unavailable")
}

func TestServerImplementsHandler(t *testing.T) {
	h := startHarness(t, 1)
	h.leaderURL()

	// The server is usable directly as an http.Handler, not only through
	// Handler().
	srv := New(Options{Node: h.nodes[1]})
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestUnknownRoutesAre404(t *testing.T) {
	h := startHarness(t, 1)
	base := h.leaderURL()
	if resp, _ := do(t, http.MethodGet, base+"/nope", ""); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestEmptyValuesAreAllowed(t *testing.T) {
	h := startHarness(t, 1)
	base := h.leaderURL()

	if resp, body := do(t, http.MethodPut, base+"/kv/empty", ""); resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT = %d: %s", resp.StatusCode, body)
	}
	resp, body := do(t, http.MethodGet, base+"/kv/empty", "")
	if resp.StatusCode != http.StatusOK || body != "" {
		t.Fatalf("GET = %d %q", resp.StatusCode, body)
	}
}

func TestLargeValueAtTheLimit(t *testing.T) {
	h := startHarness(t, 1)
	base := h.leaderURL()

	value := strings.Repeat("a", maxValueBytes)
	if resp, _ := do(t, http.MethodPut, base+"/kv/big", value); resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT at the limit = %d", resp.StatusCode)
	}
	resp, body := do(t, http.MethodGet, base+"/kv/big", "")
	if resp.StatusCode != http.StatusOK || len(body) != maxValueBytes {
		t.Fatalf("GET = %d, %d bytes", resp.StatusCode, len(body))
	}
}

func TestErrorBodiesAreJSON(t *testing.T) {
	h := startHarness(t, 1)
	base := h.leaderURL()

	resp, body := do(t, http.MethodGet, base+"/kv/missing", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content type = %q", ct)
	}
	var e errorBody
	if err := json.Unmarshal([]byte(body), &e); err != nil || e.Error == "" {
		t.Fatalf("body = %q (%v)", body, err)
	}
}

func TestBufferedBodyIsNotRequired(t *testing.T) {
	// A request with no body at all must be handled, not panic.
	h := startHarness(t, 1)
	base := h.leaderURL()

	req, err := http.NewRequest(http.MethodPut, base+"/kv/nobody", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		out, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d: %s", resp.StatusCode, out)
	}
	_ = bytes.MinRead
}

func TestConsensusErrorsMapToStatuses(t *testing.T) {
	// The distinction that matters to a client is "ask someone else" versus
	// "this went wrong", so the mapping is worth pinning down directly.
	cases := []struct {
		err  error
		want int
	}{
		{raft.ErrNotLeader, http.StatusServiceUnavailable},
		{node.ErrLeadershipLost, http.StatusServiceUnavailable},
		{raft.ErrReadIndexUnavailable, http.StatusServiceUnavailable},
		{node.ErrStopped, http.StatusServiceUnavailable},
		{raft.ErrConfChangeInFlight, http.StatusConflict},
		{raft.ErrInvalidConfChange, http.StatusBadRequest},
		{context.DeadlineExceeded, http.StatusGatewayTimeout},
		{fmt.Errorf("wrapped: %w", raft.ErrNotLeader), http.StatusServiceUnavailable},
	}
	for _, tc := range cases {
		if got := statusFor(tc.err); got != tc.want {
			t.Fatalf("statusFor(%v) = %d, want %d", tc.err, got, tc.want)
		}
	}
}

func TestMetricsEndpoint(t *testing.T) {
	h := startHarness(t, 3)
	base := h.leaderURL()
	if resp, body := do(t, http.MethodPut, base+"/kv/k", "v"); resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT = %d: %s", resp.StatusCode, body)
	}

	resp, body := do(t, http.MethodGet, base+"/metrics", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("content type = %q", ct)
	}
	for _, want := range []string{
		"# TYPE raftlite_term gauge",
		"# TYPE raftlite_proposals_total counter",
		"raftlite_is_leader{",
		"raftlite_keys{",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in:\n%s", want, body)
		}
	}
	// Any node serves its own metrics, leader or not.
	follower := h.followerURL()
	if resp, _ := doNoRedirect(t, http.MethodGet, follower+"/metrics", ""); resp.StatusCode != http.StatusOK {
		t.Fatalf("follower /metrics = %d", resp.StatusCode)
	}
}

func TestAWriteIsVisibleInStatusAsSoonAsItReturns(t *testing.T) {
	// A client that gets a success and immediately asks what the cluster looks
	// like must not be shown a view that predates its own change. This is
	// ordering inside the node's event loop, not luck, so it is asserted in a
	// tight loop rather than with a wait.
	h := startHarness(t, 3)
	base := h.leaderURL()

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%02d", i)
		if resp, body := do(t, http.MethodPut, base+"/kv/"+key, "v"); resp.StatusCode != http.StatusOK {
			t.Fatalf("PUT = %d: %s", resp.StatusCode, body)
		}
		_, body := do(t, http.MethodGet, base+"/status", "")
		var st statusBody
		if err := json.Unmarshal([]byte(body), &st); err != nil {
			t.Fatalf("decoding %q: %v", body, err)
		}
		if st.Keys != i+1 {
			t.Fatalf("after write %d the status reported %d keys", i+1, st.Keys)
		}
	}

	// The same for membership: add a member and read the list back at once.
	memberBody, _ := json.Marshal(addMemberRequest{ID: 4, Addr: "mem://4", ClientAddr: "127.0.0.1:1"})
	resp, out := do(t, http.MethodPost, base+"/members", string(memberBody))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /members = %d: %s", resp.StatusCode, out)
	}
	if !strings.Contains(out, `"id":4`) {
		t.Fatalf("the response to an add did not include the added member: %s", out)
	}
	if _, list := do(t, http.MethodGet, base+"/members", ""); !strings.Contains(list, `"id":4`) {
		t.Fatalf("member list read immediately after the add: %s", list)
	}
}
