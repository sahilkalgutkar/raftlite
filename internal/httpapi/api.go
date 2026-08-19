// Package httpapi is the client-facing surface: plain HTTP, so the cluster can
// be driven with curl and debugged without a client library.
//
// The design decision worth naming is what happens on a node that is not the
// leader. Silently forwarding the request would make every node look like it
// can accept writes, which hides where the work is really happening and turns
// one slow leader into a mystery. Instead a non-leader answers 307 with the
// leader's address, so the caller learns the topology and can go straight
// there next time. Reads that are allowed to be stale are served locally by
// anyone.
package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/sahilkalgutkar/raftlite/internal/fsm"
	"github.com/sahilkalgutkar/raftlite/internal/node"
	"github.com/sahilkalgutkar/raftlite/internal/raft"
)

// maxValueBytes caps a single value. Every write is replicated to every node
// and held in memory by all of them, so an unbounded value is an unbounded
// cluster-wide memory commitment handed over by any client.
const maxValueBytes = 1 << 20 // 1 MiB

// Options configures the API server.
type Options struct {
	Node *node.Node
	// RequestTimeout bounds how long a request may wait on consensus. Without
	// it a partitioned node accumulates hung clients until it runs out of
	// sockets.
	RequestTimeout time.Duration
	Logger         *slog.Logger
}

// Server serves the client API for one node.
type Server struct {
	node    *node.Node
	timeout time.Duration
	logger  *slog.Logger
	mux     *http.ServeMux
}

// New builds the server and registers its routes.
func New(opts Options) *Server {
	if opts.RequestTimeout <= 0 {
		opts.RequestTimeout = 5 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}

	s := &Server{
		node:    opts.Node,
		timeout: opts.RequestTimeout,
		logger:  opts.Logger,
		mux:     http.NewServeMux(),
	}

	s.mux.HandleFunc("GET /kv/{key}", s.handleGet)
	s.mux.HandleFunc("PUT /kv/{key}", s.handlePut)
	s.mux.HandleFunc("DELETE /kv/{key}", s.handleDelete)
	s.mux.HandleFunc("GET /status", s.handleStatus)
	s.mux.HandleFunc("GET /members", s.handleListMembers)
	s.mux.HandleFunc("POST /members", s.handleAddMember)
	s.mux.HandleFunc("POST /members/{id}/promote", s.handlePromoteMember)
	s.mux.HandleFunc("DELETE /members/{id}", s.handleRemoveMember)
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)
	return s
}

// Handler returns the routed handler.
func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// errorBody is the shape of every failure response.
type errorBody struct {
	Error string `json:"error"`
	// Leader is filled in when the failure is "you asked the wrong node".
	Leader string `json:"leader,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, errorBody{Error: err.Error()})
}

// requireLeader sends the caller to the leader unless this node is it. It
// reports whether the request should continue here.
func (s *Server) requireLeader(w http.ResponseWriter, r *http.Request) bool {
	if s.node.IsLeader() {
		return true
	}
	st := s.node.Status()
	if st.Leader == raft.None {
		writeJSON(w, http.StatusServiceUnavailable, errorBody{
			Error: "no leader elected; the cluster cannot accept this request right now",
		})
		return false
	}

	addr := s.node.LeaderClientAddr()
	w.Header().Set("X-Raft-Leader-ID", strconv.FormatUint(uint64(st.Leader), 10))
	if addr == "" {
		// A leader exists but never advertised a client address. Say so
		// plainly rather than sending the caller to an empty URL.
		writeJSON(w, http.StatusServiceUnavailable, errorBody{
			Error:  "the leader has no advertised client address",
			Leader: strconv.FormatUint(uint64(st.Leader), 10),
		})
		return false
	}

	w.Header().Set("X-Raft-Leader", addr)
	// 307 rather than 302: the method and body must survive the redirect, or
	// every PUT would arrive at the leader as a GET.
	http.Redirect(w, r, "http://"+addr+r.URL.RequestURI(), http.StatusTemporaryRedirect)
	return false
}

// statusFor maps a consensus error onto an HTTP status. The distinction that
// matters is between "ask someone else" and "this went wrong".
func statusFor(err error) int {
	switch {
	case errors.Is(err, raft.ErrNotLeader), errors.Is(err, node.ErrLeadershipLost),
		errors.Is(err, raft.ErrReadIndexUnavailable):
		return http.StatusServiceUnavailable
	case errors.Is(err, raft.ErrConfChangeInFlight):
		return http.StatusConflict
	case errors.Is(err, raft.ErrInvalidConfChange):
		return http.StatusBadRequest
	case errors.Is(err, node.ErrStopped):
		return http.StatusServiceUnavailable
	default:
		return http.StatusGatewayTimeout
	}
}

// handleMetrics serves the Prometheus text exposition format.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if _, err := s.node.Metrics().WriteTo(w); err != nil {
		s.logger.Warn("could not write metrics", "err", err)
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	select {
	case <-s.node.Done():
		writeError(w, http.StatusServiceUnavailable, errors.New("node has stopped"))
	default:
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

func readValue(r *http.Request) ([]byte, error) {
	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, maxValueBytes+1))
	if err != nil {
		return nil, fmt.Errorf("reading the request body: %w", err)
	}
	if len(body) > maxValueBytes {
		return nil, fmt.Errorf("value is larger than the %d byte limit", maxValueBytes)
	}
	return body, nil
}

// resultBody is what a successful write returns.
type resultBody struct {
	Key      string `json:"key"`
	Revision uint64 `json:"revision"`
	Existed  bool   `json:"existed"`
	Swapped  bool   `json:"swapped,omitempty"`
}

func resultFrom(key string, res fsm.Result) resultBody {
	return resultBody{Key: key, Revision: res.Revision, Existed: res.Existed, Swapped: res.Swapped}
}
