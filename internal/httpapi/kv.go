package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/sahilkalgutkar/raftlite/internal/fsm"
)

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	// Reads default to linearizable. Stale reads are useful and cheap, but
	// they should be something a caller opts into knowingly rather than a
	// surprise the first time a follower answers.
	linearizable := true
	if v := r.URL.Query().Get("consistency"); v != "" {
		switch v {
		case "linearizable":
			linearizable = true
		case "stale":
			linearizable = false
		default:
			writeError(w, http.StatusBadRequest,
				errors.New("consistency must be either linearizable or stale"))
			return
		}
	}
	if linearizable && !s.requireLeader(w, r) {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()

	value, ok, err := s.node.Get(ctx, key, linearizable)
	if err != nil {
		writeError(w, statusFor(err), err)
		return
	}
	if !ok {
		writeJSON(w, http.StatusNotFound, errorBody{Error: "key not found"})
		return
	}

	// Values are returned as raw bytes rather than wrapped in JSON, so that
	// what a client stored is exactly what it gets back and curl stays useful.
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Raft-Revision", strconv.FormatUint(value.Revision, 10))
	w.Header().Set("X-Raft-Consistency", consistencyLabel(linearizable))
	_, _ = w.Write(value.Data)
}

func consistencyLabel(linearizable bool) string {
	if linearizable {
		return "linearizable"
	}
	return "stale"
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w, r) {
		return
	}
	key := r.PathValue("key")

	value, err := readValue(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	cmd, err := buildWrite(r, key, value)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()

	res, err := s.node.Propose(ctx, cmd)
	if err != nil {
		writeError(w, statusFor(err), err)
		return
	}
	if cmd.Op == fsm.OpCAS && !res.Swapped {
		// The comparison failed, so nothing was written. Hand back what is
		// actually stored: the caller needs it to retry, and fetching it
		// separately could read a newer value than the one that lost.
		w.Header().Set("X-Raft-Revision", strconv.FormatUint(res.Revision, 10))
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":    "compare-and-swap failed",
			"key":      key,
			"current":  string(res.Value),
			"existed":  res.Existed,
			"revision": res.Revision,
		})
		return
	}
	writeJSON(w, http.StatusOK, resultFrom(key, res))
}

// buildWrite turns the query parameters into either a plain put or a
// compare-and-swap.
//
//	?prev=<value>          swap only if the key currently holds <value>
//	?prev_exists=false     write only if the key does not exist
func buildWrite(r *http.Request, key string, value []byte) (fsm.Command, error) {
	q := r.URL.Query()
	prev, hasPrev := q["prev"]
	prevExists := q.Get("prev_exists")

	switch {
	case hasPrev && prevExists == "false":
		return fsm.Command{}, errors.New("prev and prev_exists=false contradict each other")
	case hasPrev:
		return fsm.CompareAndSwap(key, []byte(prev[0]), value, true), nil
	case prevExists == "false":
		return fsm.CompareAndSwap(key, nil, value, false), nil
	case prevExists != "":
		return fsm.Command{}, errors.New("prev_exists only accepts false")
	default:
		return fsm.Put(key, value), nil
	}
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w, r) {
		return
	}
	key := r.PathValue("key")

	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()

	res, err := s.node.Propose(ctx, fsm.Delete(key))
	if err != nil {
		writeError(w, statusFor(err), err)
		return
	}
	if !res.Existed {
		writeJSON(w, http.StatusNotFound, errorBody{Error: "key not found"})
		return
	}
	writeJSON(w, http.StatusOK, resultFrom(key, res))
}
