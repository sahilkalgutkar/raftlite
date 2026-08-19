package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/sahilkalgutkar/raftlite/internal/raft"
)

// statusBody is the JSON shape of GET /status.
type statusBody struct {
	ID        uint64         `json:"id"`
	Role      string         `json:"role"`
	Term      uint64         `json:"term"`
	Leader    uint64         `json:"leader"`
	LeaderURL string         `json:"leader_url,omitempty"`
	Commit    uint64         `json:"commit_index"`
	Applied   uint64         `json:"applied_index"`
	LastIndex uint64         `json:"last_index"`
	Snapshot  uint64         `json:"snapshot_index"`
	Keys      int            `json:"keys"`
	Members   []memberBody   `json:"members"`
	Followers []followerBody `json:"followers,omitempty"`
}

type memberBody struct {
	ID         uint64 `json:"id"`
	Addr       string `json:"addr"`
	ClientAddr string `json:"client_addr,omitempty"`
	Learner    bool   `json:"learner"`
}

// followerBody is the leader's view of one follower, which is the first thing
// worth looking at when a cluster is behaving oddly: it shows exactly who is
// behind and by how much.
type followerBody struct {
	ID           uint64 `json:"id"`
	Match        uint64 `json:"match_index"`
	Next         uint64 `json:"next_index"`
	Learner      bool   `json:"learner"`
	RecentActive bool   `json:"recently_active"`
}

func membersOf(cfg raft.Config) []memberBody {
	out := make([]memberBody, 0, len(cfg.Members))
	for _, m := range cfg.Members {
		out = append(out, memberBody{
			ID: uint64(m.ID), Addr: m.Addr, ClientAddr: m.ClientAddr, Learner: m.Learner,
		})
	}
	return out
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	st := s.node.Status()
	body := statusBody{
		ID:        uint64(st.ID),
		Role:      st.Role.String(),
		Term:      st.Term,
		Leader:    uint64(st.Leader),
		LeaderURL: s.node.LeaderClientAddr(),
		Commit:    st.Commit,
		Applied:   st.Applied,
		LastIndex: st.LastIndex,
		Snapshot:  st.Snapshot,
		Keys:      s.node.Store().Len(),
		Members:   membersOf(st.Config),
	}
	for _, m := range st.Config.Members {
		p, ok := st.Progress[m.ID]
		if !ok {
			continue
		}
		body.Followers = append(body.Followers, followerBody{
			ID: uint64(m.ID), Match: p.Match, Next: p.Next, Learner: p.Learner, RecentActive: p.RecentActive,
		})
	}
	writeJSON(w, http.StatusOK, body)
}

func (s *Server) handleListMembers(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"members": membersOf(s.node.Status().Config)})
}

// addMemberRequest is the body of POST /members.
type addMemberRequest struct {
	ID         uint64 `json:"id"`
	Addr       string `json:"addr"`
	ClientAddr string `json:"client_addr"`
	Voting     bool   `json:"voting"`
}

func (s *Server) handleAddMember(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w, r) {
		return
	}

	var req addMemberRequest
	if err := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 64<<10)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.ID == 0 || req.Addr == "" {
		writeError(w, http.StatusBadRequest, errors.New("id and addr are required"))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()

	if err := s.node.AddMember(ctx, raft.NodeID(req.ID), req.Addr, req.ClientAddr, req.Voting); err != nil {
		writeError(w, statusFor(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"members": membersOf(s.node.Status().Config)})
}

func (s *Server) handlePromoteMember(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w, r) {
		return
	}
	id, err := memberID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()

	if err := s.node.PromoteMember(ctx, id); err != nil {
		writeError(w, statusFor(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"members": membersOf(s.node.Status().Config)})
}

func (s *Server) handleRemoveMember(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w, r) {
		return
	}
	id, err := memberID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()

	if err := s.node.RemoveMember(ctx, id); err != nil {
		writeError(w, statusFor(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"members": membersOf(s.node.Status().Config)})
}

func memberID(r *http.Request) (raft.NodeID, error) {
	raw := r.PathValue("id")
	id, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || id == 0 {
		return 0, errors.New("member id must be a positive integer")
	}
	return raft.NodeID(id), nil
}
