// Package cli is the raftctl client: the operator-facing view of a cluster.
//
// It talks to the same HTTP API as everything else, and it is deliberately
// thin. Anything it can work out for itself -- which endpoint is up, where the
// leader is -- it works out, because an operator reaching for a CLI during an
// incident should not also have to know which node is currently in charge.
package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client talks to one cluster through any of its endpoints.
type Client struct {
	endpoints []string
	http      *http.Client
}

// NewClient builds a client over a list of node addresses.
func NewClient(endpoints []string, timeout time.Duration) (*Client, error) {
	cleaned := make([]string, 0, len(endpoints))
	for _, e := range endpoints {
		e = strings.TrimSpace(e)
		e = strings.TrimPrefix(e, "http://")
		e = strings.TrimSuffix(e, "/")
		if e != "" {
			cleaned = append(cleaned, e)
		}
	}
	if len(cleaned) == 0 {
		return nil, errors.New("no endpoints configured")
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &Client{endpoints: cleaned, http: &http.Client{Timeout: timeout}}, nil
}

// Endpoints returns the addresses this client will try.
func (c *Client) Endpoints() []string { return c.endpoints }

// response is one HTTP reply, already read.
type response struct {
	status int
	body   []byte
	header http.Header
}

// do sends a request to each endpoint in turn until one answers at all.
//
// Only a connection failure moves on to the next endpoint. A node that answers
// with an error has answered, and hiding that behind a retry against a
// different node would turn one clear message into a confusing one -- the
// operator would see whatever the last node said, not the first thing that
// went wrong.
func (c *Client) do(method, path string, body []byte) (*response, error) {
	var lastErr error
	for _, endpoint := range c.endpoints {
		var reader io.Reader
		if body != nil {
			reader = bytes.NewReader(body)
		}
		req, err := http.NewRequest(method, "http://"+endpoint+path, reader)
		if err != nil {
			return nil, err
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}

		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("%s: %w", endpoint, err)
			continue
		}
		out, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			lastErr = fmt.Errorf("%s: %w", endpoint, readErr)
			continue
		}
		return &response{status: resp.StatusCode, body: out, header: resp.Header}, nil
	}
	return nil, fmt.Errorf("no endpoint answered: %w", lastErr)
}

// apiError turns an error response into something worth printing.
func apiError(r *response) error {
	var body struct {
		Error   string `json:"error"`
		Current string `json:"current"`
	}
	if err := json.Unmarshal(r.body, &body); err == nil && body.Error != "" {
		if body.Current != "" {
			return fmt.Errorf("%s (current value: %q)", body.Error, body.Current)
		}
		return errors.New(body.Error)
	}
	text := strings.TrimSpace(string(r.body))
	if text == "" {
		text = http.StatusText(r.status)
	}
	return fmt.Errorf("%s (HTTP %d)", text, r.status)
}

// WriteResult is what a successful write reports.
type WriteResult struct {
	Key      string `json:"key"`
	Revision uint64 `json:"revision"`
	Existed  bool   `json:"existed"`
	Swapped  bool   `json:"swapped"`
}

// Put writes a value, optionally conditionally.
func (c *Client) Put(key string, value []byte, cond Condition) (WriteResult, error) {
	path := "/kv/" + url.PathEscape(key) + cond.query()
	resp, err := c.do(http.MethodPut, path, value)
	if err != nil {
		return WriteResult{}, err
	}
	if resp.status != http.StatusOK {
		return WriteResult{}, apiError(resp)
	}
	var out WriteResult
	if err := json.Unmarshal(resp.body, &out); err != nil {
		return WriteResult{}, fmt.Errorf("decoding the response: %w", err)
	}
	return out, nil
}

// Condition expresses a compare-and-swap.
type Condition struct {
	// Prev is the value the key must currently hold.
	Prev *string
	// MustBeAbsent requires the key not to exist.
	MustBeAbsent bool
}

func (c Condition) query() string {
	switch {
	case c.MustBeAbsent:
		return "?prev_exists=false"
	case c.Prev != nil:
		return "?prev=" + url.QueryEscape(*c.Prev)
	default:
		return ""
	}
}

// ReadResult is a value plus the revision it was read at.
type ReadResult struct {
	Value       []byte
	Revision    string
	Consistency string
}

// Get reads a key. A stale read is served by whichever node answers first; a
// linearizable one is redirected to the leader by the node that receives it.
func (c *Client) Get(key string, stale bool) (ReadResult, bool, error) {
	path := "/kv/" + url.PathEscape(key)
	if stale {
		path += "?consistency=stale"
	}
	resp, err := c.do(http.MethodGet, path, nil)
	if err != nil {
		return ReadResult{}, false, err
	}
	switch resp.status {
	case http.StatusOK:
		return ReadResult{
			Value:       resp.body,
			Revision:    resp.header.Get("X-Raft-Revision"),
			Consistency: resp.header.Get("X-Raft-Consistency"),
		}, true, nil
	case http.StatusNotFound:
		return ReadResult{}, false, nil
	default:
		return ReadResult{}, false, apiError(resp)
	}
}

// Delete removes a key, reporting whether it was there.
func (c *Client) Delete(key string) (WriteResult, bool, error) {
	resp, err := c.do(http.MethodDelete, "/kv/"+url.PathEscape(key), nil)
	if err != nil {
		return WriteResult{}, false, err
	}
	switch resp.status {
	case http.StatusOK:
		var out WriteResult
		if err := json.Unmarshal(resp.body, &out); err != nil {
			return WriteResult{}, false, fmt.Errorf("decoding the response: %w", err)
		}
		return out, true, nil
	case http.StatusNotFound:
		return WriteResult{}, false, nil
	default:
		return WriteResult{}, false, apiError(resp)
	}
}

// Member is one server as the API reports it.
type Member struct {
	ID         uint64 `json:"id"`
	Addr       string `json:"addr"`
	ClientAddr string `json:"client_addr"`
	Learner    bool   `json:"learner"`
}

// Follower is the leader's view of one replica's progress.
type Follower struct {
	ID           uint64 `json:"id"`
	Match        uint64 `json:"match_index"`
	Next         uint64 `json:"next_index"`
	Learner      bool   `json:"learner"`
	RecentActive bool   `json:"recently_active"`
}

// Status is one node's view of the cluster.
type Status struct {
	ID        uint64     `json:"id"`
	Role      string     `json:"role"`
	Term      uint64     `json:"term"`
	Leader    uint64     `json:"leader"`
	LeaderURL string     `json:"leader_url"`
	Commit    uint64     `json:"commit_index"`
	Applied   uint64     `json:"applied_index"`
	LastIndex uint64     `json:"last_index"`
	Snapshot  uint64     `json:"snapshot_index"`
	Keys      int        `json:"keys"`
	Members   []Member   `json:"members"`
	Followers []Follower `json:"followers"`
}

// StatusOf asks one specific endpoint what it thinks, which is the point of
// the status command: disagreement between nodes is the interesting signal.
func (c *Client) StatusOf(endpoint string) (Status, error) {
	single := &Client{endpoints: []string{endpoint}, http: c.http}
	resp, err := single.do(http.MethodGet, "/status", nil)
	if err != nil {
		return Status{}, err
	}
	if resp.status != http.StatusOK {
		return Status{}, apiError(resp)
	}
	var out Status
	if err := json.Unmarshal(resp.body, &out); err != nil {
		return Status{}, fmt.Errorf("decoding the response: %w", err)
	}
	return out, nil
}

// Members lists the cluster configuration.
func (c *Client) Members() ([]Member, error) {
	resp, err := c.do(http.MethodGet, "/members", nil)
	if err != nil {
		return nil, err
	}
	if resp.status != http.StatusOK {
		return nil, apiError(resp)
	}
	var out struct {
		Members []Member `json:"members"`
	}
	if err := json.Unmarshal(resp.body, &out); err != nil {
		return nil, fmt.Errorf("decoding the response: %w", err)
	}
	return out.Members, nil
}

// AddMember adds a server to the cluster.
func (c *Client) AddMember(id uint64, addr, clientAddr string, voting bool) ([]Member, error) {
	body, err := json.Marshal(map[string]any{
		"id": id, "addr": addr, "client_addr": clientAddr, "voting": voting,
	})
	if err != nil {
		return nil, err
	}
	return c.memberCall(http.MethodPost, "/members", body)
}

// PromoteMember turns a learner into a voter.
func (c *Client) PromoteMember(id uint64) ([]Member, error) {
	return c.memberCall(http.MethodPost, fmt.Sprintf("/members/%d/promote", id), nil)
}

// RemoveMember drops a server.
func (c *Client) RemoveMember(id uint64) ([]Member, error) {
	return c.memberCall(http.MethodDelete, fmt.Sprintf("/members/%d", id), nil)
}

func (c *Client) memberCall(method, path string, body []byte) ([]Member, error) {
	resp, err := c.do(method, path, body)
	if err != nil {
		return nil, err
	}
	if resp.status != http.StatusOK {
		return nil, apiError(resp)
	}
	var out struct {
		Members []Member `json:"members"`
	}
	if err := json.Unmarshal(resp.body, &out); err != nil {
		return nil, fmt.Errorf("decoding the response: %w", err)
	}
	return out.Members, nil
}
