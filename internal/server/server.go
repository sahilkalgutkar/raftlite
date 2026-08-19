package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/sahilkalgutkar/raftlite/internal/httpapi"
	"github.com/sahilkalgutkar/raftlite/internal/node"
	"github.com/sahilkalgutkar/raftlite/internal/raft"
)

// Instance is a running server: one consensus node and the HTTP listener in
// front of it.
type Instance struct {
	cfg    Config
	node   *node.Node
	http   *http.Server
	ln     net.Listener
	logger *slog.Logger

	serveErr chan error
}

// Start brings a server up. It returns as soon as both the node and the
// listener are live, so a caller can immediately talk to it.
func Start(cfg Config) (*Instance, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}

	peers := cfg.Peers
	if cfg.JoinTarget != "" {
		discovered, err := discoverMembers(cfg)
		if err != nil {
			return nil, err
		}
		peers = discovered
	}

	// Bind the client listener before starting the node so a bad address fails
	// immediately, rather than after a log has been opened and a cluster has
	// started forming around it.
	ln, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return nil, fmt.Errorf("server: listen for clients on %s: %w", cfg.HTTPAddr, err)
	}

	n, err := node.Start(node.Config{
		ID:                cfg.ID,
		Addr:              cfg.RaftAddr,
		Dir:               cfg.DataDir,
		Peers:             peers,
		Bootstrap:         cfg.Bootstrap,
		TickInterval:      cfg.TickInterval,
		ElectionTicks:     cfg.ElectionTicks,
		HeartbeatTicks:    cfg.HeartbeatTicks,
		SnapshotThreshold: cfg.SnapshotThreshold,
		NoSync:            cfg.NoSync,
		Logger:            cfg.Logger,
	})
	if err != nil {
		_ = ln.Close()
		return nil, err
	}

	api := httpapi.New(httpapi.Options{
		Node:           n,
		RequestTimeout: cfg.RequestTimeout,
		Logger:         cfg.Logger,
	})

	inst := &Instance{
		cfg:    cfg,
		node:   n,
		ln:     ln,
		logger: cfg.Logger,
		http: &http.Server{
			Handler:           api.Handler(),
			ReadHeaderTimeout: 5 * time.Second,
		},
		serveErr: make(chan error, 1),
	}

	go func() {
		err := inst.http.Serve(ln)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		inst.serveErr <- err
	}()

	cfg.Logger.Info("raftlite is up",
		"id", uint64(cfg.ID), "raft_addr", n.Addr(), "http_addr", inst.HTTPAddr(), "data_dir", cfg.DataDir)

	if cfg.JoinTarget != "" {
		// Announce ourselves only once we are actually reachable, so the
		// leader's first append does not arrive at a closed port.
		if err := announceJoin(cfg, inst.HTTPAddr(), n.Addr()); err != nil {
			_ = inst.Shutdown(context.Background())
			return nil, err
		}
	}
	return inst, nil
}

// HTTPAddr is the address clients actually reached, which matters when the
// configured port was zero.
func (i *Instance) HTTPAddr() string { return i.ln.Addr().String() }

// Node exposes the consensus node, mostly so tests can inspect it.
func (i *Instance) Node() *node.Node { return i.node }

// Wait blocks until the context is cancelled, the HTTP server fails, or the
// node stops on its own.
func (i *Instance) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-i.serveErr:
		return err
	case <-i.node.Done():
		return errors.New("server: the consensus node stopped")
	}
}

// Shutdown stops serving and then stops the node, in that order: refusing new
// requests before tearing down what answers them means in-flight callers get a
// real response instead of a closed connection.
func (i *Instance) Shutdown(ctx context.Context) error {
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var errs []error
	if err := i.http.Shutdown(shutdownCtx); err != nil {
		errs = append(errs, fmt.Errorf("server: http shutdown: %w", err))
	}
	if err := i.node.Stop(); err != nil {
		errs = append(errs, err)
	}
	i.logger.Info("raftlite stopped", "id", uint64(i.cfg.ID))
	return errors.Join(errs...)
}

// Run starts a server and blocks until the context is cancelled, then shuts it
// down. It is what main does.
func Run(ctx context.Context, cfg Config) error {
	inst, err := Start(cfg)
	if err != nil {
		return err
	}
	waitErr := inst.Wait(ctx)
	return errors.Join(waitErr, inst.Shutdown(context.Background()))
}

// discoverMembers asks a running cluster who is in it, so a joining server
// does not have to be handed the membership by whoever launches it.
func discoverMembers(cfg Config) ([]raft.Member, error) {
	url := "http://" + cfg.JoinTarget + "/members"
	client := &http.Client{Timeout: 10 * time.Second}

	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("server: asking %s for the member list: %w", cfg.JoinTarget, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server: %s returned %s for the member list", cfg.JoinTarget, resp.Status)
	}

	var body struct {
		Members []struct {
			ID         uint64 `json:"id"`
			Addr       string `json:"addr"`
			ClientAddr string `json:"client_addr"`
			Learner    bool   `json:"learner"`
		} `json:"members"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("server: decoding the member list: %w", err)
	}

	members := make([]raft.Member, 0, len(body.Members)+1)
	for _, m := range body.Members {
		members = append(members, raft.Member{
			ID: raft.NodeID(m.ID), Addr: m.Addr, ClientAddr: m.ClientAddr, Learner: m.Learner,
		})
	}
	if containsID(members, cfg.ID) {
		return nil, fmt.Errorf("server: id %d is already a member of that cluster", uint64(cfg.ID))
	}
	// Include ourselves as a learner. We are not one yet -- the cluster has to
	// agree to that -- but we need the address book to be able to answer the
	// leader when it starts replicating to us.
	members = append(members, raft.Member{
		ID: cfg.ID, Addr: cfg.RaftAddr, ClientAddr: cfg.HTTPAddr, Learner: true,
	})
	cfg.Logger.Info("discovered cluster membership",
		"id", uint64(cfg.ID), "via", cfg.JoinTarget, "members", len(members)-1)
	return members, nil
}

// announceJoin asks the cluster to add this server as a learner. Any member
// will do: a follower answers with a redirect to the leader, which the client
// follows.
func announceJoin(cfg Config, httpAddr, raftAddr string) error {
	body, err := json.Marshal(map[string]any{
		"id":          uint64(cfg.ID),
		"addr":        raftAddr,
		"client_addr": httpAddr,
		"voting":      false,
	})
	if err != nil {
		return fmt.Errorf("server: encoding the join request: %w", err)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post("http://"+cfg.JoinTarget+"/members", "application/json", bytesReader(body))
	if err != nil {
		return fmt.Errorf("server: joining via %s: %w", cfg.JoinTarget, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server: %s refused the join request: %s", cfg.JoinTarget, resp.Status)
	}
	cfg.Logger.Info("joined the cluster as a learner",
		"id", uint64(cfg.ID), "via", cfg.JoinTarget)
	return nil
}

// bytesReader keeps the import list honest: net/http wants an io.Reader and
// this is the only place a byte slice becomes one.
func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }
