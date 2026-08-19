package transport

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/sahilkalgutkar/raftlite/internal/raft"
	"github.com/sahilkalgutkar/raftlite/internal/wire"
)

const (
	defaultQueueSize   = 256
	defaultDialTimeout = 2 * time.Second
	minReconnectDelay  = 50 * time.Millisecond
	maxReconnectDelay  = 2 * time.Second
)

// Options configures a TCP transport.
type Options struct {
	ID   raft.NodeID
	Addr string // listen address, ":0" picks a free port

	// QueueSize bounds the per-peer outbound queue. It is a bound on how far
	// behind a slow peer may fall before its messages start being dropped, and
	// dropping is the correct behaviour: Raft retries, and blocking the
	// consensus loop on a slow socket is a far worse failure.
	QueueSize   int
	DialTimeout time.Duration
	Logger      *slog.Logger
}

// TCP is a Transport backed by one listener and one outbound connection per
// peer.
//
// Connections are one-directional on purpose: a node dials its peers to send,
// and accepted connections are only ever read from. Two nodes therefore hold
// two connections between them instead of agreeing on one, which costs a
// socket and saves an entire handshake protocol with its own tie-break rules
// and failure modes.
type TCP struct {
	id          raft.NodeID
	listener    net.Listener
	handler     Handler
	logger      *slog.Logger
	queueSize   int
	dialTimeout time.Duration

	mu    sync.Mutex
	peers map[raft.NodeID]*peer
	// conns tracks accepted connections so shutdown can close them. Without
	// this, Close waits forever on reader goroutines that are blocked on
	// sockets only the remote end can close -- a node that cannot restart
	// because a peer is holding it open.
	conns  map[net.Conn]struct{}
	closed bool

	inbound sync.WaitGroup
	stats   Stats
}

// Listen starts a transport and begins accepting connections.
func Listen(opts Options, handler Handler) (*TCP, error) {
	if handler == nil {
		return nil, errors.New("transport: a handler is required")
	}
	if opts.QueueSize <= 0 {
		opts.QueueSize = defaultQueueSize
	}
	if opts.DialTimeout <= 0 {
		opts.DialTimeout = defaultDialTimeout
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}

	ln, err := net.Listen("tcp", opts.Addr)
	if err != nil {
		return nil, fmt.Errorf("transport: listen on %s: %w", opts.Addr, err)
	}

	t := &TCP{
		id:          opts.ID,
		listener:    ln,
		handler:     handler,
		logger:      opts.Logger,
		queueSize:   opts.QueueSize,
		dialTimeout: opts.DialTimeout,
		peers:       make(map[raft.NodeID]*peer),
		conns:       make(map[net.Conn]struct{}),
	}
	t.inbound.Add(1)
	go t.accept()
	return t, nil
}

// Addr is the address actually bound, which matters when the caller asked for
// port zero.
func (t *TCP) Addr() string { return t.listener.Addr().String() }

// Stats returns the transport's counters.
func (t *TCP) Stats() StatsSnapshot { return t.stats.Snapshot() }

func (t *TCP) accept() {
	defer t.inbound.Done()
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			t.mu.Lock()
			closed := t.closed
			t.mu.Unlock()
			if !closed {
				t.logger.Error("accept failed", "id", uint64(t.id), "err", err)
			}
			return
		}
		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			_ = conn.Close()
			return
		}
		t.conns[conn] = struct{}{}
		t.mu.Unlock()

		t.stats.Connections.Add(1)
		t.inbound.Add(1)
		go t.serve(conn)
	}
}

// serve reads framed messages off one inbound connection until it dies.
func (t *TCP) serve(conn net.Conn) {
	defer t.inbound.Done()
	defer t.stats.Connections.Add(-1)
	defer conn.Close()
	defer func() {
		t.mu.Lock()
		delete(t.conns, conn)
		t.mu.Unlock()
	}()

	for {
		payload, err := wire.ReadFrame(conn, 0)
		if err != nil {
			// A peer hanging up is routine, and so is a node restarting. Only
			// a frame that failed to parse is worth complaining about, since
			// that means something on the wire is genuinely wrong.
			if !isDisconnect(err) {
				t.logger.Warn("dropping a connection after a bad frame",
					"id", uint64(t.id), "remote", conn.RemoteAddr().String(), "err", err)
			}
			return
		}
		msg, err := wire.DecodeMessage(payload)
		if err != nil {
			t.logger.Warn("undecodable message", "id", uint64(t.id), "err", err)
			return
		}
		t.stats.Received.Add(1)
		t.handler(msg)
	}
}

// SetPeers reconciles the address book with the cluster configuration:
// new members get a queue and a sender, departed ones are disconnected.
func (t *TCP) SetPeers(members []raft.Member) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}

	wanted := make(map[raft.NodeID]bool, len(members))
	for _, m := range members {
		if m.ID == t.id {
			continue // no need to talk to ourselves
		}
		wanted[m.ID] = true

		if existing, ok := t.peers[m.ID]; ok {
			if existing.addr == m.Addr {
				continue
			}
			// A member that moved: tear the old connection down so the next
			// message dials the new address.
			existing.stop()
			delete(t.peers, m.ID)
		}
		p := newPeer(m.ID, m.Addr, t)
		t.peers[m.ID] = p
		go p.run()
	}

	for id, p := range t.peers {
		if !wanted[id] {
			p.stop()
			delete(t.peers, id)
			t.logger.Info("disconnected a departed member", "id", uint64(t.id), "peer", uint64(id))
		}
	}
}

// Send queues a message for delivery. It never blocks: if a peer's queue is
// full, the message is dropped and counted. That is the right trade because
// the algorithm retries on its own schedule, whereas a blocked send would stop
// the node from making progress with anyone else.
func (t *TCP) Send(msg raft.Message) {
	t.mu.Lock()
	p, ok := t.peers[msg.To]
	closed := t.closed
	t.mu.Unlock()

	if closed || !ok {
		t.stats.Dropped.Add(1)
		return
	}
	select {
	case p.out <- msg:
		t.stats.Sent.Add(1)
	default:
		t.stats.Dropped.Add(1)
		t.logger.Debug("dropped a message: peer queue is full",
			"id", uint64(t.id), "peer", uint64(msg.To), "type", msg.Type.String())
	}
}

// Close stops the listener, disconnects every peer, and waits for the
// goroutines it started.
func (t *TCP) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	peers := make([]*peer, 0, len(t.peers))
	for id, p := range t.peers {
		peers = append(peers, p)
		delete(t.peers, id)
	}
	conns := make([]net.Conn, 0, len(t.conns))
	for c := range t.conns {
		conns = append(conns, c)
	}
	t.mu.Unlock()

	err := t.listener.Close()
	for _, p := range peers {
		p.stop()
	}
	// Closing an accepted connection is what unblocks its reader goroutine.
	for _, c := range conns {
		_ = c.Close()
	}
	t.inbound.Wait()
	if err != nil {
		return fmt.Errorf("transport: close listener: %w", err)
	}
	return nil
}
