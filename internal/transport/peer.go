package transport

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/sahilkalgutkar/raftlite/internal/raft"
	"github.com/sahilkalgutkar/raftlite/internal/wire"
)

// peer is one outbound connection: a queue, a goroutine draining it, and a
// socket that is redialled whenever it breaks.
type peer struct {
	id   raft.NodeID
	addr string
	t    *TCP

	out  chan raft.Message
	done chan struct{}
	once sync.Once

	mu   sync.Mutex
	conn net.Conn
}

func newPeer(id raft.NodeID, addr string, t *TCP) *peer {
	return &peer{
		id:   id,
		addr: addr,
		t:    t,
		out:  make(chan raft.Message, t.queueSize),
		done: make(chan struct{}),
	}
}

// run drains the queue forever, reconnecting as needed.
func (p *peer) run() {
	delay := minReconnectDelay
	for {
		select {
		case <-p.done:
			p.closeConn()
			return
		case msg := <-p.out:
			if err := p.deliver(msg); err != nil {
				p.closeConn()
				// Back off before trying again. A peer that is genuinely down
				// would otherwise get a fresh connection attempt for every
				// heartbeat, which is a lot of syscalls to no purpose.
				select {
				case <-p.done:
					return
				case <-time.After(delay):
				}
				if delay < maxReconnectDelay {
					delay *= 2
				}
				continue
			}
			delay = minReconnectDelay
		}
	}
}

func (p *peer) deliver(msg raft.Message) error {
	conn, err := p.connect()
	if err != nil {
		p.t.stats.DialFailed.Add(1)
		p.t.logger.Debug("dial failed", "id", uint64(p.t.id), "peer", uint64(p.id), "addr", p.addr, "err", err)
		return err
	}
	return wire.WriteFrame(conn, wire.EncodeMessage(msg))
}

func (p *peer) connect() (net.Conn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn != nil {
		return p.conn, nil
	}
	conn, err := net.DialTimeout("tcp", p.addr, p.t.dialTimeout)
	if err != nil {
		return nil, err
	}
	// Consensus messages are small and latency-sensitive; waiting to coalesce
	// them into a bigger packet is exactly the wrong trade for a heartbeat.
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	p.conn = conn
	p.t.logger.Debug("connected to peer", "id", uint64(p.t.id), "peer", uint64(p.id), "addr", p.addr)
	return conn, nil
}

func (p *peer) closeConn() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn != nil {
		_ = p.conn.Close()
		p.conn = nil
	}
}

// stop shuts the peer down. It is safe to call more than once, which matters
// because both Close and SetPeers can retire the same peer.
func (p *peer) stop() {
	p.once.Do(func() { close(p.done) })
}

// isDisconnect reports whether an error is just the other end going away, as
// opposed to something worth logging.
func isDisconnect(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	return errors.Is(err, io.ErrUnexpectedEOF)
}
