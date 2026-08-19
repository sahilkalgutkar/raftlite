package transport

import (
	"sync"
	"sync/atomic"

	"github.com/sahilkalgutkar/raftlite/internal/raft"
)

// Mesh is an in-memory network: a whole cluster inside one process, with
// partitions the caller controls.
//
// It exists so cluster-level behaviour -- an election after a leader dies, a
// minority that cannot commit, a partition that heals -- can be tested without
// sockets, ports or timing luck. The real TCP transport is exercised on its
// own; what needs testing here is the cluster, not the network stack.
type Mesh struct {
	mu      sync.RWMutex
	members map[raft.NodeID]*memTransport
	group   map[raft.NodeID]int

	delivered atomic.Uint64
	dropped   atomic.Uint64
}

// NewMesh returns an empty mesh.
func NewMesh() *Mesh {
	return &Mesh{
		members: make(map[raft.NodeID]*memTransport),
		group:   make(map[raft.NodeID]int),
	}
}

// Factory returns a constructor for one node's transport, in the shape the
// node runtime expects.
func (m *Mesh) Factory(id raft.NodeID, addr string) func(Handler) (Transport, error) {
	return func(h Handler) (Transport, error) {
		t := &memTransport{id: id, addr: addr, mesh: m, handler: h}
		m.mu.Lock()
		defer m.mu.Unlock()
		m.members[id] = t
		m.group[id] = 0
		return t, nil
	}
}

// Isolate cuts each named node off from every other node.
func (m *Mesh) Isolate(ids ...raft.NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	next := 1
	for _, g := range m.group {
		if g >= next {
			next = g + 1
		}
	}
	for _, id := range ids {
		m.group[id] = next
		next++
	}
}

// Partition splits the cluster into two groups that cannot reach each other.
func (m *Mesh) Partition(a, b []raft.NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range a {
		m.group[id] = 1
	}
	for _, id := range b {
		m.group[id] = 2
	}
}

// Heal puts every node back on the same network.
func (m *Mesh) Heal() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id := range m.group {
		m.group[id] = 0
	}
}

// Stats reports delivered and dropped message counts.
func (m *Mesh) Stats() (delivered, dropped uint64) {
	return m.delivered.Load(), m.dropped.Load()
}

func (m *Mesh) deliver(from raft.NodeID, msg raft.Message) {
	m.mu.RLock()
	target, ok := m.members[msg.To]
	reachable := ok && m.group[from] == m.group[msg.To]
	m.mu.RUnlock()

	if !reachable || target.isClosed() {
		m.dropped.Add(1)
		return
	}
	m.delivered.Add(1)
	target.handler(msg)
}

type memTransport struct {
	id      raft.NodeID
	addr    string
	mesh    *Mesh
	handler Handler
	closed  atomic.Bool
}

func (t *memTransport) Send(msg raft.Message) {
	if t.closed.Load() {
		return
	}
	t.mesh.deliver(t.id, msg)
}

// SetPeers is a no-op: a mesh addresses nodes by ID, so there is no address
// book to keep up to date.
func (t *memTransport) SetPeers([]raft.Member) {}

func (t *memTransport) Addr() string { return t.addr }

func (t *memTransport) isClosed() bool { return t.closed.Load() }

func (t *memTransport) Close() error {
	t.closed.Store(true)
	t.mesh.mu.Lock()
	defer t.mesh.mu.Unlock()
	delete(t.mesh.members, t.id)
	return nil
}
