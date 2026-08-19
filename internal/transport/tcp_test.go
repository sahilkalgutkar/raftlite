package transport

import (
	"bytes"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sahilkalgutkar/raftlite/internal/raft"
	"github.com/sahilkalgutkar/raftlite/internal/wire"
)

// inbox collects the messages a transport received.
type inbox struct {
	mu   sync.Mutex
	msgs []raft.Message
}

func (i *inbox) handler() Handler {
	return func(m raft.Message) {
		i.mu.Lock()
		defer i.mu.Unlock()
		i.msgs = append(i.msgs, m)
	}
}

func (i *inbox) len() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return len(i.msgs)
}

func (i *inbox) all() []raft.Message {
	i.mu.Lock()
	defer i.mu.Unlock()
	return append([]raft.Message(nil), i.msgs...)
}

// waitFor polls until cond holds. Delivery is asynchronous by design, so tests
// wait on the observable outcome rather than sleeping a guessed interval.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func startTransport(t *testing.T, id raft.NodeID) (*TCP, *inbox) {
	t.Helper()
	box := &inbox{}
	tr, err := Listen(Options{ID: id, Addr: "127.0.0.1:0"}, box.handler())
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	return tr, box
}

func member(id raft.NodeID, tr *TCP) raft.Member {
	return raft.Member{ID: id, Addr: tr.Addr()}
}

func TestTwoNodesExchangeMessages(t *testing.T) {
	a, boxA := startTransport(t, 1)
	b, boxB := startTransport(t, 2)

	members := []raft.Member{member(1, a), member(2, b)}
	a.SetPeers(members)
	b.SetPeers(members)

	a.Send(raft.Message{Type: raft.MsgHeartbeatReq, From: 1, To: 2, Term: 4, Commit: 9})
	waitFor(t, "b to receive a heartbeat", func() bool { return boxB.len() == 1 })

	got := boxB.all()[0]
	if got.Type != raft.MsgHeartbeatReq || got.From != 1 || got.Term != 4 || got.Commit != 9 {
		t.Fatalf("received %+v", got)
	}

	b.Send(raft.Message{Type: raft.MsgHeartbeatResp, From: 2, To: 1, Term: 4})
	waitFor(t, "a to receive the reply", func() bool { return boxA.len() == 1 })

	if s := a.Stats(); s.Sent != 1 || s.Received != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestEntriesAndSnapshotsSurviveTheWire(t *testing.T) {
	a, _ := startTransport(t, 1)
	b, boxB := startTransport(t, 2)
	members := []raft.Member{member(1, a), member(2, b)}
	a.SetPeers(members)
	b.SetPeers(members)

	big := bytes.Repeat([]byte("state"), 200000) // a megabyte of snapshot
	a.Send(raft.Message{
		Type: raft.MsgSnapshotReq, From: 1, To: 2, Term: 3,
		Snapshot: &raft.Snapshot{
			Meta: raft.SnapshotMeta{Index: 900, Term: 3, Config: raft.NewConfig(raft.Member{ID: 1, Addr: "x"})},
			Data: big,
		},
	})
	waitFor(t, "the snapshot to arrive", func() bool { return boxB.len() == 1 })

	got := boxB.all()[0]
	if got.Snapshot == nil || !bytes.Equal(got.Snapshot.Data, big) {
		t.Fatal("snapshot did not survive the wire intact")
	}
	if got.Snapshot.Meta.Index != 900 {
		t.Fatalf("snapshot meta = %+v", got.Snapshot.Meta)
	}
}

func TestManyMessagesArriveInOrder(t *testing.T) {
	a, _ := startTransport(t, 1)
	b, boxB := startTransport(t, 2)
	members := []raft.Member{member(1, a), member(2, b)}
	a.SetPeers(members)
	b.SetPeers(members)

	const n = 200
	for i := 0; i < n; i++ {
		a.Send(raft.Message{Type: raft.MsgAppendReq, From: 1, To: 2, Term: 1, PrevLogIndex: uint64(i)})
	}
	waitFor(t, "every message to arrive", func() bool { return boxB.len() == n })

	// One connection, one sender goroutine: a single stream must stay ordered
	// even though delivery itself is best effort.
	for i, m := range boxB.all() {
		if m.PrevLogIndex != uint64(i) {
			t.Fatalf("message %d arrived out of order: %+v", i, m)
		}
	}
}

func TestPeerThatComesBackStartsReceivingAgain(t *testing.T) {
	a, _ := startTransport(t, 1)

	// Reserve a port, then close the listener so nothing is there yet.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	a.SetPeers([]raft.Member{{ID: 1, Addr: a.Addr()}, {ID: 2, Addr: addr}})
	a.Send(raft.Message{Type: raft.MsgHeartbeatReq, From: 1, To: 2, Term: 1})
	waitFor(t, "the dial to fail", func() bool { return a.Stats().DialFailed > 0 })

	// Now bring the peer up on the address the sender is already retrying.
	box := &inbox{}
	b, err := Listen(Options{ID: 2, Addr: addr}, box.handler())
	if err != nil {
		t.Fatalf("Listen on the reserved port: %v", err)
	}
	defer b.Close()

	waitFor(t, "delivery to resume", func() bool {
		a.Send(raft.Message{Type: raft.MsgHeartbeatReq, From: 1, To: 2, Term: 2})
		return box.len() > 0
	})
}

func TestAFullQueueDropsRatherThanBlocking(t *testing.T) {
	// One unreachable follower must never stall the consensus loop. With a
	// tiny queue and a peer that does not exist, Send has to return promptly
	// and count the loss.
	box := &inbox{}
	tr, err := Listen(Options{ID: 1, Addr: "127.0.0.1:0", QueueSize: 1, DialTimeout: 50 * time.Millisecond}, box.handler())
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer tr.Close()

	// 127.0.0.1:1 is reserved and refuses connections immediately.
	tr.SetPeers([]raft.Member{{ID: 1, Addr: tr.Addr()}, {ID: 2, Addr: "127.0.0.1:1"}})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			tr.Send(raft.Message{Type: raft.MsgHeartbeatReq, From: 1, To: 2, Term: uint64(i)})
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Send blocked on an unreachable peer")
	}
	if tr.Stats().Dropped == 0 {
		t.Fatal("nothing was counted as dropped")
	}
}

func TestSendToAnUnknownPeerIsDropped(t *testing.T) {
	tr, _ := startTransport(t, 1)
	tr.Send(raft.Message{Type: raft.MsgHeartbeatReq, From: 1, To: 99})
	if tr.Stats().Dropped != 1 {
		t.Fatalf("dropped = %d, want 1", tr.Stats().Dropped)
	}
}

func TestSetPeersRemovesDepartedMembers(t *testing.T) {
	a, _ := startTransport(t, 1)
	b, boxB := startTransport(t, 2)

	a.SetPeers([]raft.Member{member(1, a), member(2, b)})
	a.Send(raft.Message{Type: raft.MsgHeartbeatReq, From: 1, To: 2})
	waitFor(t, "the first message", func() bool { return boxB.len() == 1 })

	// Node 2 leaves the cluster.
	a.SetPeers([]raft.Member{member(1, a)})
	a.Send(raft.Message{Type: raft.MsgHeartbeatReq, From: 1, To: 2})

	time.Sleep(50 * time.Millisecond)
	if boxB.len() != 1 {
		t.Fatalf("a removed member is still receiving messages: %d", boxB.len())
	}
}

func TestSetPeersFollowsAnAddressChange(t *testing.T) {
	a, _ := startTransport(t, 1)
	oldB, oldBox := startTransport(t, 2)
	newB, newBox := startTransport(t, 2)

	a.SetPeers([]raft.Member{member(1, a), {ID: 2, Addr: oldB.Addr()}})
	a.Send(raft.Message{Type: raft.MsgHeartbeatReq, From: 1, To: 2, Term: 1})
	waitFor(t, "delivery to the old address", func() bool { return oldBox.len() == 1 })

	// The member moved. The next message has to follow it.
	a.SetPeers([]raft.Member{member(1, a), {ID: 2, Addr: newB.Addr()}})
	a.Send(raft.Message{Type: raft.MsgHeartbeatReq, From: 1, To: 2, Term: 2})
	waitFor(t, "delivery to the new address", func() bool { return newBox.len() == 1 })

	if oldBox.len() != 1 {
		t.Fatalf("the old address kept receiving: %d messages", oldBox.len())
	}
}

func TestSetPeersIsIdempotent(t *testing.T) {
	a, _ := startTransport(t, 1)
	b, boxB := startTransport(t, 2)
	members := []raft.Member{member(1, a), member(2, b)}

	for i := 0; i < 5; i++ {
		a.SetPeers(members)
	}
	a.Send(raft.Message{Type: raft.MsgHeartbeatReq, From: 1, To: 2})
	waitFor(t, "delivery after repeated peer updates", func() bool { return boxB.len() == 1 })
}

func TestGarbageOnTheWireClosesOnlyThatConnection(t *testing.T) {
	a, boxA := startTransport(t, 1)

	conn, err := net.Dial("tcp", a.Addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if _, err := conn.Write([]byte("this is not a raftlite frame at all")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	waitFor(t, "the bad connection to be dropped", func() bool { return a.Stats().Connections == 0 })
	_ = conn.Close()

	// The listener must still be serving everyone else.
	b, _ := startTransport(t, 2)
	b.SetPeers([]raft.Member{member(1, a), member(2, b)})
	b.Send(raft.Message{Type: raft.MsgHeartbeatReq, From: 2, To: 1})
	waitFor(t, "a healthy peer to still get through", func() bool { return boxA.len() == 1 })
}

func TestAFrameThatDecodesToNothingClosesTheConnection(t *testing.T) {
	a, _ := startTransport(t, 1)

	conn, err := net.Dial("tcp", a.Addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	// A well-formed frame whose payload is not a message.
	if err := wire.WriteFrame(conn, []byte{0x00}); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	waitFor(t, "the connection to be dropped", func() bool { return a.Stats().Connections == 0 })
}

func TestConcurrentSendsAreSafe(t *testing.T) {
	a, _ := startTransport(t, 1)
	b, boxB := startTransport(t, 2)
	members := []raft.Member{member(1, a), member(2, b)}
	a.SetPeers(members)
	b.SetPeers(members)

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				a.Send(raft.Message{Type: raft.MsgHeartbeatReq, From: 1, To: 2, Term: uint64(w)})
				if i%10 == 0 {
					a.SetPeers(members)
				}
			}
		}(w)
	}
	wg.Wait()
	waitFor(t, "most messages to land", func() bool { return boxB.len() > 100 })
}

func TestCloseIsIdempotentAndStopsDelivery(t *testing.T) {
	a, _ := startTransport(t, 1)
	b, boxB := startTransport(t, 2)
	a.SetPeers([]raft.Member{member(1, a), member(2, b)})

	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	a.Send(raft.Message{Type: raft.MsgHeartbeatReq, From: 1, To: 2})
	a.SetPeers([]raft.Member{member(1, a), member(2, b)}) // must not resurrect anything
	time.Sleep(50 * time.Millisecond)
	if boxB.len() != 0 {
		t.Fatalf("a closed transport delivered %d messages", boxB.len())
	}
}

func TestListenValidatesItsArguments(t *testing.T) {
	if _, err := Listen(Options{ID: 1, Addr: "127.0.0.1:0"}, nil); err == nil {
		t.Fatal("Listen accepted a nil handler")
	}
	if _, err := Listen(Options{ID: 1, Addr: "not-an-address"}, func(raft.Message) {}); err == nil {
		t.Fatal("Listen accepted an unusable address")
	}
}

func TestStatsSnapshotIsAPlainCopy(t *testing.T) {
	var s Stats
	s.Sent.Add(3)
	s.Dropped.Add(1)
	s.Received.Add(7)
	s.DialFailed.Add(2)
	s.Connections.Add(1)

	got := s.Snapshot()
	if got.Sent != 3 || got.Dropped != 1 || got.Received != 7 || got.DialFailed != 2 || got.Connections != 1 {
		t.Fatalf("snapshot = %+v", got)
	}
	s.Sent.Add(10)
	if got.Sent != 3 {
		t.Fatal("the snapshot tracked later changes")
	}
}

func TestTransportSatisfiesTheInterface(t *testing.T) {
	var _ Transport = (*TCP)(nil)
}
