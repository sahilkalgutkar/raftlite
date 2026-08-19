package transport

import (
	"sync"
	"testing"

	"github.com/sahilkalgutkar/raftlite/internal/raft"
)

func meshNode(t *testing.T, m *Mesh, id raft.NodeID) (Transport, *inbox) {
	t.Helper()
	box := &inbox{}
	tr, err := m.Factory(id, "mem://x")(box.handler())
	if err != nil {
		t.Fatalf("Factory: %v", err)
	}
	return tr, box
}

func TestMeshDeliversAndPartitions(t *testing.T) {
	m := NewMesh()
	a, _ := meshNode(t, m, 1)
	_, boxB := meshNode(t, m, 2)
	_, boxC := meshNode(t, m, 3)

	a.SetPeers([]raft.Member{{ID: 2}, {ID: 3}}) // a mesh has no address book
	a.Send(raft.Message{Type: raft.MsgHeartbeatReq, From: 1, To: 2})
	if boxB.len() != 1 {
		t.Fatalf("b received %d messages", boxB.len())
	}
	if a.Addr() != "mem://x" {
		t.Fatalf("addr = %q", a.Addr())
	}

	// Split 1 from 2 and 3: messages across the cut vanish.
	m.Partition([]raft.NodeID{1}, []raft.NodeID{2, 3})
	a.Send(raft.Message{Type: raft.MsgHeartbeatReq, From: 1, To: 2})
	a.Send(raft.Message{Type: raft.MsgHeartbeatReq, From: 1, To: 3})
	if boxB.len() != 1 || boxC.len() != 0 {
		t.Fatalf("a partition leaked messages: b=%d c=%d", boxB.len(), boxC.len())
	}

	m.Heal()
	a.Send(raft.Message{Type: raft.MsgHeartbeatReq, From: 1, To: 3})
	if boxC.len() != 1 {
		t.Fatalf("c received %d messages after healing", boxC.len())
	}

	delivered, dropped := m.Stats()
	if delivered != 2 || dropped != 2 {
		t.Fatalf("stats = %d delivered, %d dropped", delivered, dropped)
	}
}

func TestMeshIsolateAndClose(t *testing.T) {
	m := NewMesh()
	a, _ := meshNode(t, m, 1)
	_, boxB := meshNode(t, m, 2)

	m.Isolate(1)
	a.Send(raft.Message{Type: raft.MsgHeartbeatReq, From: 1, To: 2})
	if boxB.len() != 0 {
		t.Fatal("an isolated node still delivered")
	}
	m.Heal()

	b := m.members[2]
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	a.Send(raft.Message{Type: raft.MsgHeartbeatReq, From: 1, To: 2})
	if boxB.len() != 0 {
		t.Fatal("a closed node still received messages")
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	a.Send(raft.Message{Type: raft.MsgHeartbeatReq, From: 1, To: 2}) // must not panic
}

func TestMeshIsSafeUnderConcurrency(t *testing.T) {
	m := NewMesh()
	a, _ := meshNode(t, m, 1)
	_, boxB := meshNode(t, m, 2)

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				a.Send(raft.Message{Type: raft.MsgHeartbeatReq, From: 1, To: 2})
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			m.Isolate(1)
			m.Heal()
		}
	}()
	wg.Wait()
	if boxB.len() == 0 {
		t.Fatal("nothing was delivered at all")
	}
}
