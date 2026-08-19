package raft

import (
	"errors"
	"testing"
)

func threeVoters() Config {
	return NewConfig(
		Member{ID: 3, Addr: "127.0.0.1:9003"},
		Member{ID: 1, Addr: "127.0.0.1:9001"},
		Member{ID: 2, Addr: "127.0.0.1:9002"},
	)
}

func TestConfigNormalisesOrder(t *testing.T) {
	c := threeVoters()
	for i, m := range c.Members {
		if m.ID != NodeID(i+1) {
			t.Fatalf("members not sorted by ID: %v", c)
		}
	}
}

func TestConfigQuorum(t *testing.T) {
	cases := []struct {
		voters, learners, quorum int
	}{
		{1, 0, 1},
		{2, 0, 2},
		{3, 0, 2},
		{4, 0, 3},
		{5, 0, 3},
		{3, 2, 2}, // learners never move the quorum
	}
	for _, tc := range cases {
		c := Config{}
		id := NodeID(1)
		for i := 0; i < tc.voters; i++ {
			c = c.With(Member{ID: id})
			id++
		}
		for i := 0; i < tc.learners; i++ {
			c = c.With(Member{ID: id, Learner: true})
			id++
		}
		if got := c.Quorum(); got != tc.quorum {
			t.Fatalf("%d voters + %d learners: quorum = %d, want %d", tc.voters, tc.learners, got, tc.quorum)
		}
		if len(c.Voters()) != tc.voters || len(c.Learners()) != tc.learners {
			t.Fatalf("voter/learner split wrong for %v", c)
		}
	}
}

func TestConfigLookup(t *testing.T) {
	c := threeVoters().With(Member{ID: 4, Addr: "127.0.0.1:9004", Learner: true})

	if !c.Has(4) || !c.Has(1) || c.Has(99) {
		t.Fatalf("Has is wrong for %v", c)
	}
	if !c.IsVoter(1) || c.IsVoter(4) || c.IsVoter(99) {
		t.Fatalf("IsVoter is wrong for %v", c)
	}
	m, ok := c.Member(4)
	if !ok || !m.Learner || m.Addr != "127.0.0.1:9004" {
		t.Fatalf("Member(4) = %+v, %v", m, ok)
	}
	if _, ok := c.Member(99); ok {
		t.Fatal("Member(99) should not resolve")
	}
	if c.String() == "" {
		t.Fatal("String() is empty")
	}
}

func TestConfigWithReplacesInPlace(t *testing.T) {
	c := threeVoters()
	next := c.With(Member{ID: 2, Addr: "10.0.0.2:9002"})
	if len(next.Members) != 3 {
		t.Fatalf("replacement grew the config: %v", next)
	}
	if m, _ := next.Member(2); m.Addr != "10.0.0.2:9002" {
		t.Fatalf("address not replaced: %+v", m)
	}
	if m, _ := c.Member(2); m.Addr != "127.0.0.1:9002" {
		t.Fatal("With mutated the original config")
	}
}

func TestConfigWithoutIsACopy(t *testing.T) {
	c := threeVoters()
	next := c.Without(2)
	if next.Has(2) || len(next.Members) != 2 {
		t.Fatalf("Without(2) = %v", next)
	}
	if !c.Has(2) {
		t.Fatal("Without mutated the original config")
	}
	if c.Without(99).Equal(c) != true {
		t.Fatal("removing an absent member should be a no-op")
	}
}

func TestConfigEqual(t *testing.T) {
	a := threeVoters()
	if !a.Equal(a.Clone()) {
		t.Fatal("clone is not equal to the original")
	}
	if a.Equal(a.Without(1)) {
		t.Fatal("different sizes compared equal")
	}
	if a.Equal(a.With(Member{ID: 1, Addr: "changed"})) {
		t.Fatal("different addresses compared equal")
	}
}

func TestConfigRoundTrip(t *testing.T) {
	c := threeVoters().With(Member{ID: 7, Addr: "host.internal:1234", Learner: true})
	got, err := UnmarshalConfig(c.Marshal())
	if err != nil {
		t.Fatalf("UnmarshalConfig: %v", err)
	}
	if !got.Equal(c) {
		t.Fatalf("round trip: %v != %v", got, c)
	}
	// Encoding must be stable: the same config always produces the same bytes.
	if string(c.Marshal()) != string(got.Marshal()) {
		t.Fatal("encoding is not deterministic")
	}
}

func TestConfigRoundTripEmpty(t *testing.T) {
	got, err := UnmarshalConfig(Config{}.Marshal())
	if err != nil {
		t.Fatalf("UnmarshalConfig: %v", err)
	}
	if len(got.Members) != 0 {
		t.Fatalf("empty config decoded to %v", got)
	}
}

func TestUnmarshalConfigRejectsTruncation(t *testing.T) {
	full := threeVoters().Marshal()
	for cut := 0; cut < len(full); cut++ {
		if _, err := UnmarshalConfig(full[:cut]); err == nil {
			t.Fatalf("truncating to %d bytes decoded cleanly", cut)
		}
	}
}

func TestConfChangeRoundTrip(t *testing.T) {
	for _, cc := range []ConfChange{
		{Type: ConfChangeAddVoter, ID: 4, Addr: "127.0.0.1:9004"},
		{Type: ConfChangeAddLearner, ID: 5, Addr: "127.0.0.1:9005"},
		{Type: ConfChangePromote, ID: 5},
		{Type: ConfChangeRemove, ID: 2},
	} {
		got, err := UnmarshalConfChange(cc.Marshal())
		if err != nil {
			t.Fatalf("UnmarshalConfChange(%v): %v", cc, err)
		}
		if got != cc {
			t.Fatalf("round trip: %v != %v", got, cc)
		}
		if cc.String() == "" || cc.Type.String() == "" {
			t.Fatalf("%v has no string form", cc)
		}
	}
	if got := ConfChangeType(200).String(); got == "" {
		t.Fatal("unknown conf change type has no string form")
	}
}

func TestUnmarshalConfChangeRejectsTruncation(t *testing.T) {
	full := ConfChange{Type: ConfChangeAddVoter, ID: 4, Addr: "127.0.0.1:9004"}.Marshal()
	for cut := 0; cut < len(full); cut++ {
		if _, err := UnmarshalConfChange(full[:cut]); err == nil {
			t.Fatalf("truncating to %d bytes decoded cleanly", cut)
		}
	}
}

func TestConfigApply(t *testing.T) {
	base := threeVoters()

	added, err := base.Apply(ConfChange{Type: ConfChangeAddVoter, ID: 4, Addr: "a"})
	if err != nil || !added.IsVoter(4) {
		t.Fatalf("add voter: %v, %v", added, err)
	}

	learner, err := base.Apply(ConfChange{Type: ConfChangeAddLearner, ID: 5, Addr: "b"})
	if err != nil || learner.IsVoter(5) || !learner.Has(5) {
		t.Fatalf("add learner: %v, %v", learner, err)
	}

	promoted, err := learner.Apply(ConfChange{Type: ConfChangePromote, ID: 5})
	if err != nil || !promoted.IsVoter(5) {
		t.Fatalf("promote: %v, %v", promoted, err)
	}
	if m, _ := promoted.Member(5); m.Addr != "b" {
		t.Fatalf("promotion dropped the address: %+v", m)
	}

	readdressed, err := learner.Apply(ConfChange{Type: ConfChangePromote, ID: 5, Addr: "c"})
	if err != nil {
		t.Fatalf("promote with new address: %v", err)
	}
	if m, _ := readdressed.Member(5); m.Addr != "c" {
		t.Fatalf("promotion ignored the new address: %+v", m)
	}

	removed, err := base.Apply(ConfChange{Type: ConfChangeRemove, ID: 3})
	if err != nil || removed.Has(3) {
		t.Fatalf("remove: %v, %v", removed, err)
	}
}

func TestConfigApplyRejectsUnsafeChanges(t *testing.T) {
	base := threeVoters()

	if _, err := base.Apply(ConfChange{Type: ConfChangeAddLearner, ID: 1}); !errors.Is(err, ErrInvalidConfChange) {
		t.Fatalf("demoting a voter to learner = %v", err)
	}
	if _, err := base.Apply(ConfChange{Type: ConfChangePromote, ID: 99}); !errors.Is(err, ErrInvalidConfChange) {
		t.Fatalf("promoting a non-member = %v", err)
	}
	if _, err := base.Apply(ConfChange{Type: ConfChangeRemove, ID: 99}); !errors.Is(err, ErrInvalidConfChange) {
		t.Fatalf("removing a non-member = %v", err)
	}
	if _, err := base.Apply(ConfChange{Type: ConfChangeType(9), ID: 1}); !errors.Is(err, ErrInvalidConfChange) {
		t.Fatalf("unknown change type = %v", err)
	}

	single := NewConfig(Member{ID: 1}, Member{ID: 2, Learner: true})
	if _, err := single.Apply(ConfChange{Type: ConfChangeRemove, ID: 1}); !errors.Is(err, ErrInvalidConfChange) {
		t.Fatalf("removing the last voter = %v, want rejection", err)
	}
}

func TestConfigCarriesBothAddresses(t *testing.T) {
	// Peer traffic and client traffic are different things and may live on
	// different ports, so both addresses are replicated: every node has to
	// know where the leader answers clients in order to redirect them there.
	c := NewConfig(
		Member{ID: 1, Addr: "10.0.0.1:9001", ClientAddr: "10.0.0.1:8001"},
		Member{ID: 2, Addr: "10.0.0.2:9001", ClientAddr: "10.0.0.2:8001", Learner: true},
	)
	got, err := UnmarshalConfig(c.Marshal())
	if err != nil {
		t.Fatalf("UnmarshalConfig: %v", err)
	}
	if !got.Equal(c) {
		t.Fatalf("round trip: %v != %v", got, c)
	}
	m, _ := got.Member(1)
	if m.ClientAddr != "10.0.0.1:8001" {
		t.Fatalf("client address = %q", m.ClientAddr)
	}
}

func TestConfChangeCarriesBothAddresses(t *testing.T) {
	cc := ConfChange{Type: ConfChangeAddVoter, ID: 4, Addr: "10.0.0.4:9001", ClientAddr: "10.0.0.4:8001"}
	got, err := UnmarshalConfChange(cc.Marshal())
	if err != nil {
		t.Fatalf("UnmarshalConfChange: %v", err)
	}
	if got != cc {
		t.Fatalf("round trip: %+v != %+v", got, cc)
	}

	applied, err := Config{}.Apply(cc)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	m, _ := applied.Member(4)
	if m.ClientAddr != "10.0.0.4:8001" {
		t.Fatalf("applied member = %+v", m)
	}

	// Promotion can also update the addresses.
	promoted, err := applied.Apply(ConfChange{Type: ConfChangePromote, ID: 4, ClientAddr: "10.0.0.4:8002"})
	if err != nil {
		t.Fatalf("Apply promote: %v", err)
	}
	if m, _ := promoted.Member(4); m.ClientAddr != "10.0.0.4:8002" {
		t.Fatalf("promotion did not update the client address: %+v", m)
	}
}
