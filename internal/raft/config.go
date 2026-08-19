package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// Member is one server in the cluster configuration.
//
// A learner replicates the log and can be caught up to the leader, but it does
// not vote and does not count toward a quorum. That is how a new server joins
// without stalling writes: it streams the log as a learner first, and only
// gets promoted once it is close enough that adding it to the quorum is cheap.
type Member struct {
	ID   NodeID
	Addr string
	// ClientAddr is where clients reach this server, as opposed to Addr which
	// is where its peers do. They are separate because they genuinely are two
	// different things: peer traffic is consensus, client traffic is the API,
	// and a deployment may well want them on different ports, interfaces or
	// networks. It also has to be part of the replicated configuration rather
	// than local knowledge, since redirecting a client to the leader means
	// every node has to know where the leader answers.
	ClientAddr string
	Learner    bool
}

// Config is a cluster configuration. Members are kept sorted by ID so that the
// encoded form is byte-identical on every node -- configurations travel inside
// log entries and snapshots, so a non-deterministic encoding would show up as
// a phantom divergence.
type Config struct {
	Members []Member
}

// NewConfig builds a configuration from a set of members, normalising order.
func NewConfig(members ...Member) Config {
	c := Config{Members: append([]Member(nil), members...)}
	c.normalize()
	return c
}

func (c *Config) normalize() {
	sort.Slice(c.Members, func(i, j int) bool { return c.Members[i].ID < c.Members[j].ID })
}

// Clone returns a deep copy, so callers can mutate a configuration without
// disturbing the one the log committed.
func (c Config) Clone() Config {
	return Config{Members: append([]Member(nil), c.Members...)}
}

// Member looks a member up by ID.
func (c Config) Member(id NodeID) (Member, bool) {
	for _, m := range c.Members {
		if m.ID == id {
			return m, true
		}
	}
	return Member{}, false
}

// Has reports whether the ID is part of the configuration in any capacity.
func (c Config) Has(id NodeID) bool {
	_, ok := c.Member(id)
	return ok
}

// IsVoter reports whether the ID counts toward a quorum.
func (c Config) IsVoter(id NodeID) bool {
	m, ok := c.Member(id)
	return ok && !m.Learner
}

// Voters returns the IDs that count toward a quorum, in ID order.
func (c Config) Voters() []NodeID {
	out := make([]NodeID, 0, len(c.Members))
	for _, m := range c.Members {
		if !m.Learner {
			out = append(out, m.ID)
		}
	}
	return out
}

// Learners returns the non-voting IDs, in ID order.
func (c Config) Learners() []NodeID {
	out := make([]NodeID, 0, len(c.Members))
	for _, m := range c.Members {
		if m.Learner {
			out = append(out, m.ID)
		}
	}
	return out
}

// Quorum is the number of votes needed to decide anything: a strict majority
// of the voters.
func (c Config) Quorum() int { return len(c.Voters())/2 + 1 }

// With returns a copy with the member added, or replaced if the ID is already
// present.
func (c Config) With(m Member) Config {
	next := c.Clone()
	for i := range next.Members {
		if next.Members[i].ID == m.ID {
			next.Members[i] = m
			return next
		}
	}
	next.Members = append(next.Members, m)
	next.normalize()
	return next
}

// Without returns a copy with the given ID removed.
func (c Config) Without(id NodeID) Config {
	next := Config{}
	for _, m := range c.Members {
		if m.ID != id {
			next.Members = append(next.Members, m)
		}
	}
	return next
}

// Equal compares two configurations member by member.
func (c Config) Equal(o Config) bool {
	if len(c.Members) != len(o.Members) {
		return false
	}
	for i := range c.Members {
		if c.Members[i] != o.Members[i] {
			return false
		}
	}
	return true
}

func (c Config) String() string {
	parts := make([]string, 0, len(c.Members))
	for _, m := range c.Members {
		suffix := ""
		if m.Learner {
			suffix = " (learner)"
		}
		parts = append(parts, fmt.Sprintf("%d@%s%s", uint64(m.ID), m.Addr, suffix))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// Marshal encodes the configuration deterministically.
func (c Config) Marshal() []byte {
	buf := make([]byte, 0, 32*len(c.Members)+binary.MaxVarintLen64)
	buf = binary.AppendUvarint(buf, uint64(len(c.Members)))
	for _, m := range c.Members {
		buf = binary.AppendUvarint(buf, uint64(m.ID))
		buf = binary.AppendUvarint(buf, uint64(len(m.Addr)))
		buf = append(buf, m.Addr...)
		buf = binary.AppendUvarint(buf, uint64(len(m.ClientAddr)))
		buf = append(buf, m.ClientAddr...)
		if m.Learner {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	}
	return buf
}

// ErrMalformedConfig is returned when a configuration cannot be decoded.
var ErrMalformedConfig = errors.New("raft: malformed configuration encoding")

// UnmarshalConfig decodes a configuration produced by Config.Marshal.
func UnmarshalConfig(b []byte) (Config, error) {
	count, n := binary.Uvarint(b)
	if n <= 0 {
		return Config{}, ErrMalformedConfig
	}
	b = b[n:]
	cfg := Config{Members: make([]Member, 0, count)}
	for i := uint64(0); i < count; i++ {
		id, n := binary.Uvarint(b)
		if n <= 0 {
			return Config{}, ErrMalformedConfig
		}
		b = b[n:]
		alen, n := binary.Uvarint(b)
		if n <= 0 || uint64(len(b[n:])) < alen {
			return Config{}, ErrMalformedConfig
		}
		b = b[n:]
		addr := string(b[:alen])
		b = b[alen:]

		clen, n := binary.Uvarint(b)
		if n <= 0 || uint64(len(b[n:])) < clen {
			return Config{}, ErrMalformedConfig
		}
		b = b[n:]
		clientAddr := string(b[:clen])
		b = b[clen:]

		if len(b) < 1 {
			return Config{}, ErrMalformedConfig
		}
		cfg.Members = append(cfg.Members, Member{
			ID: NodeID(id), Addr: addr, ClientAddr: clientAddr, Learner: b[0] == 1,
		})
		b = b[1:]
	}
	cfg.normalize()
	return cfg, nil
}

// ConfChangeType is the kind of membership edit a ConfChange requests.
type ConfChangeType uint8

const (
	// ConfChangeAddVoter adds a server that votes immediately.
	ConfChangeAddVoter ConfChangeType = iota
	// ConfChangeAddLearner adds a non-voting replica that only catches up.
	ConfChangeAddLearner
	// ConfChangePromote turns an existing learner into a voter.
	ConfChangePromote
	// ConfChangeRemove drops a server from the configuration.
	ConfChangeRemove
)

func (t ConfChangeType) String() string {
	switch t {
	case ConfChangeAddVoter:
		return "add-voter"
	case ConfChangeAddLearner:
		return "add-learner"
	case ConfChangePromote:
		return "promote"
	case ConfChangeRemove:
		return "remove"
	default:
		return fmt.Sprintf("confchange(%d)", uint8(t))
	}
}

// ConfChange is a single-server membership edit. Raft only stays safe across
// reconfiguration if the old and new configurations always share a majority,
// which single-server changes guarantee for free -- so this type deliberately
// cannot express a batch edit.
type ConfChange struct {
	Type       ConfChangeType
	ID         NodeID
	Addr       string
	ClientAddr string
}

func (cc ConfChange) String() string {
	return fmt.Sprintf("%s %d@%s", cc.Type, uint64(cc.ID), cc.Addr)
}

// Marshal encodes a ConfChange for storage inside a log entry.
func (cc ConfChange) Marshal() []byte {
	buf := make([]byte, 0, len(cc.Addr)+2*binary.MaxVarintLen64+1)
	buf = append(buf, byte(cc.Type))
	buf = binary.AppendUvarint(buf, uint64(cc.ID))
	buf = binary.AppendUvarint(buf, uint64(len(cc.Addr)))
	buf = append(buf, cc.Addr...)
	buf = binary.AppendUvarint(buf, uint64(len(cc.ClientAddr)))
	buf = append(buf, cc.ClientAddr...)
	return buf
}

// UnmarshalConfChange decodes an entry payload written by ConfChange.Marshal.
func UnmarshalConfChange(b []byte) (ConfChange, error) {
	if len(b) < 1 {
		return ConfChange{}, ErrMalformedConfig
	}
	cc := ConfChange{Type: ConfChangeType(b[0])}
	b = b[1:]
	id, n := binary.Uvarint(b)
	if n <= 0 {
		return ConfChange{}, ErrMalformedConfig
	}
	cc.ID = NodeID(id)
	b = b[n:]
	alen, n := binary.Uvarint(b)
	if n <= 0 || uint64(len(b[n:])) < alen {
		return ConfChange{}, ErrMalformedConfig
	}
	b = b[n:]
	cc.Addr = string(b[:alen])
	b = b[alen:]

	clen, n := binary.Uvarint(b)
	if n <= 0 || uint64(len(b[n:])) < clen {
		return ConfChange{}, ErrMalformedConfig
	}
	cc.ClientAddr = string(b[n : uint64(n)+clen])
	return cc, nil
}

// Apply folds a membership edit into a configuration, rejecting edits that
// would leave the cluster unable to elect a leader.
func (c Config) Apply(cc ConfChange) (Config, error) {
	switch cc.Type {
	case ConfChangeAddVoter:
		return c.With(Member{ID: cc.ID, Addr: cc.Addr, ClientAddr: cc.ClientAddr, Learner: false}), nil
	case ConfChangeAddLearner:
		if c.IsVoter(cc.ID) {
			return c, fmt.Errorf("%w: cannot demote voter %d to learner", ErrInvalidConfChange, uint64(cc.ID))
		}
		return c.With(Member{ID: cc.ID, Addr: cc.Addr, ClientAddr: cc.ClientAddr, Learner: true}), nil
	case ConfChangePromote:
		m, ok := c.Member(cc.ID)
		if !ok {
			return c, fmt.Errorf("%w: %d is not a member", ErrInvalidConfChange, uint64(cc.ID))
		}
		m.Learner = false
		if cc.Addr != "" {
			m.Addr = cc.Addr
		}
		if cc.ClientAddr != "" {
			m.ClientAddr = cc.ClientAddr
		}
		return c.With(m), nil
	case ConfChangeRemove:
		if !c.Has(cc.ID) {
			return c, fmt.Errorf("%w: %d is not a member", ErrInvalidConfChange, uint64(cc.ID))
		}
		next := c.Without(cc.ID)
		if len(next.Voters()) == 0 {
			return c, fmt.Errorf("%w: removing %d would leave no voters", ErrInvalidConfChange, uint64(cc.ID))
		}
		return next, nil
	default:
		return c, fmt.Errorf("%w: unknown type %d", ErrInvalidConfChange, uint8(cc.Type))
	}
}
