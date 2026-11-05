// Package rsm glues a raft.Raft to an application state machine.
//
// Core is single-threaded and callback based, so the same code is driven by
// the deterministic simulator (callbacks run inline) and by the production
// server (callbacks hand results to waiting gRPC handlers).
package rsm

import (
	"errors"
	"sort"

	"github.com/ps12007/raftkv/raft"
)

// ErrWrongLeader means the command was not (or may not have been) committed
// by this node as leader. The client should retry elsewhere; deduplication in
// the state machine makes the retry safe.
var ErrWrongLeader = errors.New("wrong leader")

// StateMachine is the replicated application.
type StateMachine interface {
	// Apply executes a committed command and returns its result.
	Apply(cmd []byte) []byte
	// Snapshot serialises the full state.
	Snapshot() []byte
	// Restore replaces the state with a snapshot.
	Restore(data []byte)
}

// Callback receives a command's result.
type Callback func(result []byte, err error)

type pending struct {
	term uint64
	cb   Callback
}

// Core couples a Raft node and a state machine.
type Core struct {
	R  *raft.Raft
	SM StateMachine
	// MaxLogBytes triggers a snapshot when the log grows beyond it (0 = never).
	MaxLogBytes int

	pending map[uint64]pending
	applied uint64
	// OnApply, if set, is called after every applied entry (for metrics
	// and the simulator's safety checks).
	OnApply func(e *raft.Entry)
}

// New returns a Core.
func New(r *raft.Raft, sm StateMachine, maxLogBytes int) *Core {
	return &Core{R: r, SM: sm, MaxLogBytes: maxLogBytes, pending: map[uint64]pending{}}
}

// Applied returns the last applied index.
func (c *Core) Applied() uint64 { return c.applied }

// Submit proposes a command. cb is called exactly once, from a later Flush
// (or immediately if this node is not the leader).
func (c *Core) Submit(cmd []byte, cb Callback) {
	idx, term, err := c.R.Propose(cmd)
	if err != nil {
		cb(nil, ErrWrongLeader)
		return
	}
	c.pending[idx] = pending{term: term, cb: cb}
}

// SubmitConfChange proposes a single-server membership change.
func (c *Core) SubmitConfChange(add bool, m raft.Member, cb Callback) {
	idx, term, err := c.R.ProposeConfChange(add, m)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			err = ErrWrongLeader
		}
		cb(nil, err)
		return
	}
	c.pending[idx] = pending{term: term, cb: cb}
}

// LeaderHint returns the believed leader and its address.
func (c *Core) LeaderHint() (raft.NodeID, string) {
	l := c.R.Leader()
	for _, m := range c.R.Members() {
		if m.ID == l {
			return l, m.Addr
		}
	}
	return l, ""
}

// Flush persists Raft state, applies newly committed entries, completes
// callbacks and returns the Raft messages to send.
func (c *Core) Flush() []raft.Message {
	msgs, applies := c.R.Flush()
	for _, a := range applies {
		if a.Snapshot != nil {
			c.SM.Restore(a.Snapshot.Data)
			c.applied = a.Snapshot.Index
			c.failPending(func(idx uint64, _ pending) bool { return idx <= a.Snapshot.Index })
			continue
		}
		e := a.Entry
		var res []byte
		if e.Type == raft.EntryNormal {
			res = c.SM.Apply(e.Data)
		}
		c.applied = e.Index
		if p, ok := c.pending[e.Index]; ok {
			delete(c.pending, e.Index)
			if p.term == e.Term {
				p.cb(res, nil)
			} else {
				p.cb(nil, ErrWrongLeader)
			}
		}
		if c.OnApply != nil {
			c.OnApply(e)
		}
	}
	if c.MaxLogBytes > 0 && c.R.LogBytes() > c.MaxLogBytes {
		c.R.Snapshot(c.applied, c.SM.Snapshot())
	}
	// Once leadership is lost (or the term moves on) pending commands may
	// never be applied here; fail them so clients retry promptly.
	if len(c.pending) > 0 {
		lead, term := c.R.IsLeader(), c.R.Term()
		c.failPending(func(_ uint64, p pending) bool { return !lead || p.term != term })
	}
	return msgs
}

func (c *Core) failPending(match func(uint64, pending) bool) {
	var idxs []uint64
	for idx, p := range c.pending {
		if match(idx, p) {
			idxs = append(idxs, idx)
		}
	}
	sort.Slice(idxs, func(i, j int) bool { return idxs[i] < idxs[j] }) // deterministic order
	for _, idx := range idxs {
		p := c.pending[idx]
		delete(c.pending, idx)
		p.cb(nil, ErrWrongLeader)
	}
}

// Close fails all outstanding callbacks.
func (c *Core) Close() {
	c.failPending(func(uint64, pending) bool { return true })
}
