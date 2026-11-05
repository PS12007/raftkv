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

// Reader is implemented by state machines that can answer read-only
// queries directly, enabling ReadIndex reads that bypass the log.
type Reader interface {
	Read(query []byte) []byte
}

// Callback receives a command's result.
type Callback func(result []byte, err error)

type pending struct {
	term uint64
	cb   Callback
}

type pendingRead struct {
	query []byte
	term  uint64
	cb    Callback
}

type confirmedRead struct {
	index uint64
	pendingRead
}

// Core couples a Raft node and a state machine.
type Core struct {
	R  *raft.Raft
	SM StateMachine
	// MaxLogBytes triggers a snapshot when the log grows beyond it (0 = never).
	MaxLogBytes int

	pending map[uint64]pending
	applied uint64

	readCtx   uint64
	reads     map[uint64]pendingRead // ctx -> read awaiting confirmation
	confirmed []confirmedRead        // confirmed, waiting for applied >= index
	// OnApply, if set, is called after every applied entry (for metrics
	// and the simulator's safety checks).
	OnApply func(e *raft.Entry)
}

// New returns a Core.
func New(r *raft.Raft, sm StateMachine, maxLogBytes int) *Core {
	return &Core{R: r, SM: sm, MaxLogBytes: maxLogBytes, pending: map[uint64]pending{}, reads: map[uint64]pendingRead{}}
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

// SubmitRead performs a linearizable read-only query using ReadIndex: no
// log entry is written. If the state machine cannot answer queries, or the
// leader is not ready for ReadIndex yet, the query goes through the log
// like a command (so query must also be a valid command).
func (c *Core) SubmitRead(query []byte, cb Callback) {
	if _, ok := c.SM.(Reader); !ok {
		c.Submit(query, cb)
		return
	}
	c.readCtx++
	err := c.R.ReadIndex(c.readCtx)
	switch {
	case errors.Is(err, raft.ErrNotReady):
		c.Submit(query, cb)
	case err != nil:
		cb(nil, ErrWrongLeader)
	default:
		c.reads[c.readCtx] = pendingRead{query: query, term: c.R.Term(), cb: cb}
	}
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
	c.serveReads()
	if c.MaxLogBytes > 0 && c.R.LogBytes() > c.MaxLogBytes {
		c.R.Snapshot(c.applied, c.SM.Snapshot())
	}
	// Once leadership is lost (or the term moves on) pending commands may
	// never be applied here; fail them so clients retry promptly.
	if len(c.pending) > 0 {
		lead, term := c.R.IsLeader(), c.R.Term()
		c.failPending(func(_ uint64, p pending) bool { return !lead || p.term != term })
	}
	if len(c.reads) > 0 || len(c.confirmed) > 0 {
		if lead, term := c.R.IsLeader(), c.R.Term(); !lead || c.anyReadTermMismatch(term) {
			c.failReads()
		}
	}
	return msgs
}

func (c *Core) anyReadTermMismatch(term uint64) bool {
	for _, r := range c.reads {
		if r.term != term {
			return true
		}
	}
	for _, r := range c.confirmed {
		if r.term != term {
			return true
		}
	}
	return false
}

// serveReads moves confirmed ReadIndex reads into the wait list and answers
// those whose index has been applied.
func (c *Core) serveReads() {
	for _, rs := range c.R.ReadStates() {
		if p, ok := c.reads[rs.Ctx]; ok {
			delete(c.reads, rs.Ctx)
			c.confirmed = append(c.confirmed, confirmedRead{index: rs.Index, pendingRead: p})
		}
	}
	if len(c.confirmed) == 0 {
		return
	}
	rd := c.SM.(Reader)
	keep := c.confirmed[:0]
	for _, r := range c.confirmed {
		if c.applied >= r.index {
			r.cb(rd.Read(r.query), nil)
		} else {
			keep = append(keep, r)
		}
	}
	c.confirmed = keep
}

// failReads fails every outstanding read, in a deterministic order.
func (c *Core) failReads() {
	var ctxs []uint64
	for ctx := range c.reads {
		ctxs = append(ctxs, ctx)
	}
	sort.Slice(ctxs, func(i, j int) bool { return ctxs[i] < ctxs[j] })
	for _, ctx := range ctxs {
		p := c.reads[ctx]
		delete(c.reads, ctx)
		p.cb(nil, ErrWrongLeader)
	}
	conf := c.confirmed
	c.confirmed = nil
	for _, r := range conf {
		r.cb(nil, ErrWrongLeader)
	}
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
	c.failReads()
	c.failPending(func(uint64, pending) bool { return true })
}
