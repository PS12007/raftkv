// Package linearize checks histories of concurrent operations for
// linearizability, in the style of Porcupine (Athalye): the Wing & Gong
// search with Lowe's memoisation of (linearized-set, state) pairs, plus
// P-compositionality (each partition, e.g. each key, is checked separately).
package linearize

import (
	"math"
	"math/bits"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Operation is one completed (or possibly-completed) client operation.
// Call and Return are timestamps; an operation whose outcome is unknown
// should have Return = math.MaxInt64 (it may take effect at any point after
// its call, or never).
type Operation struct {
	ClientID int
	Input    any
	Output   any
	Call     int64
	Return   int64
}

// Model is a sequential specification.
type Model struct {
	// Partition splits a history into independent sub-histories. Optional.
	Partition func(history []Operation) [][]Operation
	// Init returns the initial state.
	Init func() any
	// Step applies input to state and reports whether output is a legal
	// result, along with the new state.
	Step func(state, input, output any) (bool, any)
	// Equal compares states. Optional; defaults to ==.
	Equal func(a, b any) bool
	// DescribeOperation renders an operation for diagnostics. Optional.
	DescribeOperation func(input, output any) string
}

// Result is the checker verdict.
type Result int

const (
	Ok Result = iota
	Illegal
	Unknown // timed out
)

func (r Result) String() string { return [...]string{"Ok", "Illegal", "Unknown"}[r] }

// Info carries diagnostics for a failed check.
type Info struct {
	// FailedPartition is the sub-history that could not be linearized.
	FailedPartition []Operation
	// Longest is the longest linearizable prefix found (operation indices
	// into FailedPartition, in linearization order).
	Longest []int
}

type entryKind uint8

const (
	callEntry entryKind = iota
	returnEntry
)

type entry struct {
	kind  entryKind
	value any
	id    int
	time  int64
}

type node struct {
	value any
	match *node // for call nodes: the matching return node
	id    int
	next  *node
	prev  *node
}

func makeEntries(history []Operation) []entry {
	es := make([]entry, 0, 2*len(history))
	for i, op := range history {
		es = append(es, entry{callEntry, op.Input, i, op.Call})
		es = append(es, entry{returnEntry, op.Output, i, op.Return})
	}
	sort.SliceStable(es, func(i, j int) bool {
		if es[i].time != es[j].time {
			return es[i].time < es[j].time
		}
		// Calls before returns at equal timestamps (more permissive).
		return es[i].kind < es[j].kind
	})
	return es
}

func makeLinkedList(es []entry) *node {
	var root *node
	match := map[int]*node{}
	for i := len(es) - 1; i >= 0; i-- {
		e := es[i]
		var n *node
		if e.kind == returnEntry {
			n = &node{value: e.value, id: e.id}
			match[e.id] = n
		} else {
			n = &node{value: e.value, match: match[e.id], id: e.id}
		}
		n.next = root
		if root != nil {
			root.prev = n
		}
		root = n
	}
	head := &node{id: -1, next: root}
	if root != nil {
		root.prev = head
	}
	return head
}

func lift(n *node) {
	n.prev.next = n.next
	n.next.prev = n.prev
	m := n.match
	m.prev.next = m.next
	if m.next != nil {
		m.next.prev = m.prev
	}
}

func unlift(n *node) {
	m := n.match
	m.prev.next = m
	if m.next != nil {
		m.next.prev = m
	}
	n.prev.next = n
	n.next.prev = n
}

type bitset []uint64

func newBitset(n int) bitset { return make(bitset, (n+63)/64) }

func (b bitset) set(i int) bitset   { b[i/64] |= 1 << uint(i%64); return b }
func (b bitset) clear(i int) bitset { b[i/64] &^= 1 << uint(i%64); return b }
func (b bitset) clone() bitset      { return append(bitset(nil), b...) }
func (b bitset) popcount() int {
	n := 0
	for _, w := range b {
		n += bits.OnesCount64(w)
	}
	return n
}
func (b bitset) hash() uint64 {
	h := uint64(len(b))
	for _, w := range b {
		h ^= w
		h *= 0x100000001b3
	}
	return h
}
func (b bitset) equals(o bitset) bool {
	for i := range b {
		if b[i] != o[i] {
			return false
		}
	}
	return true
}

type cacheEntry struct {
	linearized bitset
	state      any
}

type callsEntry struct {
	n     *node
	state any
}

func checkSingle(model Model, history []Operation, kill *atomic.Bool) (bool, []int) {
	n := len(history)
	head := makeLinkedList(makeEntries(history))
	equal := model.Equal
	if equal == nil {
		equal = func(a, b any) bool { return a == b }
	}
	linearized := newBitset(n)
	cache := map[uint64][]cacheEntry{}
	var calls []callsEntry
	var longest []int
	state := model.Init()
	cur := head.next
	iter := 0
	for head.next != nil {
		iter++
		if iter&0xfff == 0 && kill.Load() {
			return false, nil
		}
		if cur.match != nil { // call node
			ok, newState := model.Step(state, cur.value, cur.match.value)
			if ok {
				nl := linearized.clone().set(cur.id)
				h := nl.hash()
				seen := false
				for _, ce := range cache[h] {
					if ce.linearized.equals(nl) && equal(ce.state, newState) {
						seen = true
						break
					}
				}
				if !seen {
					cache[h] = append(cache[h], cacheEntry{nl, newState})
					calls = append(calls, callsEntry{cur, state})
					state = newState
					linearized.set(cur.id)
					lift(cur)
					if len(calls) > len(longest) {
						longest = longest[:0]
						for _, c := range calls {
							longest = append(longest, c.n.id)
						}
					}
					cur = head.next
					continue
				}
			}
			cur = cur.next
		} else { // return node of an operation not yet linearized: backtrack
			if len(calls) == 0 {
				return false, longest
			}
			top := calls[len(calls)-1]
			calls = calls[:len(calls)-1]
			state = top.state
			linearized.clear(top.n.id)
			unlift(top.n)
			cur = top.n.next
		}
	}
	return true, nil
}

// Check checks a history. timeout <= 0 means no timeout.
func Check(model Model, history []Operation, timeout time.Duration) (Result, *Info) {
	parts := [][]Operation{history}
	if model.Partition != nil {
		parts = model.Partition(history)
	}
	var kill atomic.Bool
	var wg sync.WaitGroup
	var mu sync.Mutex
	var info *Info
	illegal := false
	for _, p := range parts {
		wg.Add(1)
		go func(p []Operation) {
			defer wg.Done()
			ok, longest := checkSingle(model, p, &kill)
			if !ok && !kill.Load() {
				mu.Lock()
				if !illegal {
					illegal = true
					info = &Info{FailedPartition: p, Longest: longest}
				}
				mu.Unlock()
				kill.Store(true)
			}
		}(p)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	timedOut := false
	if timeout > 0 {
		select {
		case <-done:
		case <-time.After(timeout):
			timedOut = true
			kill.Store(true)
			<-done
		}
	} else {
		<-done
	}
	switch {
	case illegal:
		return Illegal, info
	case timedOut:
		return Unknown, nil
	}
	return Ok, nil
}

// Pending is the Return timestamp for an operation whose outcome is unknown.
const Pending int64 = math.MaxInt64
