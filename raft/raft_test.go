package raft

import (
	"fmt"
	"math/rand"
	"testing"
)

// cluster is a minimal synchronous test harness: messages are queued and
// delivered in FIFO order, optionally filtered.
type cluster struct {
	t       *testing.T
	nodes   map[NodeID]*Raft
	stores  map[NodeID]*MemStorage
	applied map[NodeID][]Entry
	queue   []Message
	cut     map[NodeID]bool // isolated nodes
	down    map[NodeID]bool
	peers   []Member
}

func newCluster(t *testing.T, n int) *cluster {
	c := &cluster{t: t, nodes: map[NodeID]*Raft{}, stores: map[NodeID]*MemStorage{},
		applied: map[NodeID][]Entry{}, cut: map[NodeID]bool{}, down: map[NodeID]bool{}}
	for i := 1; i <= n; i++ {
		c.peers = append(c.peers, Member{ID: NodeID(i), Addr: fmt.Sprintf("n%d", i)})
	}
	for _, p := range c.peers {
		c.stores[p.ID] = NewMemStorage()
		c.start(p.ID, c.peers)
	}
	return c
}

func (c *cluster) start(id NodeID, peers []Member) {
	r, err := New(Config{ID: id, Peers: peers, Storage: c.stores[id], ElectionTicks: 10, HeartbeatTicks: 2,
		Rand: rand.New(rand.NewSource(int64(id)))})
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[id] = r
	c.applied[id] = nil
	delete(c.down, id)
	c.flush(id)
}

func (c *cluster) crash(id NodeID) { c.down[id] = true }

func (c *cluster) flush(id NodeID) {
	msgs, apply := c.nodes[id].Flush()
	c.queue = append(c.queue, msgs...)
	for _, a := range apply {
		if a.Snapshot != nil {
			c.applied[id] = nil // state replaced; keep only entries after it
			continue
		}
		c.applied[id] = append(c.applied[id], *a.Entry)
	}
}

func (c *cluster) settle() {
	for i := 0; len(c.queue) > 0; i++ {
		if i > 100000 {
			c.t.Fatal("message storm")
		}
		m := c.queue[0]
		c.queue = c.queue[1:]
		if c.down[m.To] || c.down[m.From] || c.cut[m.To] || c.cut[m.From] {
			continue
		}
		if r := c.nodes[m.To]; r != nil {
			r.Step(m)
			c.flush(m.To)
		}
	}
}

func (c *cluster) tick(n int) {
	for i := 0; i < n; i++ {
		for id, r := range c.nodes {
			if !c.down[id] {
				r.Tick()
				c.flush(id)
			}
		}
		c.settle()
	}
}

func (c *cluster) leader() *Raft {
	var l *Raft
	for id, r := range c.nodes {
		if !c.down[id] && !c.cut[id] && r.IsLeader() {
			if l != nil && l.Term() == r.Term() {
				c.t.Fatalf("two leaders in term %d", r.Term())
			}
			if l == nil || r.Term() > l.Term() {
				l = r
			}
		}
	}
	return l
}

func (c *cluster) waitLeader() *Raft {
	for i := 0; i < 200; i++ {
		if l := c.leader(); l != nil {
			return l
		}
		c.tick(1)
	}
	c.t.Fatal("no leader elected")
	return nil
}

func (c *cluster) propose(s string) uint64 {
	l := c.waitLeader()
	idx, _, err := l.Propose([]byte(s))
	if err != nil {
		c.t.Fatal(err)
	}
	c.flush(l.ID())
	c.settle()
	return idx
}

func normalData(es []Entry) []string {
	var out []string
	for _, e := range es {
		if e.Type == EntryNormal {
			out = append(out, string(e.Data))
		}
	}
	return out
}

func TestElection(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader()
	term := l.Term()
	c.tick(50)
	if c.leader() != l || l.Term() != term {
		t.Fatal("leadership changed in a stable cluster")
	}
	c.crash(l.ID())
	l2 := c.waitLeader()
	if l2 == l || l2.Term() <= term {
		t.Fatal("no new leader after crash")
	}
}

func TestReplicationAndCommit(t *testing.T) {
	c := newCluster(t, 5)
	for i := 0; i < 10; i++ {
		c.propose(fmt.Sprint(i))
	}
	c.tick(5)
	for id := range c.nodes {
		got := normalData(c.applied[id])
		if len(got) != 10 {
			t.Fatalf("node %d applied %v", id, got)
		}
	}
}

func TestNoCommitWithoutMajority(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader()
	for id := range c.nodes {
		if id != l.ID() {
			c.cut[id] = true
		}
	}
	commit := l.Commit()
	l.Propose([]byte("x"))
	c.flush(l.ID())
	c.settle()
	if l.Commit() != commit {
		t.Fatal("committed without a majority")
	}
	// Check-quorum makes the isolated leader step down.
	c.tick(30)
	if l.IsLeader() {
		t.Fatal("isolated leader did not step down")
	}
}

func TestLogRepairAfterPartition(t *testing.T) {
	c := newCluster(t, 5)
	old := c.waitLeader()
	c.propose("a")
	// Isolate the leader; it accepts entries that can never commit.
	c.cut[old.ID()] = true
	for i := 0; i < 5; i++ {
		old.Propose([]byte(fmt.Sprint("lost", i)))
		c.flush(old.ID())
	}
	c.settle()
	nl := c.waitLeader()
	if nl == old {
		t.Fatal("expected a new leader")
	}
	c.propose("b")
	c.propose("c")
	delete(c.cut, old.ID())
	c.tick(20)
	for id := range c.nodes {
		got := normalData(c.applied[id])
		if fmt.Sprint(got) != "[a b c]" {
			t.Fatalf("node %d applied %v", id, got)
		}
	}
}

func TestPersistenceAcrossRestart(t *testing.T) {
	c := newCluster(t, 3)
	for i := 0; i < 5; i++ {
		c.propose(fmt.Sprint(i))
	}
	c.tick(5)
	for _, p := range c.peers {
		c.crash(p.ID)
	}
	for _, p := range c.peers {
		c.start(p.ID, c.peers)
	}
	c.waitLeader()
	c.propose("after")
	c.tick(5)
	for id := range c.nodes {
		if got := normalData(c.applied[id]); len(got) != 6 {
			t.Fatalf("node %d applied %v after restart", id, got)
		}
	}
}

func TestSnapshotCatchUp(t *testing.T) {
	c := newCluster(t, 3)
	c.waitLeader()
	lagger := NodeID(0)
	for id, r := range c.nodes {
		if !r.IsLeader() {
			lagger = id
			break
		}
	}
	c.crash(lagger)
	for i := 0; i < 20; i++ {
		c.propose(fmt.Sprint(i))
	}
	c.tick(2)
	for id, r := range c.nodes {
		if id != lagger {
			r.Snapshot(r.applied, []byte("state"))
			c.flush(id)
		}
	}
	c.start(lagger, c.peers)
	c.tick(20)
	r := c.nodes[lagger]
	if r.SnapIndex() == 0 || r.Commit() < 21 {
		t.Fatalf("lagger did not install snapshot: snap=%d commit=%d", r.SnapIndex(), r.Commit())
	}
}

func TestMembershipAddRemove(t *testing.T) {
	c := newCluster(t, 3)
	c.propose("x")
	l := c.waitLeader()
	// Add node 4 (a fresh, empty node).
	c.stores[4] = NewMemStorage()
	c.start(4, nil)
	if _, _, err := l.ProposeConfChange(true, Member{ID: 4, Addr: "n4"}); err != nil {
		t.Fatal(err)
	}
	c.flush(l.ID())
	if _, _, err := l.ProposeConfChange(true, Member{ID: 5}); err != ErrConfChangePending {
		t.Fatalf("want ErrConfChangePending, got %v", err)
	}
	c.tick(20)
	if n := len(c.nodes[4].Members()); n != 4 {
		t.Fatalf("node 4 sees %d members", n)
	}
	c.propose("y")
	c.tick(5)
	if got := normalData(c.applied[4]); fmt.Sprint(got) != "[x y]" {
		t.Fatalf("node 4 applied %v", got)
	}
	// Remove the leader itself; it must step down once that commits.
	l = c.waitLeader()
	if _, _, err := l.ProposeConfChange(false, Member{ID: l.ID()}); err != nil {
		t.Fatal(err)
	}
	c.flush(l.ID())
	c.tick(50)
	if l.IsLeader() {
		t.Fatal("removed leader still leading")
	}
	nl := c.waitLeader()
	if nl == l || len(nl.Members()) != 3 {
		t.Fatalf("bad leader after removal: %d members", len(nl.Members()))
	}
	c.propose("z")
	c.tick(50)
	if l.IsLeader() || c.leader() != nl {
		t.Fatal("removed server disrupted the cluster")
	}
}

func TestMembersEncoding(t *testing.T) {
	ms := []Member{{ID: 3, Addr: "c:3"}, {ID: 1, Addr: "a:1"}}
	got := DecodeMembers(EncodeMembers(ms))
	if len(got) != 2 || got[0].ID != 1 || got[1].Addr != "c:3" {
		t.Fatalf("got %+v", got)
	}
}

func TestReadIndex(t *testing.T) {
	c := newCluster(t, 3)
	c.propose("x")
	l := c.waitLeader()
	if err := l.ReadIndex(1); err != nil {
		t.Fatal(err)
	}
	c.flush(l.ID())
	c.settle()
	rs := l.ReadStates()
	if len(rs) != 1 || rs[0].Ctx != 1 || rs[0].Index != l.Commit() {
		t.Fatalf("read not confirmed: %+v (commit %d)", rs, l.Commit())
	}

	// A leader cut off from the majority must never confirm a read: another
	// leader may already be accepting writes.
	for id := range c.nodes {
		if id != l.ID() {
			c.cut[id] = true
		}
	}
	if err := l.ReadIndex(2); err != nil {
		t.Fatal(err)
	}
	c.flush(l.ID())
	c.settle()
	c.tick(5)
	if rs := l.ReadStates(); len(rs) != 0 {
		t.Fatalf("isolated leader confirmed a read: %+v", rs)
	}
}

func TestReadIndexNotReadyBeforeNoopCommits(t *testing.T) {
	// A new leader whose no-op has not committed does not know the commit
	// index of earlier terms, so it must refuse ReadIndex.
	r, _ := New(Config{ID: 9, Peers: []Member{{ID: 9}, {ID: 10}, {ID: 11}}, Storage: NewMemStorage()})
	for i := 0; i < 100 && r.State() != Candidate; i++ {
		r.Tick()
	}
	r.Step(Message{Type: MsgVoteResp, From: 10, To: 9, Term: r.Term(), Granted: true})
	if !r.IsLeader() {
		t.Fatal("expected to become leader")
	}
	if err := r.ReadIndex(1); err != ErrNotReady {
		t.Fatalf("want ErrNotReady, got %v", err)
	}
}

func TestLearnerCatchUpAndPromotion(t *testing.T) {
	c := newCluster(t, 3)
	for i := 0; i < 20; i++ {
		c.propose(fmt.Sprint(i))
	}
	l := c.waitLeader()
	// While node 4 is down, adding it as a learner must not change the
	// quorum: commits keep flowing with the original three voters.
	c.stores[4] = NewMemStorage()
	c.start(4, nil)
	c.crash(4)
	if _, _, err := l.ProposeConfChange(true, Member{ID: 4, Addr: "n4", Learner: true}); err != nil {
		t.Fatal(err)
	}
	c.flush(l.ID())
	c.settle()
	if l.quorum() != 2 {
		t.Fatalf("learner counted in quorum: %d", l.quorum())
	}
	before := l.Commit()
	c.propose("while-learner-down")
	if l.Commit() <= before {
		t.Fatal("commit blocked by a down learner")
	}
	// Bring it up: it catches up, never campaigns, then gets promoted.
	delete(c.down, 4)
	for i := 0; i < 100 && c.nodes[4].State() == Follower; i++ {
		c.tick(1)
		for _, m := range c.nodes[4].Members() {
			if m.ID == 4 && !m.Learner {
				goto promoted
			}
		}
	}
	t.Fatalf("learner never promoted; members %+v", l.Members())
promoted:
	if c.nodes[4].State() != Follower {
		t.Fatal("learner campaigned")
	}
	c.tick(10)
	if l.quorum() != 3 {
		t.Fatalf("quorum after promotion: %d", l.quorum())
	}
	if got := normalData(c.applied[4]); len(got) != 21 {
		t.Fatalf("promoted node applied %d entries", len(got))
	}
	// A bad change: promoting an existing voter.
	if _, _, err := c.waitLeader().ProposeConfChange(true, Member{ID: 4}); err != ErrBadConfChange && err != ErrConfChangePending {
		t.Fatalf("want ErrBadConfChange, got %v", err)
	}
}

func TestLearnerNeverCampaigns(t *testing.T) {
	s := NewMemStorage()
	r, _ := New(Config{ID: 2, Peers: []Member{{ID: 1}, {ID: 2, Learner: true}}, Storage: s})
	for i := 0; i < 200; i++ {
		r.Tick()
	}
	if r.State() != Follower || r.Term() != 0 {
		t.Fatalf("learner campaigned: state %v term %d", r.State(), r.Term())
	}
}

// TestSelfRemovalCrashDoesNotDeadlock: a leader proposes its own removal,
// the entry reaches nobody, and the leader restarts. Its log is now the
// most up-to-date, so the other voter cannot win; the old leader must be
// allowed to campaign (counting only the new config's votes), commit its
// removal and step aside.
func TestSelfRemovalCrashDoesNotDeadlock(t *testing.T) {
	c := newCluster(t, 2)
	c.propose("x")
	l := c.waitLeader()
	other := NodeID(3) - l.ID()
	c.cut[other] = true
	if _, _, err := l.ProposeConfChange(false, Member{ID: l.ID()}); err != nil {
		t.Fatal(err)
	}
	c.flush(l.ID())
	c.settle() // dropped: other never sees the removal
	c.crash(l.ID())
	delete(c.cut, other)
	c.start(l.ID(), c.peers)
	c.tick(200)
	nl := c.leader()
	if nl == nil || nl.ID() != other {
		t.Fatalf("cluster deadlocked after self-removal crash (leader %v)", nl)
	}
	if ms := nl.Members(); len(ms) != 1 || ms[0].ID != other {
		t.Fatalf("removal not committed: %+v", ms)
	}
}
