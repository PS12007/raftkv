// Package sim is a deterministic discrete-event simulator for raftkv.
//
// A whole cluster (Raft nodes, KV state machines and clients) runs in one
// goroutine on a virtual clock. All randomness comes from one seeded RNG, so
// a seed reproduces a run exactly, including every dropped, delayed,
// duplicated and reordered message, every partition and every crash.
package sim

import (
	"container/heap"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"math/rand"
	"sort"
	"time"

	"github.com/ps12007/raftkv/kv"
	"github.com/ps12007/raftkv/linearize"
	"github.com/ps12007/raftkv/raft"
	"github.com/ps12007/raftkv/rsm"
)

// Time is virtual time in microseconds.
type Time = int64

// Virtual time units.
const (
	Microsecond Time = 1
	Millisecond      = 1000 * Microsecond
	Second           = 1000 * Millisecond
)

// Config describes a simulation run.
type Config struct {
	Seed    int64
	Nodes   int // initial cluster size
	Clients int
	Keys    int

	// ChaosDuration is how long faults are injected. Afterwards the cluster
	// is healed and must make progress (liveness) within QuietDuration.
	ChaosDuration Time
	QuietDuration Time

	// Fault injection.
	Crashes    bool
	Partitions bool
	Unreliable bool // drops, duplicates, long delays (reordering)
	Membership bool // random single-server add/remove
	// LeaderChurn repeatedly crashes the leader shortly after it is
	// elected (sub-election-timeout), restarting nodes to keep a majority.
	// This targets the "Figure 8" scenario of the Raft paper.
	LeaderChurn bool
	// FaultEvery is the mean time between nemesis actions.
	FaultEvery Time

	// Network model.
	MinDelay, MaxDelay Time
	DropRate           float64
	DupRate            float64
	LongDelayRate      float64
	LongDelayMax       Time

	TickInterval   Time
	ElectionTicks  int
	HeartbeatTicks int
	ClientTimeout  Time
	MaxLogBytes    int // snapshot threshold (0 disables snapshots)
	// MaxEntriesPerMsg caps AppendEntries batches. Small values leave
	// followers partially caught up, which exercises log repair.
	MaxEntriesPerMsg int
	PreVote          bool
	// ReadIndex serves Gets with ReadIndex instead of through the log.
	ReadIndex bool
	// NoCheckQuorum lets a partitioned leader keep believing it leads,
	// which widens the window for stale reads if ReadIndex were wrong.
	NoCheckQuorum bool

	Logf func(format string, args ...any)
}

// DefaultConfig returns a configuration with all faults enabled.
func DefaultConfig(seed int64) Config {
	return Config{
		Seed: seed, Nodes: 5, Clients: 5, Keys: 4,
		ChaosDuration: 6 * Second, QuietDuration: 10 * Second,
		Crashes: true, Partitions: true, Unreliable: true,
		FaultEvery: 300 * Millisecond,
		MinDelay:   100 * Microsecond, MaxDelay: 3 * Millisecond,
		DropRate: 0.1, DupRate: 0.05, LongDelayRate: 0.05, LongDelayMax: 200 * Millisecond,
		TickInterval: 10 * Millisecond, ElectionTicks: 10, HeartbeatTicks: 3,
		ClientTimeout: 250 * Millisecond,
		MaxLogBytes:   4096,
	}
}

type event struct {
	at  Time
	seq uint64
	fn  func()
}

type eventQueue []*event

func (q eventQueue) Len() int { return len(q) }
func (q eventQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}
func (q eventQueue) Swap(i, j int)  { q[i], q[j] = q[j], q[i] }
func (q *eventQueue) Push(x any)    { *q = append(*q, x.(*event)) }
func (q *eventQueue) Pop() any      { old := *q; e := old[len(old)-1]; *q = old[:len(old)-1]; return e }
func (q eventQueue) peek() *event   { return q[0] }
func (q *eventQueue) push(e *event) { heap.Push(q, e) }

// Stats counts what happened during a run.
type Stats struct {
	Events, Sent, Delivered, Dropped, Duplicated int
	Crashes, Restarts, Partitions, Heals         int
	AddServer, RemoveServer                      int
	Elections                                    int
	Snapshots, SnapshotsInstalled                int
	OpsCompleted, OpsPending                     int
	MaxTerm                                      uint64
}

// Sim is a simulated cluster.
type Sim struct {
	cfg   Config
	rng   *rand.Rand
	now   Time
	q     eventQueue
	seq   uint64
	stamp int64

	nodes    map[raft.NodeID]*Node
	nodeIDs  []raft.NodeID // every node ever created, ascending
	nextNode raft.NodeID
	group    map[raft.NodeID]int // partition group of each node
	flaky    bool                // network currently unreliable

	clients []*Client
	ops     []linearize.Operation
	stopOps bool

	// Safety bookkeeping.
	leaderOf  map[uint64]raft.NodeID // term -> leader
	appliedAt map[uint64]uint64      // index -> term of applied entry
	err       error

	trace hash.Hash64
	Stats Stats
}

// New builds a simulation.
func New(cfg Config) *Sim {
	s := &Sim{
		cfg:       cfg,
		rng:       rand.New(rand.NewSource(cfg.Seed)),
		nodes:     map[raft.NodeID]*Node{},
		group:     map[raft.NodeID]int{},
		leaderOf:  map[uint64]raft.NodeID{},
		appliedAt: map[uint64]uint64{},
		trace:     fnv.New64a(),
	}
	var peers []raft.Member
	for i := 1; i <= cfg.Nodes; i++ {
		peers = append(peers, raft.Member{ID: raft.NodeID(i)})
	}
	s.nextNode = raft.NodeID(cfg.Nodes + 1)
	for _, p := range peers {
		n := s.newNode(p.ID, peers)
		n.start()
	}
	for i := 0; i < cfg.Clients; i++ {
		c := &Client{s: s, idx: i, addr: clientAddr(i), cid: uint64(i + 1)}
		s.clients = append(s.clients, c)
		s.after(Time(s.rng.Int63n(int64(20*Millisecond))), c.issue)
	}
	return s
}

func (s *Sim) logf(format string, args ...any) {
	if s.cfg.Logf != nil {
		s.cfg.Logf("%8.3fs "+format, append([]any{float64(s.now) / float64(Second)}, args...)...)
	}
}

func (s *Sim) after(d Time, fn func()) {
	s.seq++
	s.q.push(&event{at: s.now + d, seq: s.seq, fn: fn})
}

func (s *Sim) nextStamp() int64 { s.stamp++; return s.stamp }

func (s *Sim) traceInts(vs ...uint64) {
	var b [8]byte
	for _, v := range vs {
		binary.LittleEndian.PutUint64(b[:], v)
		s.trace.Write(b[:])
	}
}

func (s *Sim) fail(format string, args ...any) {
	if s.err == nil {
		s.err = fmt.Errorf("seed %d at %.3fs: %s", s.cfg.Seed, float64(s.now)/float64(Second), fmt.Sprintf(format, args...))
	}
}

// ---------- network ----------

const clientBase = 1 << 20

func clientAddr(i int) uint64 { return clientBase + uint64(i) }

type clientReq struct {
	op      kv.Op
	attempt uint64
}

type clientResp struct {
	seq     uint64
	attempt uint64
	ok      bool
	res     kv.Result
	leader  raft.NodeID
}

func (s *Sim) blocked(from, to uint64) bool {
	if from >= clientBase || to >= clientBase {
		return false // clients are never partitioned, but do see drops/delays
	}
	return s.group[from] != s.group[to]
}

func (s *Sim) delay() Time {
	d := s.cfg.MinDelay + Time(s.rng.Int63n(int64(s.cfg.MaxDelay-s.cfg.MinDelay+1)))
	if s.flaky && s.rng.Float64() < s.cfg.LongDelayRate {
		d += Time(s.rng.Int63n(int64(s.cfg.LongDelayMax)))
	}
	return d
}

func (s *Sim) send(from, to uint64, payload any) {
	s.Stats.Sent++
	if s.blocked(from, to) {
		s.Stats.Dropped++
		return
	}
	if s.flaky && s.rng.Float64() < s.cfg.DropRate {
		s.Stats.Dropped++
		return
	}
	copies := 1
	if s.flaky && s.rng.Float64() < s.cfg.DupRate {
		copies = 2
		s.Stats.Duplicated++
	}
	for i := 0; i < copies; i++ {
		s.after(s.delay(), func() { s.deliver(from, to, payload) })
	}
}

func (s *Sim) deliver(from, to uint64, payload any) {
	if s.blocked(from, to) { // partition formed while in flight
		s.Stats.Dropped++
		return
	}
	s.Stats.Delivered++
	s.traceInts(uint64(s.now), from, to)
	if to >= clientBase {
		s.clients[to-clientBase].onResp(payload.(clientResp))
		return
	}
	if n := s.nodes[to]; n != nil {
		n.deliver(from, payload)
	}
}

// ---------- nodes ----------

// Node is a simulated server: a Raft peer, its KV store and its "disk".
type Node struct {
	s       *Sim
	id      raft.NodeID
	peers   []raft.Member // bootstrap membership (nil for joiners)
	storage *raft.MemStorage
	core    *rsm.Core
	store   *kv.Store
	up      bool
	gen     int
	removed bool
}

func (s *Sim) newNode(id raft.NodeID, peers []raft.Member) *Node {
	n := &Node{s: s, id: id, peers: peers, storage: raft.NewMemStorage()}
	s.nodes[id] = n
	s.nodeIDs = append(s.nodeIDs, id)
	return n
}

func (n *Node) start() {
	s := n.s
	var logf func(string, ...any)
	if s.cfg.Logf != nil {
		logf = func(f string, a ...any) { s.logf(f, a...) }
	}
	r, err := raft.New(raft.Config{
		ID: n.id, Peers: n.peers, Storage: n.storage,
		ElectionTicks: s.cfg.ElectionTicks, HeartbeatTicks: s.cfg.HeartbeatTicks,
		MaxEntriesPerMsg: s.cfg.MaxEntriesPerMsg, PreVote: s.cfg.PreVote,
		DisableCheckQuorum: s.cfg.NoCheckQuorum,
		Rand:               rand.New(rand.NewSource(s.rng.Int63())), Logf: logf,
	})
	if err != nil {
		panic(err)
	}
	n.store = kv.NewStore()
	n.core = rsm.New(r, n.store, s.cfg.MaxLogBytes)
	n.core.OnApply = n.onApply
	n.up = true
	n.gen++
	gen := n.gen
	var tick func()
	tick = func() {
		if !n.up || n.gen != gen {
			return
		}
		n.core.R.Tick()
		n.flush()
		// ±10% clock drift per tick.
		s.after(s.cfg.TickInterval*9/10+Time(s.rng.Int63n(int64(s.cfg.TickInterval/5+1))), tick)
	}
	s.after(Time(s.rng.Int63n(int64(s.cfg.TickInterval))), tick)
	n.flush()
}

func (n *Node) crash() {
	n.up = false
	n.core = nil // pending client callbacks are lost, as in a real crash
	n.store = nil
}

func (n *Node) onApply(e *raft.Entry) {
	s := n.s
	if t, ok := s.appliedAt[e.Index]; ok && t != e.Term {
		s.fail("state machine safety: node %d applied index %d term %d, another node applied term %d", n.id, e.Index, e.Term, t)
	}
	s.appliedAt[e.Index] = e.Term
}

func (n *Node) deliver(from uint64, payload any) {
	if !n.up {
		return
	}
	switch p := payload.(type) {
	case raft.Message:
		n.core.R.Step(p)
	case clientReq:
		attempt, seq := p.attempt, p.op.Seq
		submit := n.core.Submit
		if p.op.Type == kv.OpGet && n.s.cfg.ReadIndex {
			submit = n.core.SubmitRead
		}
		submit(kv.EncodeOp(p.op), func(res []byte, err error) {
			resp := clientResp{seq: seq, attempt: attempt}
			resp.leader, _ = n.core.LeaderHint()
			if err == nil {
				resp.ok = true
				resp.res, _ = kv.DecodeResult(res)
			}
			n.s.send(n.id, from, resp)
		})
	}
	n.flush()
}

func (n *Node) flush() {
	s := n.s
	snapBefore := n.core.R.SnapIndex()
	msgs := n.core.Flush()
	r := n.core.R
	if r.SnapIndex() != snapBefore {
		s.Stats.Snapshots++
	}
	if r.Term() > s.Stats.MaxTerm {
		s.Stats.MaxTerm = r.Term()
	}
	if r.IsLeader() {
		if l, ok := s.leaderOf[r.Term()]; ok && l != n.id {
			s.fail("election safety: nodes %d and %d both leader in term %d", l, n.id, r.Term())
		} else if !ok {
			s.leaderOf[r.Term()] = n.id
			s.Stats.Elections++
			s.logf("node %d elected leader for term %d", n.id, r.Term())
		}
	}
	for _, m := range msgs {
		if m.Type == raft.MsgSnap {
			s.Stats.SnapshotsInstalled++
		}
		s.send(n.id, m.To, m)
	}
}

// ---------- clients ----------

// Client is a simulated KV client with one outstanding operation, retrying
// with the same sequence number until it succeeds.
type Client struct {
	s       *Sim
	idx     int
	addr    uint64
	cid     uint64
	seq     uint64
	leader  raft.NodeID
	attempt uint64
	cur     *kv.Op
	hist    int // index into s.ops of the current op
}

func (c *Client) issue() {
	s := c.s
	if s.stopOps {
		return
	}
	c.seq++
	key := fmt.Sprintf("k%d", s.rng.Intn(s.cfg.Keys))
	op := kv.Op{Key: key, ClientID: c.cid, Seq: c.seq}
	switch x := s.rng.Intn(10); {
	case x < 4:
		op.Type = kv.OpGet
	case x < 6:
		op.Type = kv.OpPut
		op.Value = fmt.Sprintf("<%d.%d>", c.idx, c.seq)
	case x < 9:
		op.Type = kv.OpAppend
		op.Value = fmt.Sprintf("<%d.%d>", c.idx, c.seq)
	default:
		op.Type = kv.OpDelete
	}
	c.cur = &op
	c.hist = len(s.ops)
	s.ops = append(s.ops, linearize.Operation{
		ClientID: c.idx,
		Input:    linearize.KvInput{Op: uint8(op.Type), Key: op.Key, Value: op.Value},
		Call:     s.nextStamp(),
		Return:   linearize.Pending,
	})
	c.sendAttempt()
}

func (c *Client) pickServer() raft.NodeID {
	if c.leader != raft.None {
		return c.leader
	}
	ids := c.s.nodeIDs
	return ids[c.s.rng.Intn(len(ids))]
}

func (c *Client) sendAttempt() {
	s := c.s
	c.attempt++
	a := c.attempt
	to := c.pickServer()
	s.send(c.addr, to, clientReq{op: *c.cur, attempt: a})
	s.after(s.cfg.ClientTimeout, func() {
		if c.cur != nil && c.attempt == a {
			c.leader = raft.None
			c.sendAttempt()
		}
	})
}

func (c *Client) onResp(r clientResp) {
	s := c.s
	if c.cur == nil || r.seq != c.cur.Seq {
		return // reply to an operation that already completed
	}
	if r.ok {
		// Any successful reply for the current seq is valid, even from an
		// older attempt (deduplication guarantees a single execution).
		op := &s.ops[c.hist]
		op.Output = linearize.KvOutput{Value: r.res.Value}
		op.Return = s.nextStamp()
		s.Stats.OpsCompleted++
		s.traceInts(uint64(c.idx), c.cur.Seq, uint64(len(r.res.Value)))
		c.cur = nil
		s.after(Time(s.rng.Int63n(int64(10*Millisecond))), c.issue)
		return
	}
	if r.attempt != c.attempt {
		return
	}
	// Wrong leader: follow the hint, else try another server after a pause.
	if r.leader != raft.None && r.leader != c.leader {
		c.leader = r.leader
		c.sendAttempt()
		return
	}
	c.leader = raft.None
	c.attempt++ // invalidate the pending timeout
	a := c.attempt
	s.after(10*Millisecond, func() {
		if c.cur != nil && c.attempt == a {
			c.sendAttempt()
		}
	})
}

// ---------- nemesis ----------

func (s *Sim) upNodes() []*Node {
	var out []*Node
	for _, id := range s.nodeIDs {
		if n := s.nodes[id]; n.up && !n.removed {
			out = append(out, n)
		}
	}
	return out
}

func (s *Sim) leader() *Node {
	var best *Node
	for _, id := range s.nodeIDs {
		n := s.nodes[id]
		if n.up && n.core.R.IsLeader() && (best == nil || n.core.R.Term() > best.core.R.Term()) {
			best = n
		}
	}
	return best
}

func (s *Sim) heal() {
	for id := range s.group {
		s.group[id] = 0
	}
	s.Stats.Heals++
	s.logf("nemesis: heal")
}

func (s *Sim) partition() {
	// Random split into two sides; sometimes specifically isolate the leader.
	for _, id := range s.nodeIDs {
		s.group[id] = s.rng.Intn(2)
	}
	if l := s.leader(); l != nil && s.rng.Intn(2) == 0 {
		for _, id := range s.nodeIDs {
			s.group[id] = 0
		}
		s.group[l.id] = 1
	}
	s.Stats.Partitions++
	s.logf("nemesis: partition %v", s.group)
}

func (s *Sim) nemesis() {
	if s.now >= s.cfg.ChaosDuration {
		return
	}
	defer s.after(Time(s.rng.Int63n(int64(2*s.cfg.FaultEvery))), s.nemesis)
	var actions []func()
	if s.cfg.Partitions {
		actions = append(actions, s.partition, s.heal)
	}
	if s.cfg.Unreliable {
		actions = append(actions, func() { s.flaky = !s.flaky; s.logf("nemesis: flaky=%v", s.flaky) })
	}
	if s.cfg.Crashes {
		actions = append(actions,
			func() { // crash a random node (or the leader)
				ups := s.upNodes()
				if len(ups) == 0 {
					return
				}
				n := ups[s.rng.Intn(len(ups))]
				if l := s.leader(); l != nil && s.rng.Intn(3) == 0 {
					n = l
				}
				n.crash()
				s.Stats.Crashes++
				s.logf("nemesis: crash %d", n.id)
			},
			s.restartOne,
		)
	}
	if s.cfg.Membership {
		actions = append(actions, s.addServer, s.removeServer)
	}
	if s.cfg.LeaderChurn {
		// Leader killer: replaces the regular mix.
		if l := s.leader(); l != nil && s.rng.Intn(2) == 0 {
			l.crash()
			s.Stats.Crashes++
			s.logf("nemesis: crash leader %d", l.id)
		}
		if len(s.upNodes())*2 <= s.cfg.Nodes {
			s.restartOne()
		}
		return
	}
	if len(actions) > 0 {
		actions[s.rng.Intn(len(actions))]()
	}
}

func (s *Sim) restartOne() {
	var down []*Node
	for _, id := range s.nodeIDs {
		if n := s.nodes[id]; !n.up && !n.removed {
			down = append(down, n)
		}
	}
	if len(down) == 0 {
		return
	}
	n := down[s.rng.Intn(len(down))]
	n.start()
	s.Stats.Restarts++
	s.logf("nemesis: restart %d", n.id)
}

func (s *Sim) addServer() {
	l := s.leader()
	if l == nil || len(l.core.R.Members()) >= 7 {
		return
	}
	id := s.nextNode
	s.nextNode++
	n := s.newNode(id, nil)
	n.start()
	// Half the time join as a learner (auto-promoted once caught up),
	// otherwise as a voter straight away.
	learner := s.rng.Intn(2) == 0
	l.core.SubmitConfChange(true, raft.Member{ID: id, Learner: learner}, func(_ []byte, err error) {
		s.logf("add server %d: %v", id, err)
	})
	l.flush()
	s.Stats.AddServer++
}

func (s *Sim) removeServer() {
	l := s.leader()
	if l == nil {
		return
	}
	ms := l.core.R.Members()
	if len(ms) <= 3 {
		return
	}
	victim := ms[s.rng.Intn(len(ms))].ID
	l.core.SubmitConfChange(false, raft.Member{ID: victim}, func(_ []byte, err error) {
		s.logf("remove server %d: %v", victim, err)
		if err == nil {
			s.nodes[victim].removed = true
		}
	})
	l.flush()
	s.Stats.RemoveServer++
}

// ---------- running ----------

// Result is the outcome of a run.
type Result struct {
	Seed    int64
	Err     error // safety or liveness violation (nil if none)
	History []linearize.Operation
	Trace   uint64 // hash of the full execution, for determinism checks
	Stats   Stats

	SimTime, CheckTime time.Duration // wall-clock cost (set by tests)
	Inconclusive       bool          // the linearizability check timed out
}

func (s *Sim) runUntil(t Time, done func() bool) {
	for s.q.Len() > 0 && s.err == nil {
		e := s.q.peek()
		if e.at > t || (done != nil && done()) {
			break
		}
		heap.Pop(&s.q)
		s.now = e.at
		s.Stats.Events++
		e.fn()
	}
	if s.now < t && (done == nil || !done()) {
		s.now = t
	}
}

func (s *Sim) outstanding() bool {
	for _, c := range s.clients {
		if c.cur != nil {
			return true
		}
	}
	return false
}

// Run executes the simulation: a chaos phase with faults, then a quiet
// phase in which the cluster is healed and all client operations must
// complete.
func (s *Sim) Run() Result {
	if s.cfg.Crashes || s.cfg.Partitions || s.cfg.Unreliable || s.cfg.Membership || s.cfg.LeaderChurn {
		s.after(s.cfg.FaultEvery, s.nemesis)
	}
	s.runUntil(s.cfg.ChaosDuration, nil)

	// Heal everything and let the system recover.
	s.heal()
	s.flaky = false
	for _, id := range s.nodeIDs {
		if n := s.nodes[id]; !n.up {
			n.start()
			s.Stats.Restarts++
		}
	}
	s.runUntil(s.now+s.cfg.QuietDuration/5, nil)
	s.stopOps = true
	deadline := s.now + s.cfg.QuietDuration
	s.runUntil(deadline, func() bool { return !s.outstanding() })
	if s.err == nil && s.outstanding() {
		s.fail("liveness: client operations still pending %.1fs after the cluster was healed\n%s", float64(s.cfg.QuietDuration)/float64(Second), s.describeNodes())
	}
	if s.err == nil {
		s.checkConvergence()
	}
	for _, op := range s.ops {
		if op.Return == linearize.Pending {
			s.Stats.OpsPending++
		}
	}
	return Result{Seed: s.cfg.Seed, Err: s.err, History: s.ops, Trace: s.trace.Sum64(), Stats: s.Stats}
}

// describeNodes summarises every node, for liveness failure reports.
func (s *Sim) describeNodes() string {
	var b []byte
	for _, id := range s.nodeIDs {
		n := s.nodes[id]
		if !n.up {
			b = fmt.Appendf(b, "  node %d: down\n", id)
			continue
		}
		st := n.core.R.Status()
		b = fmt.Appendf(b, "  node %d: %s term=%d leader=%d vote=%d commit=%d last=%d/%d removed=%v members=%v\n",
			id, st.State, st.Term, st.Leader, st.VotedFor, st.Commit, st.LastIndex, st.LastTerm, n.removed, st.Members)
	}
	return string(b)
}

// checkConvergence waits for every member to apply the leader's commit
// index and compares state machine contents.
func (s *Sim) checkConvergence() {
	var l *Node
	var members []raft.Member
	var commit uint64
	caughtUp := func() bool {
		l = s.leader()
		if l == nil {
			return false
		}
		members = l.core.R.Members()
		commit = l.core.R.Commit()
		for _, m := range members {
			n := s.nodes[m.ID]
			if !n.up || n.core.Applied() < commit {
				return false
			}
		}
		return true
	}
	s.runUntil(s.now+5*Second, caughtUp)
	if !caughtUp() {
		s.fail("liveness: members did not catch up to the leader's commit index")
		return
	}
	want := l.store.Snapshot()
	for _, m := range members {
		n := s.nodes[m.ID]
		if n.core.Applied() == l.core.Applied() && string(n.store.Snapshot()) != string(want) {
			s.fail("divergence: node %d state differs from leader %d at index %d", n.id, l.id, n.core.Applied())
		}
	}
}

// CheckLinearizable runs the Porcupine-style checker on a history. Gets
// whose outcome is unknown are dropped (they have no effect).
func CheckLinearizable(h []linearize.Operation, timeout time.Duration) (linearize.Result, string) {
	var ops []linearize.Operation
	for _, op := range h {
		if op.Return == linearize.Pending && op.Input.(linearize.KvInput).Op == uint8(kv.OpGet) {
			continue
		}
		if op.Output == nil {
			op.Output = linearize.KvOutput{}
		}
		ops = append(ops, op)
	}
	res, info := linearize.Check(linearize.KvModel, ops, timeout)
	if res == linearize.Illegal {
		return res, linearize.Describe(linearize.KvModel, info.FailedPartition, info.Longest)
	}
	return res, ""
}

// SortedIDs is a helper for deterministic iteration in tests.
func SortedIDs(m map[raft.NodeID]*Node) []raft.NodeID {
	ids := make([]raft.NodeID, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
