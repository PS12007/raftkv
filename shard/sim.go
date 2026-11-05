package shard

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"

	"github.com/ps12007/raftkv/kv"
	"github.com/ps12007/raftkv/linearize"
	"github.com/ps12007/raftkv/raft"
	"github.com/ps12007/raftkv/rsm"
)

// Time is virtual time in microseconds.
type Time = int64

const (
	ms  Time = 1000
	sec Time = 1000 * ms
)

// SimConfig configures a sharded simulation.
type SimConfig struct {
	Seed          int64
	Groups        int // replica groups (each 3 servers)
	Clients       int
	Keys          int
	ChaosDuration Time
	QuietDuration Time
	Crashes       bool
	Partitions    bool
	Unreliable    bool
	Reconfigure   bool // join/leave/move groups during the run
	MaxLogBytes   int
	Logf          func(string, ...any)
}

// DefaultSimConfig enables every fault.
func DefaultSimConfig(seed int64) SimConfig {
	return SimConfig{Seed: seed, Groups: 3, Clients: 5, Keys: 12, ChaosDuration: 8 * sec, QuietDuration: 20 * sec,
		Crashes: true, Partitions: true, Unreliable: true, Reconfigure: true, MaxLogBytes: 8 << 10}
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
func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *eventQueue) Push(x any)   { *q = append(*q, x.(*event)) }
func (q *eventQueue) Pop() any     { o := *q; e := o[len(o)-1]; *q = o[:len(o)-1]; return e }

// SimStats summarises a run.
type SimStats struct {
	Ops, Configs, Migrations, Crashes, Partitions int
}

// World is a simulated sharded deployment.
type World struct {
	cfg   SimConfig
	rng   *rand.Rand
	now   Time
	q     eventQueue
	seq   uint64
	stamp int64

	ends    map[uint64]endpoint
	servers []*Server // all servers, deterministic order
	ctrl    []uint64
	groups  map[int][]uint64
	gids    []int
	part    map[uint64]int
	flaky   bool

	clients []*Client
	admin   *Client
	joined  map[int]bool
	ops     []linearize.Operation
	stopOps bool
	err     error
	Stats   SimStats
}

type endpoint interface{ deliver(from uint64, p any) }

func (w *World) after(d Time, fn func()) {
	w.seq++
	heap.Push(&w.q, &event{at: w.now + d, seq: w.seq, fn: fn})
}

func (w *World) logf(f string, a ...any) {
	if w.cfg.Logf != nil {
		w.cfg.Logf("%8.3fs "+f, append([]any{float64(w.now) / float64(sec)}, a...)...)
	}
}

const clientBase = 1 << 20

func (w *World) send(from, to uint64, p any) {
	if from < clientBase && to < clientBase && w.part[from] != w.part[to] {
		return
	}
	if w.flaky && w.rng.Float64() < 0.1 {
		return
	}
	n := 1
	if w.flaky && w.rng.Float64() < 0.05 {
		n = 2
	}
	for i := 0; i < n; i++ {
		d := 100 + Time(w.rng.Int63n(int64(3*ms)))
		if w.flaky && w.rng.Float64() < 0.05 {
			d += Time(w.rng.Int63n(int64(150 * ms)))
		}
		w.after(d, func() {
			if from < clientBase && to < clientBase && w.part[from] != w.part[to] {
				return
			}
			if e := w.ends[to]; e != nil {
				e.deliver(from, p)
			}
		})
	}
}

// ---------- RPC over the simulated network ----------

type rpcReq struct {
	id   uint64
	body any
}
type rpcResp struct {
	id   uint64
	body any
}

type caller struct {
	w       *World
	addr    uint64
	next    uint64
	pending map[uint64]func(any, bool)
	dead    bool
}

func newCaller(w *World, addr uint64) *caller {
	return &caller{w: w, addr: addr, pending: map[uint64]func(any, bool){}}
}

func (c *caller) call(to uint64, body any, timeout Time, cb func(any, bool)) {
	if c.dead {
		return
	}
	c.next++
	id := c.next
	c.pending[id] = cb
	c.w.send(c.addr, to, rpcReq{id: id, body: body})
	c.w.after(timeout, func() {
		if f, ok := c.pending[id]; ok && !c.dead {
			delete(c.pending, id)
			f(nil, false)
		}
	})
}

func (c *caller) onResp(r rpcResp) {
	if f, ok := c.pending[r.id]; ok && !c.dead {
		delete(c.pending, r.id)
		f(r.body, true)
	}
}

// Request/response bodies.
type ctrlReq struct{ op CtrlOp }
type ctrlResp struct {
	wrongLeader bool
	cfg         Config
}
type kvReq struct{ op kv.Op }
type kvResp struct {
	wrongLeader bool
	wrongGroup  bool
	res         kv.Result
}
type pullReq struct{ num, shard int }
type pullResp struct {
	ok   bool
	data *ShardData
}
type deleteReq struct{ num, shard int }
type deleteResp struct{ ok bool }

// ---------- servers ----------

// Server is a controller or replica-group server.
type Server struct {
	w       *World
	id      uint64
	gid     int // 0 for controller servers
	peers   []raft.Member
	storage *raft.MemStorage
	core    *rsm.Core
	group   *Group
	up      bool
	gen     int
	rpc     *caller

	// leader background state
	querying bool
	pulling  map[int]bool
	gcing    map[int]bool
	ctrlIdx  int
}

func (w *World) newServer(id uint64, gid int, peers []raft.Member) *Server {
	s := &Server{w: w, id: id, gid: gid, peers: peers, storage: raft.NewMemStorage()}
	w.ends[id] = s
	w.servers = append(w.servers, s)
	return s
}

func (s *Server) start() {
	w := s.w
	r, err := raft.New(raft.Config{ID: s.id, Peers: s.peers, Storage: s.storage, ElectionTicks: 10, HeartbeatTicks: 3,
		PreVote: true, Rand: rand.New(rand.NewSource(w.rng.Int63()))})
	if err != nil {
		panic(err)
	}
	var sm rsm.StateMachine
	if s.gid == 0 {
		sm = NewCtrler()
	} else {
		s.group = NewGroup(s.gid)
		sm = s.group
	}
	s.core = rsm.New(r, sm, w.cfg.MaxLogBytes)
	s.up = true
	s.gen++
	s.rpc = newCaller(w, s.id)
	s.pulling, s.gcing, s.querying = map[int]bool{}, map[int]bool{}, false
	gen := s.gen
	ticks := 0
	var tick func()
	tick = func() {
		if !s.up || s.gen != gen {
			return
		}
		s.core.R.Tick()
		ticks++
		if s.group != nil && ticks%5 == 0 && s.core.R.IsLeader() {
			s.background()
		}
		s.flush()
		w.after(9*ms+Time(w.rng.Int63n(int64(2*ms))), tick)
	}
	w.after(Time(w.rng.Int63n(int64(10*ms))), tick)
	s.flush()
}

func (s *Server) crash() {
	s.up = false
	s.rpc.dead = true
	s.core = nil
	s.group = nil
}

func (s *Server) flush() {
	for _, m := range s.core.Flush() {
		s.w.send(s.id, m.To, m)
	}
}

func (s *Server) submit(cmd []byte, cb rsm.Callback) {
	s.core.Submit(cmd, cb)
}

func groupCmd(c GroupCmd) []byte {
	b, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	return b
}

func (s *Server) deliver(from uint64, p any) {
	if !s.up {
		return
	}
	switch m := p.(type) {
	case raft.Message:
		s.core.R.Step(m)
	case rpcResp:
		s.rpc.onResp(m)
	case rpcReq:
		reply := func(body any) { s.w.send(s.id, from, rpcResp{id: m.id, body: body}) }
		switch req := m.body.(type) {
		case ctrlReq:
			if s.gid != 0 {
				return
			}
			b, _ := json.Marshal(req.op)
			s.submit(b, func(res []byte, err error) {
				if err != nil {
					reply(ctrlResp{wrongLeader: true})
					return
				}
				var cfg Config
				if req.op.Type == "query" {
					json.Unmarshal(res, &cfg)
				}
				reply(ctrlResp{cfg: cfg})
			})
		case kvReq:
			if s.gid == 0 {
				return
			}
			s.submit(groupCmd(GroupCmd{Type: "op", Op: req.op}), func(res []byte, err error) {
				if err != nil {
					reply(kvResp{wrongLeader: true})
					return
				}
				var gr GroupResult
				json.Unmarshal(res, &gr)
				reply(kvResp{wrongGroup: gr.Err != "", res: gr.Result})
			})
		case pullReq:
			if s.group == nil {
				return
			}
			d, ok := s.group.PullShard(req.num, req.shard)
			reply(pullResp{ok: ok, data: d})
		case deleteReq:
			if s.group == nil {
				return
			}
			if s.group.Cfg.Num > req.num {
				reply(deleteResp{ok: true}) // already past that config
				break
			}
			g := s.group
			s.submit(groupCmd(GroupCmd{Type: "delete", Num: req.num, Shard: req.shard}), func(_ []byte, err error) {
				reply(deleteResp{ok: err == nil && (g.Cfg.Num > req.num || g.Shards[req.shard].State == Absent)})
			})
		}
	}
	s.flush()
}

// background is run periodically by a group leader: advance configuration,
// pull incoming shards, and garbage-collect shards at their old owner.
func (s *Server) background() {
	w, g, gen := s.w, s.group, s.gen
	alive := func() bool { return s.up && s.gen == gen && s.core.R.IsLeader() }
	if g.Stable() && !s.querying {
		s.querying = true
		to := w.ctrl[s.ctrlIdx%len(w.ctrl)]
		num := g.Cfg.Num + 1
		s.rpc.call(to, ctrlReq{op: CtrlOp{Type: "query", Num: num}}, 100*ms, func(r any, ok bool) {
			s.querying = false
			if !alive() {
				return
			}
			resp, _ := r.(ctrlResp)
			if !ok || resp.wrongLeader {
				s.ctrlIdx++
				return
			}
			if resp.cfg.Num == num && g.Cfg.Num == num-1 {
				s.submit(groupCmd(GroupCmd{Type: "config", Cfg: resp.cfg}), func([]byte, error) {})
			}
		})
	}
	for sh := 0; sh < NShards; sh++ {
		sh := sh
		if g.Shards[sh].State == Pulling && !s.pulling[sh] {
			owner := g.Prev.Shards[sh]
			srvs := g.Prev.Groups[owner]
			num := g.Cfg.Num
			s.pulling[sh] = true
			to := srvs[w.rng.Intn(len(srvs))]
			s.rpc.call(to, pullReq{num: num, shard: sh}, 100*ms, func(r any, ok bool) {
				s.pulling[sh] = false
				resp, _ := r.(pullResp)
				if !alive() || !ok || !resp.ok || g.Cfg.Num != num {
					return
				}
				s.submit(groupCmd(GroupCmd{Type: "install", Num: num, Shard: sh, Data: resp.data}), func([]byte, error) {})
				w.Stats.Migrations++
			})
		}
	}
	var gcs []int
	for sh := range g.GC {
		gcs = append(gcs, sh)
	}
	sort.Ints(gcs)
	for _, sh := range gcs {
		sh, num := sh, g.GC[sh]
		if s.gcing[sh] {
			continue
		}
		owner := g.Prev.Shards[sh]
		srvs := g.Prev.Groups[owner]
		s.gcing[sh] = true
		to := srvs[w.rng.Intn(len(srvs))]
		s.rpc.call(to, deleteReq{num: num, shard: sh}, 150*ms, func(r any, ok bool) {
			s.gcing[sh] = false
			resp, _ := r.(deleteResp)
			if !alive() || !ok || !resp.ok {
				return
			}
			s.submit(groupCmd(GroupCmd{Type: "gcdone", Num: num, Shard: sh}), func([]byte, error) {})
		})
	}
}

// ---------- clients ----------

// Client is a sharded KV client (or the admin issuing join/leave/move).
type Client struct {
	w       *World
	idx     int
	addr    uint64
	cid     uint64
	seq     uint64
	cfg     Config
	leader  map[int]int
	ctrlIdx int
	rpc     *caller
	attempt uint64
	cur     *kv.Op
	hist    int
	busy    bool // admin: op in flight
}

func (w *World) newClient(idx int) *Client {
	c := &Client{w: w, idx: idx, addr: clientBase + uint64(idx), cid: uint64(idx + 1), leader: map[int]int{},
		cfg: Config{Groups: map[int][]uint64{}}}
	c.rpc = newCaller(w, c.addr)
	w.ends[c.addr] = c
	return c
}

func (c *Client) deliver(_ uint64, p any) {
	if r, ok := p.(rpcResp); ok {
		c.rpc.onResp(r)
	}
}

// ctrlCall retries a controller op until it succeeds.
func (c *Client) ctrlCall(op CtrlOp, done func(Config)) {
	to := c.w.ctrl[c.ctrlIdx%len(c.w.ctrl)]
	c.rpc.call(to, ctrlReq{op: op}, 100*ms, func(r any, ok bool) {
		resp, _ := r.(ctrlResp)
		if !ok || resp.wrongLeader {
			c.ctrlIdx++
			c.w.after(5*ms, func() { c.ctrlCall(op, done) })
			return
		}
		done(resp.cfg)
	})
}

func (c *Client) issue() {
	w := c.w
	if w.stopOps {
		return
	}
	c.seq++
	key := fmt.Sprintf("k%d", w.rng.Intn(w.cfg.Keys))
	op := kv.Op{Key: key, ClientID: c.cid, Seq: c.seq}
	val := fmt.Sprintf("<%d.%d>", c.idx, c.seq)
	switch x := w.rng.Intn(10); {
	case x < 4:
		op.Type = kv.OpGet
	case x < 6:
		op.Type, op.Value = kv.OpPut, val
	default:
		op.Type, op.Value = kv.OpAppend, val
	}
	c.cur = &op
	c.hist = len(w.ops)
	w.ops = append(w.ops, linearize.Operation{ClientID: c.idx,
		Input: linearize.KvInput{Op: uint8(op.Type), Key: key, Value: op.Value},
		Call:  w.nextStamp(), Return: linearize.Pending})
	c.send()
}

func (w *World) nextStamp() int64 { w.stamp++; return w.stamp }

func (c *Client) refresh() {
	seq := c.cur.Seq
	c.ctrlCall(CtrlOp{Type: "query", Num: -1}, func(cfg Config) {
		c.cfg = cfg
		if c.cur != nil && c.cur.Seq == seq {
			c.send()
		}
	})
}

func (c *Client) send() {
	w := c.w
	sh := Key2Shard(c.cur.Key)
	gid := c.cfg.Shards[sh]
	srvs := c.cfg.Groups[gid]
	if gid == 0 || len(srvs) == 0 {
		w.after(20*ms, c.refresh)
		return
	}
	c.attempt++
	a, seq := c.attempt, c.cur.Seq
	to := srvs[c.leader[gid]%len(srvs)]
	c.rpc.call(to, kvReq{op: *c.cur}, 200*ms, func(r any, ok bool) {
		if c.cur == nil || c.cur.Seq != seq {
			return
		}
		resp, _ := r.(kvResp)
		if ok && !resp.wrongLeader && !resp.wrongGroup {
			op := &w.ops[c.hist]
			op.Output = linearize.KvOutput{Value: resp.res.Value}
			op.Return = w.nextStamp()
			w.Stats.Ops++
			c.cur = nil
			w.after(Time(w.rng.Int63n(int64(10*ms))), c.issue)
			return
		}
		if a != c.attempt {
			return
		}
		if ok && resp.wrongGroup {
			c.refresh()
			return
		}
		c.leader[gid]++
		if c.leader[gid]%len(srvs) == 0 {
			// Tried every server in the group: maybe the config moved on.
			w.after(10*ms, c.refresh)
			return
		}
		c.send()
	})
}

// ---------- nemesis and run ----------

// NewWorld builds a sharded deployment: 3 controller servers and
// cfg.Groups replica groups of 3 servers.
func NewWorld(cfg SimConfig) *World {
	w := &World{cfg: cfg, rng: rand.New(rand.NewSource(cfg.Seed)), ends: map[uint64]endpoint{},
		groups: map[int][]uint64{}, part: map[uint64]int{}, joined: map[int]bool{}}
	id := uint64(1)
	mk := func(gid int) []uint64 {
		var ms []raft.Member
		var ids []uint64
		for i := 0; i < 3; i++ {
			ms = append(ms, raft.Member{ID: id})
			ids = append(ids, id)
			id++
		}
		for _, x := range ids {
			w.newServer(x, gid, ms).start()
		}
		return ids
	}
	w.ctrl = mk(0)
	for g := 0; g < cfg.Groups; g++ {
		gid := 100 + g
		w.gids = append(w.gids, gid)
		w.groups[gid] = mk(gid)
	}
	w.admin = w.newClient(1000)
	for i := 0; i < cfg.Clients; i++ {
		c := w.newClient(i)
		w.clients = append(w.clients, c)
	}
	return w
}

func (w *World) adminOp(op CtrlOp, then func()) {
	a := w.admin
	if a.busy {
		return
	}
	a.busy = true
	a.seq++
	op.ClientID, op.Seq = a.cid, a.seq
	a.ctrlCall(op, func(Config) {
		a.busy = false
		w.Stats.Configs++
		w.logf("admin: %s %v %v done", op.Type, op.GIDs, op.Shard)
		if then != nil {
			then()
		}
	})
}

func (w *World) join(gid int) {
	w.joined[gid] = true
	w.adminOp(CtrlOp{Type: "join", Servers: map[int][]uint64{gid: w.groups[gid]}}, nil)
}

func (w *World) nemesis() {
	if w.now >= w.cfg.ChaosDuration {
		return
	}
	defer w.after(Time(w.rng.Int63n(int64(600*ms))), w.nemesis)
	var acts []func()
	if w.cfg.Crashes {
		acts = append(acts, func() {
			s := w.servers[w.rng.Intn(len(w.servers))]
			if s.up {
				s.crash()
				w.Stats.Crashes++
				w.logf("crash %d", s.id)
			}
		}, func() {
			for _, s := range w.servers {
				if !s.up {
					s.start()
					w.logf("restart %d", s.id)
					return
				}
			}
		})
	}
	if w.cfg.Partitions {
		acts = append(acts, func() {
			for _, s := range w.servers {
				w.part[s.id] = w.rng.Intn(2)
			}
			w.Stats.Partitions++
		}, func() { w.part = map[uint64]int{} })
	}
	if w.cfg.Unreliable {
		acts = append(acts, func() { w.flaky = !w.flaky })
	}
	if len(acts) > 0 {
		acts[w.rng.Intn(len(acts))]()
	}
}

// reconfigurer keeps joining, leaving and moving groups during the chaos
// phase, so shards are constantly migrating while faults happen.
func (w *World) reconfigurer() {
	if w.now >= w.cfg.ChaosDuration {
		return
	}
	defer w.after(100*ms+Time(w.rng.Int63n(int64(400*ms))), w.reconfigurer)
	{
		func() {
			var out, in []int
			for _, g := range w.gids {
				if w.joined[g] {
					in = append(in, g)
				} else {
					out = append(out, g)
				}
			}
			switch x := w.rng.Intn(3); {
			case x == 0 && len(out) > 0:
				w.join(out[w.rng.Intn(len(out))])
			case x == 1 && len(in) > 1:
				g := in[w.rng.Intn(len(in))]
				w.joined[g] = false
				w.adminOp(CtrlOp{Type: "leave", GIDs: []int{g}}, nil)
			case len(in) > 0:
				w.adminOp(CtrlOp{Type: "move", Shard: w.rng.Intn(NShards), GID: in[w.rng.Intn(len(in))]}, nil)
			}
		}()
	}
}

func (w *World) runUntil(t Time, done func() bool) {
	for w.q.Len() > 0 && w.err == nil {
		e := w.q[0]
		if e.at > t || (done != nil && done()) {
			break
		}
		heap.Pop(&w.q)
		w.now = e.at
		e.fn()
	}
	if w.now < t && (done == nil || !done()) {
		w.now = t
	}
}

func (w *World) outstanding() bool {
	for _, c := range w.clients {
		if c.cur != nil {
			return true
		}
	}
	return false
}

// Run executes the simulation and returns the client history.
func (w *World) Run() ([]linearize.Operation, error) {
	w.join(w.gids[0])
	for _, c := range w.clients {
		c := c
		w.after(200*ms+Time(w.rng.Int63n(int64(50*ms))), c.issue)
	}
	w.after(500*ms, w.nemesis)
	if w.cfg.Reconfigure {
		w.after(300*ms, w.reconfigurer)
	}
	w.runUntil(w.cfg.ChaosDuration, nil)
	w.part = map[uint64]int{}
	w.flaky = false
	for _, s := range w.servers {
		if !s.up {
			s.start()
		}
	}
	w.runUntil(w.now+2*sec, nil)
	w.stopOps = true
	w.runUntil(w.now+w.cfg.QuietDuration, func() bool { return !w.outstanding() })
	if w.err == nil && w.outstanding() {
		w.err = fmt.Errorf("seed %d: liveness: client operations pending after healing", w.cfg.Seed)
	}
	return w.ops, w.err
}
