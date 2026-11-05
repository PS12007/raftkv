package shard

import (
	"encoding/json"

	"github.com/ps12007/raftkv/kv"
)

// ShardState is a shard's lifecycle state within one group.
type ShardState int

const (
	// Absent: not ours.
	Absent ShardState = iota
	// Serving: ours, accepting client operations.
	Serving
	// Pulling: ours in the current config, waiting for data from the
	// previous owner.
	Pulling
	// BePulled: ours in the previous config only; frozen until the new owner
	// has installed it and tells us to delete it.
	BePulled
)

// Session is a per-client deduplication record; it migrates with the shard.
type Session struct {
	Seq    uint64
	Result kv.Result
}

// ShardData is one shard's contents.
type ShardData struct {
	State ShardState
	Data  map[string]string
	Dedup map[uint64]Session
}

func newShard(st ShardState) *ShardData {
	return &ShardData{State: st, Data: map[string]string{}, Dedup: map[uint64]Session{}}
}

func (s *ShardData) clone() *ShardData {
	c := newShard(s.State)
	for k, v := range s.Data {
		c.Data[k] = v
	}
	for k, v := range s.Dedup {
		c.Dedup[k] = v
	}
	return c
}

// GroupCmd is a command in a replica group's log.
type GroupCmd struct {
	Type  string     // "op", "config", "install", "delete", "gcdone"
	Op    kv.Op      `json:",omitempty"`
	Cfg   Config     `json:",omitempty"`
	Num   int        `json:",omitempty"`
	Shard int        `json:",omitempty"`
	Data  *ShardData `json:",omitempty"`
}

// GroupResult is the result of a GroupCmd.
type GroupResult struct {
	Err    string // "", "WrongGroup"
	Result kv.Result
}

// Group is a replica group's state machine (implements rsm.StateMachine).
type Group struct {
	GID    int
	Cfg    Config
	Prev   Config
	Shards [NShards]*ShardData
	// GC lists shards we installed whose previous owner has not yet
	// confirmed deletion: shard -> config num.
	GC map[int]int
}

// NewGroup returns an empty group state machine.
func NewGroup(gid int) *Group {
	g := &Group{GID: gid, Cfg: Config{Groups: map[int][]uint64{}}, Prev: Config{Groups: map[int][]uint64{}}, GC: map[int]int{}}
	for i := range g.Shards {
		g.Shards[i] = newShard(Absent)
	}
	return g
}

// Stable reports whether the group may move to the next configuration.
func (g *Group) Stable() bool {
	for _, s := range g.Shards {
		if s.State == Pulling || s.State == BePulled {
			return false
		}
	}
	return len(g.GC) == 0
}

func encodeResult(r GroupResult) []byte {
	b, _ := json.Marshal(r)
	return b
}

// Apply implements rsm.StateMachine.
func (g *Group) Apply(b []byte) []byte {
	var c GroupCmd
	if err := json.Unmarshal(b, &c); err != nil {
		panic(err)
	}
	switch c.Type {
	case "op":
		return encodeResult(g.applyOp(c.Op))
	case "config":
		g.applyConfig(c.Cfg)
	case "install":
		s := g.Shards[c.Shard]
		if c.Num == g.Cfg.Num && s.State == Pulling {
			d := c.Data.clone()
			d.State = Serving
			g.Shards[c.Shard] = d
			g.GC[c.Shard] = c.Num
		}
	case "delete":
		s := g.Shards[c.Shard]
		if c.Num == g.Cfg.Num && s.State == BePulled {
			g.Shards[c.Shard] = newShard(Absent)
		}
	case "gcdone":
		if n, ok := g.GC[c.Shard]; ok && n == c.Num {
			delete(g.GC, c.Shard)
		}
	}
	return nil
}

func (g *Group) applyOp(op kv.Op) GroupResult {
	sh := Key2Shard(op.Key)
	s := g.Shards[sh]
	if g.Cfg.Shards[sh] != g.GID || s.State != Serving {
		return GroupResult{Err: "WrongGroup"}
	}
	if sess, ok := s.Dedup[op.ClientID]; ok && op.Seq <= sess.Seq {
		return GroupResult{Result: sess.Result}
	}
	var res kv.Result
	switch op.Type {
	case kv.OpGet:
		res.Value, res.Found = s.Data[op.Key]
	case kv.OpPut:
		s.Data[op.Key] = op.Value
	case kv.OpAppend:
		s.Data[op.Key] += op.Value
	case kv.OpDelete:
		_, res.Found = s.Data[op.Key]
		delete(s.Data, op.Key)
	}
	s.Dedup[op.ClientID] = Session{Seq: op.Seq, Result: res}
	return GroupResult{Result: res}
}

func (g *Group) applyConfig(next Config) {
	if next.Num != g.Cfg.Num+1 || !g.Stable() {
		return
	}
	for sh := 0; sh < NShards; sh++ {
		was, now := g.Cfg.Shards[sh], next.Shards[sh]
		switch {
		case now == g.GID && was != g.GID:
			if was == 0 {
				g.Shards[sh] = newShard(Serving) // brand new shard
			} else {
				g.Shards[sh].State = Pulling
			}
		case was == g.GID && now != g.GID:
			if now == 0 {
				g.Shards[sh] = newShard(Absent) // no owner: data is dropped
			} else {
				g.Shards[sh].State = BePulled
			}
		}
	}
	g.Prev, g.Cfg = g.Cfg, next.clone()
}

// PullShard answers a new owner's request for shard data in config num. It
// does not go through the log: BePulled shards are frozen, so any replica
// that has applied exactly config num holds the same data.
func (g *Group) PullShard(num, sh int) (*ShardData, bool) {
	if g.Cfg.Num != num || g.Shards[sh].State != BePulled {
		return nil, false
	}
	d := g.Shards[sh].clone()
	return d, true
}

// Snapshot implements rsm.StateMachine.
func (g *Group) Snapshot() []byte {
	b, _ := json.Marshal(g)
	return b
}

// Restore implements rsm.StateMachine.
func (g *Group) Restore(data []byte) {
	gid := g.GID
	*g = *NewGroup(gid)
	if len(data) > 0 {
		if err := json.Unmarshal(data, g); err != nil {
			panic(err)
		}
	}
}
