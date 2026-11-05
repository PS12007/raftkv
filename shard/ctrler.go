// Package shard implements a sharded, multi-group raftkv: a shard
// controller (itself a Raft-replicated state machine) assigns each of
// NShards key shards to a replica group, and every replica group is a
// separate Raft cluster that serves its shards and migrates them, together
// with their deduplication state, when the configuration changes.
//
// The design follows MIT 6.5840's sharded KV lab: groups advance through
// configurations strictly one at a time, pull incoming shards from the
// previous owner, and tell the previous owner to garbage-collect them once
// installed. All of it runs (and is checked for linearizability) inside the
// deterministic simulator in this package.
package shard

import (
	"encoding/json"
	"hash/fnv"
	"sort"
)

// NShards is the number of key shards.
const NShards = 10

// Key2Shard maps a key to its shard.
func Key2Shard(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % NShards)
}

// Config is one numbered shard assignment.
type Config struct {
	Num    int
	Shards [NShards]int     // shard -> gid (0 = unassigned)
	Groups map[int][]uint64 // gid -> server endpoints
}

func (c Config) clone() Config {
	n := Config{Num: c.Num, Shards: c.Shards, Groups: map[int][]uint64{}}
	for g, s := range c.Groups {
		n.Groups[g] = append([]uint64(nil), s...)
	}
	return n
}

// CtrlOp is a controller command.
type CtrlOp struct {
	Type     string           // "join", "leave", "move", "query"
	Servers  map[int][]uint64 `json:",omitempty"` // join
	GIDs     []int            `json:",omitempty"` // leave
	Shard    int              `json:",omitempty"` // move
	GID      int              `json:",omitempty"` // move
	Num      int              `json:",omitempty"` // query (-1 = latest)
	ClientID uint64
	Seq      uint64
}

// Ctrler is the controller state machine (implements rsm.StateMachine).
type Ctrler struct {
	Configs []Config
	Last    map[uint64]uint64 // client -> last applied seq (writes only)
}

// NewCtrler returns a controller with the empty initial config.
func NewCtrler() *Ctrler {
	return &Ctrler{Configs: []Config{{Groups: map[int][]uint64{}}}, Last: map[uint64]uint64{}}
}

// Apply implements rsm.StateMachine. Query returns a JSON Config.
func (c *Ctrler) Apply(cmd []byte) []byte {
	var op CtrlOp
	if err := json.Unmarshal(cmd, &op); err != nil {
		panic(err)
	}
	if op.Type == "query" {
		n := op.Num
		if n < 0 || n >= len(c.Configs) {
			n = len(c.Configs) - 1
		}
		b, _ := json.Marshal(c.Configs[n])
		return b
	}
	if op.ClientID != 0 && op.Seq <= c.Last[op.ClientID] {
		return nil // duplicate
	}
	c.Last[op.ClientID] = op.Seq
	next := c.Configs[len(c.Configs)-1].clone()
	next.Num++
	switch op.Type {
	case "join":
		for g, s := range op.Servers {
			next.Groups[g] = append([]uint64(nil), s...)
		}
		rebalance(&next)
	case "leave":
		for _, g := range op.GIDs {
			delete(next.Groups, g)
			for s := range next.Shards {
				if next.Shards[s] == g {
					next.Shards[s] = 0
				}
			}
		}
		rebalance(&next)
	case "move":
		if _, ok := next.Groups[op.GID]; ok && op.Shard >= 0 && op.Shard < NShards {
			next.Shards[op.Shard] = op.GID
		}
	}
	c.Configs = append(c.Configs, next)
	return nil
}

// rebalance spreads shards evenly over groups, moving as few as possible.
// It is deterministic (no map iteration order dependence).
func rebalance(c *Config) {
	var gids []int
	for g := range c.Groups {
		gids = append(gids, g)
	}
	sort.Ints(gids)
	if len(gids) == 0 {
		c.Shards = [NShards]int{}
		return
	}
	owned := map[int][]int{}
	var free []int
	for s, g := range c.Shards {
		if _, ok := c.Groups[g]; ok {
			owned[g] = append(owned[g], s)
		} else {
			free = append(free, s)
		}
	}
	// Order groups by load (desc), then gid, so the extra shards go to the
	// currently most loaded groups and fewer shards move.
	order := append([]int(nil), gids...)
	sort.SliceStable(order, func(i, j int) bool { return len(owned[order[i]]) > len(owned[order[j]]) })
	base, extra := NShards/len(gids), NShards%len(gids)
	target := map[int]int{}
	for i, g := range order {
		target[g] = base
		if i < extra {
			target[g]++
		}
	}
	for _, g := range order {
		for len(owned[g]) > target[g] {
			last := owned[g][len(owned[g])-1]
			owned[g] = owned[g][:len(owned[g])-1]
			free = append(free, last)
		}
	}
	sort.Ints(free)
	for _, g := range order {
		for len(owned[g]) < target[g] {
			s := free[0]
			free = free[1:]
			owned[g] = append(owned[g], s)
			c.Shards[s] = g
		}
	}
}

// Snapshot implements rsm.StateMachine (JSON is deterministic: map keys
// are sorted).
func (c *Ctrler) Snapshot() []byte {
	b, _ := json.Marshal(c)
	return b
}

// Restore implements rsm.StateMachine.
func (c *Ctrler) Restore(data []byte) {
	*c = *NewCtrler()
	if len(data) > 0 {
		if err := json.Unmarshal(data, c); err != nil {
			panic(err)
		}
	}
}
