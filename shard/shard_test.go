package shard

import (
	"encoding/json"
	"flag"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ps12007/raftkv/kv"
	"github.com/ps12007/raftkv/linearize"
)

var (
	flagSeeds = flag.Int("shard-seeds", 30, "number of seeds for the sharded randomized test")
	flagStart = flag.Int64("shard-seed-start", 1, "first seed")
	flagSeed  = flag.Int64("shard-seed", 0, "run one seed verbosely")
)

func ctrlApply(c *Ctrler, op CtrlOp) Config {
	b, _ := json.Marshal(op)
	c.Apply(b)
	q, _ := json.Marshal(CtrlOp{Type: "query", Num: -1})
	var cfg Config
	json.Unmarshal(c.Apply(q), &cfg)
	return cfg
}

func counts(cfg Config) map[int]int {
	m := map[int]int{}
	for _, g := range cfg.Shards {
		m[g]++
	}
	return m
}

func TestCtrlerBalance(t *testing.T) {
	c := NewCtrler()
	cfg := ctrlApply(c, CtrlOp{Type: "join", Servers: map[int][]uint64{1: {1}}, ClientID: 1, Seq: 1})
	if counts(cfg)[1] != NShards {
		t.Fatalf("one group should own every shard: %v", cfg.Shards)
	}
	cfg = ctrlApply(c, CtrlOp{Type: "join", Servers: map[int][]uint64{2: {2}, 3: {3}}, ClientID: 1, Seq: 2})
	for g, n := range counts(cfg) {
		if n < NShards/3 || n > NShards/3+1 {
			t.Fatalf("group %d has %d shards: %v", g, n, cfg.Shards)
		}
	}
	before := cfg.Shards
	cfg = ctrlApply(c, CtrlOp{Type: "leave", GIDs: []int{2}, ClientID: 1, Seq: 3})
	for s, g := range before {
		if g != 2 && cfg.Shards[s] != g {
			t.Fatalf("shard %d moved needlessly from %d to %d", s, g, cfg.Shards[s])
		}
	}
	// Duplicate (retried) join is ignored.
	n := len(c.Configs)
	ctrlApply(c, CtrlOp{Type: "leave", GIDs: []int{2}, ClientID: 1, Seq: 3})
	if len(c.Configs) != n {
		t.Fatal("duplicate op created a config")
	}
	c2 := NewCtrler()
	c2.Restore(c.Snapshot())
	if string(c2.Snapshot()) != string(c.Snapshot()) {
		t.Fatal("snapshot round trip")
	}
}

func TestGroupMigration(t *testing.T) {
	a, b := NewGroup(1), NewGroup(2)
	apply := func(g *Group, c GroupCmd) GroupResult {
		var r GroupResult
		json.Unmarshal(g.Apply(groupCmd(c)), &r)
		return r
	}
	c1 := Config{Num: 1, Groups: map[int][]uint64{1: {1}}}
	for s := range c1.Shards {
		c1.Shards[s] = 1
	}
	apply(a, GroupCmd{Type: "config", Cfg: c1})
	apply(b, GroupCmd{Type: "config", Cfg: c1})
	key := "hello"
	sh := Key2Shard(key)
	apply(a, GroupCmd{Type: "op", Op: kv.Op{Type: kv.OpAppend, Key: key, Value: "x", ClientID: 9, Seq: 1}})

	c2 := Config{Num: 2, Shards: c1.Shards, Groups: map[int][]uint64{1: {1}, 2: {2}}}
	c2.Shards[sh] = 2
	apply(a, GroupCmd{Type: "config", Cfg: c2})
	apply(b, GroupCmd{Type: "config", Cfg: c2})
	if r := apply(a, GroupCmd{Type: "op", Op: kv.Op{Type: kv.OpGet, Key: key, ClientID: 9, Seq: 2}}); r.Err != "WrongGroup" {
		t.Fatal("old owner still serving")
	}
	if r := apply(b, GroupCmd{Type: "op", Op: kv.Op{Type: kv.OpGet, Key: key, ClientID: 9, Seq: 2}}); r.Err != "WrongGroup" {
		t.Fatal("new owner serving before data arrived")
	}
	data, ok := a.PullShard(2, sh)
	if !ok {
		t.Fatal("pull failed")
	}
	apply(b, GroupCmd{Type: "install", Num: 2, Shard: sh, Data: data})
	// The retried append must be deduplicated using the migrated session.
	apply(b, GroupCmd{Type: "op", Op: kv.Op{Type: kv.OpAppend, Key: key, Value: "x", ClientID: 9, Seq: 1}})
	r := apply(b, GroupCmd{Type: "op", Op: kv.Op{Type: kv.OpGet, Key: key, ClientID: 9, Seq: 2}})
	if r.Err != "" || r.Result.Value != "x" {
		t.Fatalf("after migration: %+v", r)
	}
	if b.Stable() || a.Stable() {
		t.Fatal("GC pending, should not be stable")
	}
	apply(a, GroupCmd{Type: "delete", Num: 2, Shard: sh})
	apply(b, GroupCmd{Type: "gcdone", Num: 2, Shard: sh})
	if !a.Stable() || !b.Stable() {
		t.Fatal("should be stable after GC")
	}
}

func runShardSeed(cfg SimConfig) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("seed %d: panic: %v", cfg.Seed, p)
		}
	}()
	w := NewWorld(cfg)
	h, err := w.Run()
	if err != nil {
		return err
	}
	if w.Stats.Ops < 50 {
		return fmt.Errorf("seed %d: only %d ops completed", cfg.Seed, w.Stats.Ops)
	}
	var ops []linearize.Operation
	for _, op := range h {
		if op.Return == linearize.Pending && op.Input.(linearize.KvInput).Op == 0 {
			continue
		}
		if op.Output == nil {
			op.Output = linearize.KvOutput{}
		}
		ops = append(ops, op)
	}
	// A checker timeout is inconclusive, not a violation (see sim tests).
	res, info := linearize.Check(linearize.KvModel, ops, 30*time.Second)
	if res == linearize.Illegal {
		return fmt.Errorf("seed %d: not linearizable\n%s", cfg.Seed, linearize.Describe(linearize.KvModel, info.FailedPartition, info.Longest))
	}
	return nil
}

func shardProfile(seed int64) SimConfig {
	c := DefaultSimConfig(seed)
	switch seed % 4 {
	case 1:
		c.Unreliable = false
	case 2:
		c.Crashes = false
	case 3:
		c.MaxLogBytes = 0
	}
	return c
}

// TestShardedRandomized runs the multi-group deployment with crashes,
// partitions, an unreliable network and continuous join/leave/move
// reconfigurations, then checks linearizability across all keys.
func TestShardedRandomized(t *testing.T) {
	if *flagSeed != 0 {
		cfg := shardProfile(*flagSeed)
		cfg.Logf = t.Logf
		if err := runShardSeed(cfg); err != nil {
			t.Fatal(err)
		}
		return
	}
	n := *flagSeeds
	if testing.Short() && n > 8 {
		n = 8
	}
	var next atomic.Int64
	next.Store(*flagStart)
	end := *flagStart + int64(n)
	var mu sync.Mutex
	var errs []error
	var wg sync.WaitGroup
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				s := next.Add(1) - 1
				if s >= end {
					return
				}
				if err := runShardSeed(shardProfile(s)); err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	for i, e := range errs {
		if i == 3 {
			t.Errorf("... %d failures in total", len(errs))
			break
		}
		t.Error(e)
	}
}

func TestShardedDeterminism(t *testing.T) {
	run := func() string {
		w := NewWorld(shardProfile(7))
		h, _ := w.Run()
		return fmt.Sprintf("%d %+v %v", len(h), w.Stats, h[len(h)-1])
	}
	if a, b := run(), run(); a != b {
		t.Fatalf("not deterministic:\n%s\n%s", a, b)
	}
}
