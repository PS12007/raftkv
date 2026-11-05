package sim

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ps12007/raftkv/linearize"
)

var (
	flagSeeds = flag.Int("seeds", 50, "number of seeds for randomized tests")
	flagStart = flag.Int64("seed-start", 1, "first seed")
	flagSeed  = flag.Int64("seed", 0, "run a single seed with verbose logging")
	flagOut   = flag.String("failures", "", "directory to write failing histories to")
)

// profile derives a per-seed fault configuration so that the seed space
// covers many cluster shapes and fault mixes.
func profile(seed int64) Config {
	c := DefaultConfig(seed)
	x := uint64(seed) * 0x9E3779B97F4A7C15
	pick := func(n uint64) uint64 { v := x % n; x = x/n ^ (x>>7)*31; return v }
	c.Nodes = []int{3, 5, 5, 7}[pick(4)]
	c.Clients = int(3 + pick(5))
	c.Keys = int(1 + pick(5))
	switch pick(6) {
	case 0:
		c.Unreliable = false
	case 1:
		c.Crashes = false
	case 2:
		c.Partitions = false
	}
	c.Membership = pick(3) == 0
	c.MaxLogBytes = []int{0, 1024, 4096, 64 << 10}[pick(4)]
	c.FaultEvery = []Time{100 * Millisecond, 300 * Millisecond, 800 * Millisecond}[pick(3)]
	c.MaxEntriesPerMsg = []int{1, 3, 64}[pick(3)]
	c.PreVote = pick(2) == 0
	if pick(4) == 0 {
		// High churn: frequent crashes and partitions with fast elections,
		// producing many terms and conflicting uncommitted log suffixes.
		c.FaultEvery = 40 * Millisecond
		c.ElectionTicks = 5
		c.HeartbeatTicks = 2
		c.Crashes, c.Partitions = true, true
	}
	if pick(5) == 0 {
		c.LeaderChurn = true
		c.Nodes = 5
		c.Membership = false
		c.FaultEvery = Time(5+pick(30)) * Millisecond
		c.MaxEntriesPerMsg = 1
	}
	return c
}

func runSeed(t testing.TB, cfg Config) (res Result) {
	defer func() {
		if p := recover(); p != nil {
			res = Result{Seed: cfg.Seed, Err: fmt.Errorf("seed %d: panic: %v", cfg.Seed, p)}
		}
	}()
	res = New(cfg).Run()
	if res.Err != nil {
		return res
	}
	verdict, desc := CheckLinearizable(res.History)
	if verdict != linearize.Ok {
		res.Err = fmt.Errorf("seed %d: history is not linearizable (%v)\n%s", cfg.Seed, verdict, desc)
	}
	return res
}

func saveFailure(res Result) {
	if *flagOut == "" {
		return
	}
	os.MkdirAll(*flagOut, 0o755)
	os.WriteFile(filepath.Join(*flagOut, fmt.Sprintf("seed-%d.txt", res.Seed)), []byte(res.Err.Error()), 0o644)
}

// TestRandomized runs many seeds with random faults and checks safety,
// liveness after healing, replica convergence and linearizability.
//
//	go test ./sim -run TestRandomized -seeds=5000
func TestRandomized(t *testing.T) {
	if *flagSeed != 0 {
		cfg := profile(*flagSeed)
		cfg.Logf = t.Logf
		res := runSeed(t, cfg)
		t.Logf("config %+v", cfg)
		t.Logf("stats %+v", res.Stats)
		if res.Err != nil {
			t.Fatal(res.Err)
		}
		return
	}
	n := *flagSeeds
	if testing.Short() && n > 20 {
		n = 20
	}
	var next atomic.Int64
	next.Store(*flagStart)
	end := *flagStart + int64(n)
	var mu sync.Mutex
	var failures []Result
	var total Stats
	var wg sync.WaitGroup
	for w := 0; w < runtime.GOMAXPROCS(0); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				seed := next.Add(1) - 1
				if seed >= end {
					return
				}
				res := runSeed(t, profile(seed))
				mu.Lock()
				total.OpsCompleted += res.Stats.OpsCompleted
				total.Crashes += res.Stats.Crashes
				total.Partitions += res.Stats.Partitions
				total.Elections += res.Stats.Elections
				total.Snapshots += res.Stats.Snapshots
				total.SnapshotsInstalled += res.Stats.SnapshotsInstalled
				total.AddServer += res.Stats.AddServer
				total.RemoveServer += res.Stats.RemoveServer
				total.Dropped += res.Stats.Dropped
				total.Duplicated += res.Stats.Duplicated
				if res.Err != nil {
					failures = append(failures, res)
					saveFailure(res)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	t.Logf("%d seeds: %d ops, %d elections, %d crashes, %d partitions, %d snapshots (%d sent), +%d/-%d servers, %d dropped, %d duplicated msgs",
		n, total.OpsCompleted, total.Elections, total.Crashes, total.Partitions, total.Snapshots,
		total.SnapshotsInstalled, total.AddServer, total.RemoveServer, total.Dropped, total.Duplicated)
	for i, f := range failures {
		if i >= 3 {
			t.Errorf("... and %d more failures", len(failures)-3)
			break
		}
		t.Errorf("%v", f.Err)
	}
}

// TestDeterminism runs the same seeds twice and requires identical traces.
func TestDeterminism(t *testing.T) {
	for seed := int64(1); seed <= 10; seed++ {
		a := New(profile(seed)).Run()
		b := New(profile(seed)).Run()
		if a.Trace != b.Trace || len(a.History) != len(b.History) {
			t.Fatalf("seed %d is not deterministic: trace %x vs %x", seed, a.Trace, b.Trace)
		}
	}
}

// TestNoFaults is a sanity check: a healthy cluster serves every request.
func TestNoFaults(t *testing.T) {
	cfg := DefaultConfig(42)
	cfg.Crashes, cfg.Partitions, cfg.Unreliable = false, false, false
	res := runSeed(t, cfg)
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	if res.Stats.OpsCompleted < 1000 {
		t.Fatalf("only %d ops completed", res.Stats.OpsCompleted)
	}
	if res.Stats.Elections != 1 {
		t.Fatalf("expected a single election in a healthy cluster, got %d", res.Stats.Elections)
	}
}
