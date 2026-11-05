// Command raftkv-bench measures throughput and latency of a real raftkv
// cluster (gRPC over localhost, file storage) for several cluster sizes and
// fault scenarios, and prints a Markdown report.
//
//	raftkv-bench -sizes 1,3,5,7 -faults none,leader-crash,partition,lossy,slow -duration 6s
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ps12007/raftkv/client"
	"github.com/ps12007/raftkv/cluster"
	"github.com/ps12007/raftkv/raft"
)

type result struct {
	Size       int     `json:"size"`
	Fault      string  `json:"fault"`
	Clients    int     `json:"clients"`
	Ops        int     `json:"ops"`
	Throughput float64 `json:"throughput"` // ops/s
	P50        float64 `json:"p50_ms"`
	P90        float64 `json:"p90_ms"`
	P99        float64 `json:"p99_ms"`
	Max        float64 `json:"max_ms"`
	MaxGap     float64 `json:"max_gap_ms"` // longest window with no completed op
	Retries    uint64  `json:"retries"`
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func run(size int, fault string, clients int, dur time.Duration, valueSize int, writeRatio float64, nosync bool) (result, error) {
	c, err := cluster.Start(size, cluster.Options{TickInterval: 10 * time.Millisecond, NoSync: nosync, MaxLogBytes: 4 << 20})
	if err != nil {
		return result{}, err
	}
	defer c.Close()
	if _, err := c.WaitLeader(10 * time.Second); err != nil {
		return result{}, err
	}
	value := strings.Repeat("x", valueSize)

	// Warm up connections.
	ck := client.New(c.Addrs())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := ck.Put(ctx, "warm", "up"); err != nil {
		cancel()
		return result{}, err
	}
	cancel()
	ck.Close()

	var mu sync.Mutex
	var lats []time.Duration
	var completions []time.Duration // completion times since start
	var retries atomic.Uint64
	start := time.Now()
	end := start.Add(dur)
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ck := client.New(c.Addrs())
			ck.AttemptTimeout = 500 * time.Millisecond
			defer ck.Close()
			var local []time.Duration
			var done []time.Duration
			for j := 0; time.Now().Before(end); j++ {
				key := fmt.Sprintf("key-%d-%d", i, j%100)
				t0 := time.Now()
				ctx, cancel := context.WithDeadline(context.Background(), end.Add(5*time.Second))
				var err error
				if float64(j%100)/100 < writeRatio {
					err = ck.Put(ctx, key, value)
				} else {
					_, _, err = ck.Get(ctx, key)
				}
				cancel()
				if err != nil {
					continue
				}
				t1 := time.Now()
				local = append(local, t1.Sub(t0))
				done = append(done, t1.Sub(start))
			}
			retries.Add(ck.Retries)
			mu.Lock()
			lats = append(lats, local...)
			completions = append(completions, done...)
			mu.Unlock()
		}(i)
	}

	// Fault schedule: the middle third of the run.
	third := dur / 3
	time.Sleep(third)
	var restore func()
	switch fault {
	case "none":
	case "leader-crash":
		l := c.Leader()
		c.Crash(l)
		restore = func() { c.Restart(l) }
	case "follower-crash":
		l := c.Leader()
		for _, id := range c.IDs() {
			if id != l {
				c.Crash(id)
				restore = func() { c.Restart(id) }
				break
			}
		}
	case "partition":
		// Isolate the leader from everyone else.
		l := c.Leader()
		var rest []raft.NodeID
		for _, id := range c.IDs() {
			if id != l {
				rest = append(rest, id)
			}
		}
		c.Partition([]raft.NodeID{l}, rest)
		restore = c.Heal
	case "lossy":
		c.SetNetwork(0.05, 0, 0)
		restore = func() { c.SetNetwork(0, 0, 0) }
	case "slow":
		c.SetNetwork(0, 2*time.Millisecond, 3*time.Millisecond)
		restore = func() { c.SetNetwork(0, 0, 0) }
	default:
		return result{}, fmt.Errorf("unknown fault %q", fault)
	}
	time.Sleep(third)
	if restore != nil {
		restore()
	}
	wg.Wait()
	elapsed := time.Since(start)

	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	sort.Slice(completions, func(i, j int) bool { return completions[i] < completions[j] })
	pct := func(p float64) float64 {
		if len(lats) == 0 {
			return 0
		}
		return ms(lats[min(len(lats)-1, int(p*float64(len(lats))))])
	}
	var gap, prev time.Duration
	for _, t := range completions {
		if t > dur {
			break
		}
		gap = max(gap, t-prev)
		prev = t
	}
	gap = max(gap, min(dur, elapsed)-prev)
	r := result{Size: size, Fault: fault, Clients: clients, Ops: len(lats),
		Throughput: float64(len(lats)) / elapsed.Seconds(),
		P50:        pct(0.5), P90: pct(0.9), P99: pct(0.99), MaxGap: ms(gap), Retries: retries.Load()}
	if len(lats) > 0 {
		r.Max = ms(lats[len(lats)-1])
	}
	return r, nil
}

func main() {
	sizes := flag.String("sizes", "1,3,5,7", "cluster sizes")
	faults := flag.String("faults", "none", "comma-separated: none,leader-crash,follower-crash,partition,lossy,slow")
	clients := flag.Int("clients", 32, "concurrent clients")
	dur := flag.Duration("duration", 6*time.Second, "duration per run")
	valueSize := flag.Int("value", 128, "value size in bytes")
	writes := flag.Float64("writes", 0.5, "fraction of writes (reads also go through the log)")
	nosync := flag.Bool("nosync", false, "disable fsync")
	jsonOut := flag.String("json", "", "also write results as JSON to this file")
	flag.Parse()

	var results []result
	for _, f := range strings.Split(*faults, ",") {
		for _, s := range strings.Split(*sizes, ",") {
			n, err := strconv.Atoi(s)
			if err != nil {
				log.Fatal(err)
			}
			if n == 1 && f != "none" && f != "lossy" && f != "slow" {
				continue // single node: crash/partition means unavailability by definition
			}
			r, err := run(n, f, *clients, *dur, *valueSize, *writes, *nosync)
			if err != nil {
				log.Fatalf("size %d fault %s: %v", n, f, err)
			}
			log.Printf("size=%d fault=%s ops/s=%.0f p50=%.2fms p99=%.2fms gap=%.0fms", n, f, r.Throughput, r.P50, r.P99, r.MaxGap)
			results = append(results, r)
		}
	}
	fmt.Printf("| nodes | fault | ops/s | p50 ms | p90 ms | p99 ms | max ms | longest stall ms | client retries |\n")
	fmt.Printf("|---:|---|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, r := range results {
		fmt.Printf("| %d | %s | %.0f | %.2f | %.2f | %.2f | %.1f | %.0f | %d |\n",
			r.Size, r.Fault, r.Throughput, r.P50, r.P90, r.P99, r.Max, r.MaxGap, r.Retries)
	}
	if *jsonOut != "" {
		b, _ := json.MarshalIndent(results, "", "  ")
		os.WriteFile(*jsonOut, b, 0o644)
	}
}
