package cluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ps12007/raftkv/client"
	"github.com/ps12007/raftkv/linearize"
	"github.com/ps12007/raftkv/raft"
)

func TestBasicOps(t *testing.T) {
	c, err := Start(3, Options{TickInterval: 5 * time.Millisecond, NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ck := client.New(c.Addrs())
	defer ck.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := ck.Put(ctx, "a", "1"); err != nil {
		t.Fatal(err)
	}
	if err := ck.Append(ctx, "a", "2"); err != nil {
		t.Fatal(err)
	}
	v, ok, err := ck.Get(ctx, "a")
	if err != nil || !ok || v != "12" {
		t.Fatalf("get: %q %v %v", v, ok, err)
	}
	if found, err := ck.Delete(ctx, "a"); err != nil || !found {
		t.Fatalf("delete: %v %v", found, err)
	}
	if _, ok, _ := ck.Get(ctx, "a"); ok {
		t.Fatal("key still present")
	}
}

// TestLinearizableUnderFaults runs concurrent clients over real gRPC while
// crashing, restarting and partitioning nodes, then checks the wall-clock
// history for linearizability.
func TestLinearizableUnderFaults(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	c, err := Start(5, Options{TickInterval: 5 * time.Millisecond, NoSync: true, MaxLogBytes: 16 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.WaitLeader(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	c.SetNetwork(0.02, 0, 2*time.Millisecond)

	var mu sync.Mutex
	var history []linearize.Operation
	start := time.Now()
	now := func() int64 { return int64(time.Since(start)) }
	var stop atomic.Bool
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ck := client.New(c.Addrs())
			ck.AttemptTimeout = 300 * time.Millisecond
			defer ck.Close()
			for j := 0; !stop.Load(); j++ {
				key := fmt.Sprint("k", (i+j)%3)
				val := fmt.Sprintf("<%d.%d>", i, j)
				op := linearize.Operation{ClientID: i, Call: now()}
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				var err error
				switch j % 3 {
				case 0:
					var v string
					v, _, err = ck.Get(ctx, key)
					op.Input, op.Output = linearize.KvInput{Op: 0, Key: key}, linearize.KvOutput{Value: v}
				case 1:
					err = ck.Append(ctx, key, val)
					op.Input, op.Output = linearize.KvInput{Op: 2, Key: key, Value: val}, linearize.KvOutput{}
				case 2:
					err = ck.Put(ctx, key, val)
					op.Input, op.Output = linearize.KvInput{Op: 1, Key: key, Value: val}, linearize.KvOutput{}
				}
				cancel()
				op.Return = now()
				if err != nil {
					if op.Input.(linearize.KvInput).Op == 0 {
						continue // a failed read has no effect
					}
					op.Return = linearize.Pending // outcome unknown
				}
				mu.Lock()
				history = append(history, op)
				mu.Unlock()
				if err != nil {
					return // a Clerk must not issue a new op after an unknown outcome
				}
			}
		}(i)
	}

	ids := c.IDs()
	for round := 0; round < 6; round++ {
		time.Sleep(300 * time.Millisecond)
		switch round % 3 {
		case 0:
			if l := c.Leader(); l != 0 {
				c.Crash(l)
				time.Sleep(300 * time.Millisecond)
				c.Restart(l)
			}
		case 1:
			c.Partition([]raft.NodeID{ids[0], ids[1]}, []raft.NodeID{ids[2], ids[3], ids[4]})
		case 2:
			c.Heal()
		}
	}
	c.Heal()
	c.SetNetwork(0, 0, 0)
	time.Sleep(500 * time.Millisecond)
	stop.Store(true)
	wg.Wait()

	if len(history) < 50 {
		t.Fatalf("only %d operations completed", len(history))
	}
	res, info := linearize.Check(linearize.KvModel, history, time.Minute)
	if res != linearize.Ok {
		desc := ""
		if info != nil {
			desc = linearize.Describe(linearize.KvModel, info.FailedPartition, info.Longest)
		}
		t.Fatalf("history of %d ops is %v\n%s", len(history), res, desc)
	}
	t.Logf("%d operations, linearizable", len(history))
}

func TestMembershipChange(t *testing.T) {
	c, err := Start(3, Options{TickInterval: 5 * time.Millisecond, NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ck := client.New(c.Addrs())
	defer ck.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := ck.Put(ctx, "x", "1"); err != nil {
		t.Fatal(err)
	}
	id, addr, err := c.NewNode()
	if err != nil {
		t.Fatal(err)
	}
	if err := ck.AddServer(ctx, id, addr); err != nil {
		t.Fatal(err)
	}
	// Remove the original node 1, then make sure the cluster still works and
	// the new node has the data.
	if err := ck.RemoveServer(ctx, 1); err != nil {
		t.Fatal(err)
	}
	c.Forget(1)
	if err := ck.Put(ctx, "x", "2"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		st := c.Node(id).Info()
		if st.Keys == 1 && len(st.Members) == 3 && st.AppliedSM >= st.Commit && st.Commit > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("new node did not catch up: %+v", st)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
