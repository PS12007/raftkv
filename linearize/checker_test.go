package linearize

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func get(c int, k, v string, call, ret int64) Operation {
	return Operation{ClientID: c, Input: KvInput{Op: 0, Key: k}, Output: KvOutput{Value: v}, Call: call, Return: ret}
}
func put(c int, k, v string, call, ret int64) Operation {
	return Operation{ClientID: c, Input: KvInput{Op: 1, Key: k, Value: v}, Output: KvOutput{}, Call: call, Return: ret}
}
func appendOp(c int, k, v string, call, ret int64) Operation {
	return Operation{ClientID: c, Input: KvInput{Op: 2, Key: k, Value: v}, Output: KvOutput{}, Call: call, Return: ret}
}

func TestSimpleOk(t *testing.T) {
	h := []Operation{
		put(0, "x", "a", 0, 10),
		get(1, "x", "a", 5, 15), // concurrent with put, sees it
		get(2, "x", "", 1, 3),   // concurrent with put, before it
	}
	if r, _ := Check(KvModel, h, 0); r != Ok {
		t.Fatalf("want Ok, got %v", r)
	}
}

func TestStaleRead(t *testing.T) {
	h := []Operation{
		put(0, "x", "a", 0, 10),
		put(0, "x", "b", 11, 20),
		get(1, "x", "a", 21, 30), // stale: b completed before this get began
	}
	r, info := Check(KvModel, h, 0)
	if r != Illegal {
		t.Fatalf("want Illegal, got %v", r)
	}
	if info == nil || len(info.FailedPartition) != 3 {
		t.Fatalf("bad info %+v", info)
	}
}

func TestAppendOrder(t *testing.T) {
	h := []Operation{
		appendOp(0, "x", "1", 0, 10),
		appendOp(1, "x", "2", 0, 10),
		get(2, "x", "21", 11, 12),
		get(3, "x", "12", 13, 14), // contradicts previous read
	}
	if r, _ := Check(KvModel, h, 0); r != Illegal {
		t.Fatalf("want Illegal, got %v", r)
	}
	h = h[:3]
	if r, _ := Check(KvModel, h, 0); r != Ok {
		t.Fatalf("want Ok, got %v", r)
	}
}

func TestPendingWrite(t *testing.T) {
	// A write whose outcome is unknown may or may not have happened.
	h := []Operation{
		put(0, "x", "a", 0, Pending),
		get(1, "x", "", 5, 6),
		get(1, "x", "a", 7, 8),
	}
	if r, _ := Check(KvModel, h, 0); r != Ok {
		t.Fatalf("want Ok, got %v", r)
	}
	h = append(h, get(1, "x", "", 9, 10)) // can't un-happen
	if r, _ := Check(KvModel, h, 0); r != Illegal {
		t.Fatalf("want Illegal, got %v", r)
	}
}

// TestRandomSequential generates histories from a real sequential execution
// with random overlapping intervals; they must always be linearizable.
func TestRandomSequential(t *testing.T) {
	for seed := int64(0); seed < 200; seed++ {
		rng := rand.New(rand.NewSource(seed))
		state := map[string]string{}
		var h []Operation
		var tm int64
		for i := 0; i < 60; i++ {
			k := fmt.Sprint(rng.Intn(3))
			// linearization point at tm; interval stretched around it
			call := tm - int64(rng.Intn(20))
			ret := tm + int64(rng.Intn(20))
			switch rng.Intn(3) {
			case 0:
				h = append(h, get(i, k, state[k], call, ret))
			case 1:
				v := fmt.Sprint(i)
				state[k] = v
				h = append(h, put(i, k, v, call, ret))
			case 2:
				v := fmt.Sprint(i, ".")
				state[k] += v
				h = append(h, appendOp(i, k, v, call, ret))
			}
			tm += 5
		}
		if r, info := Check(KvModel, h, 10*time.Second); r != Ok {
			t.Fatalf("seed %d: got %v\n%s", seed, r, Describe(KvModel, info.FailedPartition, info.Longest))
		}
	}
}
