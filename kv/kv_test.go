package kv

import "testing"

func TestDedupAndSnapshot(t *testing.T) {
	s := NewStore()
	s.ApplyOp(Op{Type: OpAppend, Key: "k", Value: "a", ClientID: 7, Seq: 1})
	s.ApplyOp(Op{Type: OpAppend, Key: "k", Value: "a", ClientID: 7, Seq: 1}) // retry
	if v, _ := s.Get("k"); v != "a" {
		t.Fatalf("duplicate applied: %q", v)
	}
	s.ApplyOp(Op{Type: OpPut, Key: "j", Value: "x", ClientID: 8, Seq: 1})
	s2 := NewStore()
	s2.Restore(s.Snapshot())
	if string(s2.Snapshot()) != string(s.Snapshot()) {
		t.Fatal("snapshot round trip differs")
	}
	s2.ApplyOp(Op{Type: OpAppend, Key: "k", Value: "a", ClientID: 7, Seq: 1})
	if v, _ := s2.Get("k"); v != "a" {
		t.Fatal("dedup table lost in snapshot")
	}
	op := Op{Type: OpDelete, Key: "a\x00b", Value: "v", ClientID: 1 << 60, Seq: 99}
	got, err := DecodeOp(EncodeOp(op))
	if err != nil || got != op {
		t.Fatalf("op round trip: %+v %v", got, err)
	}
}
