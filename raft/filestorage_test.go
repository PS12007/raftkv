package raft

import (
	"os"
	"path/filepath"
	"testing"
)

func ents(from, to, term uint64) []Entry {
	var es []Entry
	for i := from; i <= to; i++ {
		es = append(es, Entry{Index: i, Term: term, Data: []byte{byte(i)}})
	}
	return es
}

func TestFileStorageRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, _ := OpenFileStorage(dir)
	if _, snap, es, err := s.Load(); err != nil || snap != nil || len(es) != 0 {
		t.Fatalf("fresh load: %v %v %v", snap, es, err)
	}
	s.SaveSnapshot(HardState{}, Snapshot{Members: []Member{{ID: 1, Addr: "a"}}}, nil) // bootstrap
	s.Save(HardState{Term: 1, VotedFor: 1}, 1, ents(1, 5, 1))
	s.Save(HardState{Term: 2, VotedFor: 2}, 4, ents(4, 6, 2)) // overwrite suffix
	s.Close()

	s2, _ := OpenFileStorage(dir)
	hs, snap, es, err := s2.Load()
	if err != nil {
		t.Fatal(err)
	}
	if hs.Term != 2 || hs.VotedFor != 2 || snap == nil || len(snap.Members) != 1 {
		t.Fatalf("bad state %+v %+v", hs, snap)
	}
	if len(es) != 6 || es[2].Term != 1 || es[3].Term != 2 || es[5].Index != 6 {
		t.Fatalf("bad entries %+v", es)
	}

	// Snapshot compacts the log.
	s2.SaveSnapshot(HardState{Term: 2, VotedFor: 2}, Snapshot{Index: 4, Term: 2, Data: []byte("snap")}, es[4:])
	s2.Save(HardState{Term: 3}, 7, ents(7, 7, 3))
	s2.Close()
	s3, _ := OpenFileStorage(dir)
	hs, snap, es, err = s3.Load()
	if err != nil || hs.Term != 3 || snap.Index != 4 || string(snap.Data) != "snap" || len(es) != 3 || es[0].Index != 5 {
		t.Fatalf("after snapshot: %+v %+v %+v %v", hs, snap, es, err)
	}
}

func TestFileStorageTornTail(t *testing.T) {
	dir := t.TempDir()
	s, _ := OpenFileStorage(dir)
	s.Load()
	s.Save(HardState{Term: 1}, 1, ents(1, 3, 1))
	s.Close()
	// Simulate a crash in the middle of writing a record.
	f, _ := os.OpenFile(filepath.Join(dir, "wal"), os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{50, 0, 0, 0, 1, 2, 3, 4, 9, 9})
	f.Close()
	s2, _ := OpenFileStorage(dir)
	hs, _, es, err := s2.Load()
	if err != nil || hs.Term != 1 || len(es) != 3 {
		t.Fatalf("torn tail: %+v %+v %v", hs, es, err)
	}
	s2.Save(HardState{Term: 2}, 4, ents(4, 4, 2))
	s2.Close()
	s3, _ := OpenFileStorage(dir)
	hs, _, es, _ = s3.Load()
	if hs.Term != 2 || len(es) != 4 {
		t.Fatalf("append after torn tail: %+v %d", hs, len(es))
	}
}

// TestRaftOnFileStorage restarts a node from disk.
func TestRaftOnFileStorage(t *testing.T) {
	dir := t.TempDir()
	open := func() (*Raft, *FileStorage) {
		fs, _ := OpenFileStorage(dir)
		r, err := New(Config{ID: 1, Peers: []Member{{ID: 1}}, Storage: fs})
		if err != nil {
			t.Fatal(err)
		}
		return r, fs
	}
	r, fs := open()
	for i := 0; i < 30 && !r.IsLeader(); i++ {
		r.Tick()
	}
	r.Flush()
	r.Propose([]byte("a"))
	r.Propose([]byte("b"))
	_, ap := r.Flush()
	if len(ap) != 2 { // a + b (the no-op was applied by the first Flush)
		t.Fatalf("applied %d", len(ap))
	}
	r.Snapshot(2, []byte("S"))
	r.Propose([]byte("c"))
	r.Flush()
	term := r.Term()
	fs.Close()

	r, _ = open()
	_, ap = r.Flush()
	if r.Term() != term || len(ap) != 1 || ap[0].Snapshot == nil || string(ap[0].Snapshot.Data) != "S" {
		t.Fatalf("restart: term %d, applies %+v", r.Term(), ap)
	}
	if r.log.lastIndex() != 4 {
		t.Fatalf("lastIndex %d", r.log.lastIndex())
	}
}
