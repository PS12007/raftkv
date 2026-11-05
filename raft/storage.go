package raft

import "sync"

// Storage is Raft's stable storage. Raft calls it from Flush, always before
// returning any message that depends on the persisted state, which gives the
// paper's "persist before responding to RPCs" rule.
type Storage interface {
	// Load returns the persisted state. snap is nil if no snapshot (or
	// bootstrap configuration) was ever saved.
	Load() (hs HardState, snap *Snapshot, ents []Entry, err error)
	// Save persists hs and replaces every log entry with index >= from by
	// ents. from == 0 means the log is unchanged.
	Save(hs HardState, from uint64, ents []Entry) error
	// SaveSnapshot atomically replaces the snapshot, hard state and entire
	// log suffix (the entries that follow snap.Index).
	SaveSnapshot(hs HardState, snap Snapshot, ents []Entry) error
}

// MemStorage is an in-memory Storage. It survives a simulated crash: the
// simulator throws away the Raft object but keeps the MemStorage, exactly
// like a disk.
type MemStorage struct {
	mu   sync.Mutex
	hs   HardState
	snap *Snapshot
	ents []Entry // entries after snap.Index, contiguous

	Bytes int // approximate persisted size, for tests/metrics
}

// NewMemStorage returns an empty MemStorage.
func NewMemStorage() *MemStorage { return &MemStorage{} }

func (s *MemStorage) Load() (HardState, *Snapshot, []Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var snap *Snapshot
	if s.snap != nil {
		c := *s.snap
		c.Members = append([]Member(nil), s.snap.Members...)
		snap = &c
	}
	return s.hs, snap, append([]Entry(nil), s.ents...), nil
}

func (s *MemStorage) Save(hs HardState, from uint64, ents []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hs = hs
	if from == 0 {
		return nil
	}
	base := uint64(0)
	if s.snap != nil {
		base = s.snap.Index
	}
	if from <= base {
		panic("raft: MemStorage.Save into snapshot")
	}
	cut := from - base - 1
	if cut > uint64(len(s.ents)) {
		panic("raft: MemStorage.Save leaves a gap")
	}
	s.ents = append(s.ents[:cut:cut], ents...)
	s.recount()
	return nil
}

func (s *MemStorage) SaveSnapshot(hs HardState, snap Snapshot, ents []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hs = hs
	c := snap
	c.Members = append([]Member(nil), snap.Members...)
	s.snap = &c
	s.ents = append([]Entry(nil), ents...)
	s.recount()
	return nil
}

func (s *MemStorage) recount() {
	n := 16
	for i := range s.ents {
		n += entrySize(&s.ents[i])
	}
	s.Bytes = n
}

// SnapshotSize returns the size of the persisted snapshot data.
func (s *MemStorage) SnapshotSize() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snap == nil {
		return 0
	}
	return len(s.snap.Data)
}
