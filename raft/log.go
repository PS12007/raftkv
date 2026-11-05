package raft

// raftLog is the in-memory log: a snapshot covering indexes [1, snap.Index]
// followed by entries snap.Index+1 .. lastIndex().
type raftLog struct {
	snap    Snapshot
	entries []Entry
	bytes   int // approximate in-memory size of entries
}

const entryOverhead = 24

func entrySize(e *Entry) int { return len(e.Data) + entryOverhead }

func (l *raftLog) firstIndex() uint64 { return l.snap.Index + 1 }

func (l *raftLog) lastIndex() uint64 { return l.snap.Index + uint64(len(l.entries)) }

func (l *raftLog) lastTerm() uint64 {
	t, _ := l.term(l.lastIndex())
	return t
}

// term returns the term of entry i, if it is known (in the log or exactly
// the snapshot's last included entry).
func (l *raftLog) term(i uint64) (uint64, bool) {
	if i == l.snap.Index {
		return l.snap.Term, true
	}
	if i < l.snap.Index || i > l.lastIndex() {
		return 0, false
	}
	return l.entries[i-l.snap.Index-1].Term, true
}

func (l *raftLog) entry(i uint64) *Entry {
	return &l.entries[i-l.snap.Index-1]
}

// slice returns a copy of entries [lo, hi).
func (l *raftLog) slice(lo, hi uint64) []Entry {
	if lo >= hi {
		return nil
	}
	off := l.snap.Index + 1
	out := make([]Entry, hi-lo)
	copy(out, l.entries[lo-off:hi-off])
	return out
}

func (l *raftLog) append(ents ...Entry) {
	for i := range ents {
		l.bytes += entrySize(&ents[i])
	}
	l.entries = append(l.entries, ents...)
}

// truncateFrom discards entries with index >= i.
func (l *raftLog) truncateFrom(i uint64) {
	if i > l.lastIndex() {
		return
	}
	cut := i - l.snap.Index - 1
	for k := cut; k < uint64(len(l.entries)); k++ {
		l.bytes -= entrySize(&l.entries[k])
		l.entries[k] = Entry{} // release Data for GC
	}
	l.entries = l.entries[:cut]
}

// compact replaces the prefix up to and including snap.Index with snap.
// Entries after snap.Index are kept if they are consistent with snap.
func (l *raftLog) compact(snap Snapshot) {
	var keep []Entry
	if t, ok := l.term(snap.Index); ok && t == snap.Term && snap.Index >= l.snap.Index {
		start := snap.Index - l.snap.Index
		keep = make([]Entry, uint64(len(l.entries))-start)
		copy(keep, l.entries[start:])
	}
	l.snap = snap
	l.entries = keep
	l.bytes = 0
	for i := range l.entries {
		l.bytes += entrySize(&l.entries[i])
	}
}

// lastIndexOfTerm returns the last index whose term is t, or 0.
func (l *raftLog) lastIndexOfTerm(t uint64) uint64 {
	for i := l.lastIndex(); i > l.snap.Index; i-- {
		et := l.entry(i).Term
		if et == t {
			return i
		}
		if et < t {
			break
		}
	}
	return 0
}

// firstIndexOfTermAt returns the first index of the run of entries with the
// same term as entry i (bounded below by the snapshot).
func (l *raftLog) firstIndexOfTermAt(i uint64) uint64 {
	t, _ := l.term(i)
	for i-1 > l.snap.Index {
		if pt, _ := l.term(i - 1); pt != t {
			break
		}
		i--
	}
	return i
}
