package raft

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// FileStorage is a durable Storage: an append-only write-ahead log of hard
// state and entry records, plus a snapshot file. Every Save is fsynced. The
// WAL is rewritten (compacted) whenever a snapshot is saved.
//
// WAL record: [u32 payload length][u32 CRC32C of payload][payload].
// A torn or corrupt trailing record (crash mid-write) is ignored on load:
// it was never fsynced, so nothing was acknowledged based on it.
type FileStorage struct {
	mu   sync.Mutex
	dir  string
	wal  *os.File
	bw   *bufio.Writer
	base uint64 // snapshot index
	// NoSync disables fsync (for benchmarks that want to measure the
	// protocol rather than the disk).
	NoSync bool
}

const (
	recHardState byte = 1
	recEntries   byte = 2
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// OpenFileStorage opens (creating if needed) storage in dir.
func OpenFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &FileStorage{dir: dir}
	return s, nil
}

func (s *FileStorage) walPath() string  { return filepath.Join(s.dir, "wal") }
func (s *FileStorage) snapPath() string { return filepath.Join(s.dir, "snapshot") }

// Load implements Storage. It must be called once, before Save.
func (s *FileStorage) Load() (HardState, *Snapshot, []Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var hs HardState
	var snap *Snapshot
	if b, err := os.ReadFile(s.snapPath()); err == nil {
		sn, err := decodeSnapshotFile(b)
		if err != nil {
			return hs, nil, nil, fmt.Errorf("snapshot: %w", err)
		}
		snap = &sn
		s.base = sn.Index
	} else if !errors.Is(err, os.ErrNotExist) {
		return hs, nil, nil, err
	}

	f, err := os.OpenFile(s.walPath(), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return hs, nil, nil, err
	}
	var ents []Entry
	r := bufio.NewReader(f)
	var good int64
	for {
		payload, n, err := readRecord(r)
		if err != nil {
			break // EOF or torn tail
		}
		switch payload[0] {
		case recHardState:
			rd := reader{b: payload[1:]}
			hs.Term = rd.uvarint()
			hs.VotedFor = rd.uvarint()
		case recEntries:
			rd := reader{b: payload[1:]}
			from := rd.uvarint()
			cnt := rd.uvarint()
			es := make([]Entry, 0, cnt)
			for i := uint64(0); i < cnt; i++ {
				es = append(es, rd.entry())
			}
			if rd.err != nil {
				break
			}
			if len(ents) == 0 || from <= ents[0].Index {
				ents = es
			} else {
				cut := from - ents[0].Index
				if cut > uint64(len(ents)) {
					return hs, nil, nil, errors.New("raft: WAL has a gap")
				}
				ents = append(ents[:cut], es...)
			}
		}
		good += int64(n)
	}
	// Drop a torn tail so new records are appended after valid data.
	if err := f.Truncate(good); err != nil {
		return hs, nil, nil, err
	}
	if _, err := f.Seek(good, io.SeekStart); err != nil {
		return hs, nil, nil, err
	}
	s.wal = f
	s.bw = bufio.NewWriterSize(f, 1<<20)

	// Discard entries covered by the snapshot (a crash can leave a new
	// snapshot next to an old WAL).
	if snap != nil && len(ents) > 0 {
		if ents[len(ents)-1].Index <= snap.Index {
			ents = nil
		} else if ents[0].Index <= snap.Index {
			k := snap.Index - ents[0].Index
			if ents[k].Term != snap.Term {
				ents = nil
			} else {
				ents = ents[k+1:]
			}
		}
	}
	if len(ents) > 0 && ents[0].Index != s.base+1 {
		return hs, nil, nil, fmt.Errorf("raft: WAL starts at %d, snapshot ends at %d", ents[0].Index, s.base)
	}
	return hs, snap, ents, nil
}

func readRecord(r *bufio.Reader) ([]byte, int, error) {
	var hdr [8]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, 0, err
	}
	n := binary.LittleEndian.Uint32(hdr[:4])
	sum := binary.LittleEndian.Uint32(hdr[4:])
	if n == 0 || n > 1<<30 {
		return nil, 0, errors.New("bad record length")
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, 0, err
	}
	if crc32.Checksum(payload, crcTable) != sum {
		return nil, 0, errors.New("bad checksum")
	}
	return payload, 8 + int(n), nil
}

func writeRecord(w io.Writer, payload []byte) error {
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:], crc32.Checksum(payload, crcTable))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func hsRecord(hs HardState) []byte {
	b := []byte{recHardState}
	b = binary.AppendUvarint(b, hs.Term)
	return binary.AppendUvarint(b, hs.VotedFor)
}

func entriesRecord(from uint64, ents []Entry) []byte {
	b := []byte{recEntries}
	b = binary.AppendUvarint(b, from)
	b = binary.AppendUvarint(b, uint64(len(ents)))
	for i := range ents {
		b = appendEntry(b, &ents[i])
	}
	return b
}

func appendEntry(b []byte, e *Entry) []byte {
	b = binary.AppendUvarint(b, e.Term)
	b = binary.AppendUvarint(b, e.Index)
	b = append(b, byte(e.Type))
	b = binary.AppendUvarint(b, uint64(len(e.Data)))
	return append(b, e.Data...)
}

type reader struct {
	b   []byte
	err error
}

func (r *reader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.err = errors.New("corrupt varint")
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) bytes() []byte {
	n := r.uvarint()
	if r.err != nil || uint64(len(r.b)) < n {
		r.err = errors.New("corrupt bytes")
		return nil
	}
	out := append([]byte(nil), r.b[:n]...)
	r.b = r.b[n:]
	return out
}

func (r *reader) entry() Entry {
	var e Entry
	e.Term = r.uvarint()
	e.Index = r.uvarint()
	if r.err == nil && len(r.b) > 0 {
		e.Type = EntryType(r.b[0])
		r.b = r.b[1:]
	} else {
		r.err = errors.New("corrupt entry")
	}
	e.Data = r.bytes()
	return e
}

func (s *FileStorage) sync() error {
	if err := s.bw.Flush(); err != nil {
		return err
	}
	if s.NoSync {
		return nil
	}
	return s.wal.Sync()
}

// Save implements Storage.
func (s *FileStorage) Save(hs HardState, from uint64, ents []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := writeRecord(s.bw, hsRecord(hs)); err != nil {
		return err
	}
	if from != 0 {
		if err := writeRecord(s.bw, entriesRecord(from, ents)); err != nil {
			return err
		}
	}
	return s.sync()
}

// SaveSnapshot implements Storage.
func (s *FileStorage) SaveSnapshot(hs HardState, snap Snapshot, ents []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := writeFileAtomic(s.snapPath(), encodeSnapshotFile(snap), !s.NoSync); err != nil {
		return err
	}
	// Rewrite the WAL with just the hard state and the remaining entries.
	var buf []byte
	w := byteWriter{&buf}
	writeRecord(w, hsRecord(hs))
	if len(ents) > 0 {
		writeRecord(w, entriesRecord(ents[0].Index, ents))
	}
	if err := writeFileAtomic(s.walPath(), buf, !s.NoSync); err != nil {
		return err
	}
	s.wal.Close()
	f, err := os.OpenFile(s.walPath(), os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	s.wal = f
	s.bw.Reset(f)
	s.base = snap.Index
	return nil
}

// Close closes the WAL.
func (s *FileStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wal == nil {
		return nil
	}
	s.bw.Flush()
	return s.wal.Close()
}

type byteWriter struct{ b *[]byte }

func (w byteWriter) Write(p []byte) (int, error) { *w.b = append(*w.b, p...); return len(p), nil }

func writeFileAtomic(path string, data []byte, doSync bool) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if doSync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if doSync {
		if d, err := os.Open(filepath.Dir(path)); err == nil {
			d.Sync()
			d.Close()
		}
	}
	return nil
}

func encodeSnapshotFile(s Snapshot) []byte {
	b := binary.AppendUvarint(nil, s.Index)
	b = binary.AppendUvarint(b, s.Term)
	m := EncodeMembers(s.Members)
	b = binary.AppendUvarint(b, uint64(len(m)))
	b = append(b, m...)
	b = binary.AppendUvarint(b, uint64(len(s.Data)))
	b = append(b, s.Data...)
	sum := crc32.Checksum(b, crcTable)
	return binary.LittleEndian.AppendUint32(b, sum)
}

func decodeSnapshotFile(b []byte) (Snapshot, error) {
	if len(b) < 4 {
		return Snapshot{}, errors.New("short snapshot")
	}
	body, sum := b[:len(b)-4], binary.LittleEndian.Uint32(b[len(b)-4:])
	if crc32.Checksum(body, crcTable) != sum {
		return Snapshot{}, errors.New("snapshot checksum mismatch")
	}
	r := reader{b: body}
	var s Snapshot
	s.Index = r.uvarint()
	s.Term = r.uvarint()
	s.Members = DecodeMembers(r.bytes())
	s.Data = r.bytes()
	return s, r.err
}
