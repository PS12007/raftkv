// Package kv is the replicated key-value state machine with per-client
// request deduplication (exactly-once semantics for retried writes).
package kv

import (
	"encoding/binary"
	"errors"
	"sort"
)

// OpType is a KV operation.
type OpType uint8

const (
	OpGet OpType = iota
	OpPut
	OpAppend
	OpDelete
)

func (o OpType) String() string {
	return [...]string{"get", "put", "append", "delete"}[o]
}

// Op is a client command.
type Op struct {
	Type     OpType
	Key      string
	Value    string
	ClientID uint64
	Seq      uint64 // per-client, strictly increasing; one outstanding op per client
}

// Result is the outcome of an Op.
type Result struct {
	Value string
	Found bool
}

// ---------- encoding ----------

func putString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
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
		r.err = errors.New("kv: corrupt encoding")
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) string() string {
	n := r.uvarint()
	if r.err != nil || uint64(len(r.b)) < n {
		r.err = errors.New("kv: corrupt encoding")
		return ""
	}
	s := string(r.b[:n])
	r.b = r.b[n:]
	return s
}

// EncodeOp serialises an Op.
func EncodeOp(op Op) []byte {
	b := make([]byte, 0, 24+len(op.Key)+len(op.Value))
	b = append(b, byte(op.Type))
	b = binary.AppendUvarint(b, op.ClientID)
	b = binary.AppendUvarint(b, op.Seq)
	b = putString(b, op.Key)
	return putString(b, op.Value)
}

// DecodeOp is the inverse of EncodeOp.
func DecodeOp(b []byte) (Op, error) {
	if len(b) == 0 {
		return Op{}, errors.New("kv: empty op")
	}
	r := reader{b: b[1:]}
	op := Op{Type: OpType(b[0])}
	op.ClientID = r.uvarint()
	op.Seq = r.uvarint()
	op.Key = r.string()
	op.Value = r.string()
	return op, r.err
}

// EncodeResult serialises a Result.
func EncodeResult(res Result) []byte {
	b := []byte{0}
	if res.Found {
		b[0] = 1
	}
	return putString(b, res.Value)
}

// DecodeResult is the inverse of EncodeResult.
func DecodeResult(b []byte) (Result, error) {
	if len(b) == 0 {
		return Result{}, errors.New("kv: empty result")
	}
	r := reader{b: b[1:]}
	res := Result{Found: b[0] == 1, Value: r.string()}
	return res, r.err
}

// ---------- state machine ----------

type session struct {
	seq    uint64
	result Result
}

// Store is the KV state machine. It implements rsm.StateMachine.
type Store struct {
	data     map[string]string
	sessions map[uint64]session
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{data: map[string]string{}, sessions: map[uint64]session{}}
}

// Apply implements rsm.StateMachine.
func (s *Store) Apply(cmd []byte) []byte {
	op, err := DecodeOp(cmd)
	if err != nil {
		panic(err)
	}
	return EncodeResult(s.ApplyOp(op))
}

// ApplyOp executes op unless it is a duplicate, in which case the cached
// result of the original execution is returned.
func (s *Store) ApplyOp(op Op) Result {
	if op.ClientID != 0 {
		if sess, ok := s.sessions[op.ClientID]; ok && op.Seq <= sess.seq {
			// Duplicate (a retry). Clients have at most one op outstanding,
			// so a retry is always of the latest op.
			return sess.result
		}
	}
	var res Result
	switch op.Type {
	case OpGet:
		res.Value, res.Found = s.data[op.Key]
	case OpPut:
		s.data[op.Key] = op.Value
	case OpAppend:
		s.data[op.Key] += op.Value
	case OpDelete:
		_, res.Found = s.data[op.Key]
		delete(s.data, op.Key)
	}
	if op.ClientID != 0 {
		s.sessions[op.ClientID] = session{seq: op.Seq, result: res}
	}
	return res
}

// Get reads a key directly (for tests and debugging only; not linearizable).
func (s *Store) Get(key string) (string, bool) {
	v, ok := s.data[key]
	return v, ok
}

// Len returns the number of keys.
func (s *Store) Len() int { return len(s.data) }

// Snapshot implements rsm.StateMachine. The encoding is deterministic.
func (s *Store) Snapshot() []byte {
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b []byte
	b = binary.AppendUvarint(b, uint64(len(keys)))
	for _, k := range keys {
		b = putString(b, k)
		b = putString(b, s.data[k])
	}
	ids := make([]uint64, 0, len(s.sessions))
	for id := range s.sessions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	b = binary.AppendUvarint(b, uint64(len(ids)))
	for _, id := range ids {
		ss := s.sessions[id]
		b = binary.AppendUvarint(b, id)
		b = binary.AppendUvarint(b, ss.seq)
		b = append(b, EncodeResultLen(ss.result)...)
	}
	return b
}

// EncodeResultLen encodes a result with a length prefix.
func EncodeResultLen(r Result) []byte {
	e := EncodeResult(r)
	return append(binary.AppendUvarint(nil, uint64(len(e))), e...)
}

// Restore implements rsm.StateMachine.
func (s *Store) Restore(data []byte) {
	s.data = map[string]string{}
	s.sessions = map[uint64]session{}
	if len(data) == 0 {
		return
	}
	r := reader{b: data}
	n := r.uvarint()
	for i := uint64(0); i < n; i++ {
		k := r.string()
		s.data[k] = r.string()
	}
	n = r.uvarint()
	for i := uint64(0); i < n; i++ {
		id := r.uvarint()
		seq := r.uvarint()
		res, err := DecodeResult([]byte(r.string()))
		if err != nil {
			panic(err)
		}
		s.sessions[id] = session{seq: seq, result: res}
	}
	if r.err != nil {
		panic(r.err)
	}
}
