package server

import (
	"context"
	"io"
	"sync"
	"time"

	pb "github.com/ps12007/raftkv/proto/raftkvpb"
	"github.com/ps12007/raftkv/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Filter decides the fate of an outgoing message, for fault injection in
// tests, demos and benchmarks. It returns drop=true to discard it, or a delay.
type Filter func(from, to raft.NodeID) (drop bool, delay time.Duration)

// Transport sends Raft messages to peers over long-lived gRPC client
// streams, one per peer. Sends never block the Raft loop: if a peer's queue
// is full the message is dropped, which Raft tolerates.
type Transport struct {
	id   raft.NodeID
	addr string // advertised address
	mu   sync.Mutex
	peer map[raft.NodeID]*peerConn
	// filter is read under mu.
	filter Filter
	closed bool
}

type peerConn struct {
	addr string
	ch   chan raft.Message
	stop chan struct{}
}

// NewTransport returns a transport for node id, advertising addr.
func NewTransport(id raft.NodeID, addr string) *Transport {
	return &Transport{id: id, addr: addr, peer: map[raft.NodeID]*peerConn{}}
}

// SetFilter installs a fault-injection filter (nil removes it).
func (t *Transport) SetFilter(f Filter) {
	t.mu.Lock()
	t.filter = f
	t.mu.Unlock()
}

// UpdatePeers adds the members' addresses to the address book. Peers are
// not removed: a leader simply stops sending to servers that left.
func (t *Transport) UpdatePeers(members []raft.Member) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, m := range members {
		t.learnLocked(m.ID, m.Addr)
	}
}

// Learn records a peer address seen on an incoming message.
func (t *Transport) Learn(id raft.NodeID, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.learnLocked(id, addr)
}

func (t *Transport) learnLocked(id raft.NodeID, addr string) {
	if t.closed || id == t.id || addr == "" {
		return
	}
	if p := t.peer[id]; p != nil {
		if p.addr == addr {
			return
		}
		close(p.stop)
	}
	p := &peerConn{addr: addr, ch: make(chan raft.Message, 4096), stop: make(chan struct{})}
	t.peer[id] = p
	go t.runPeer(id, p)
}

// Send enqueues messages without blocking.
func (t *Transport) Send(msgs []raft.Message) {
	if len(msgs) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, m := range msgs {
		p := t.peer[m.To]
		if p == nil {
			continue
		}
		if t.filter != nil {
			drop, delay := t.filter(t.id, m.To)
			if drop {
				continue
			}
			if delay > 0 {
				m := m
				time.AfterFunc(delay, func() {
					select {
					case p.ch <- m:
					default:
					}
				})
				continue
			}
		}
		select {
		case p.ch <- m:
		default: // queue full: drop
		}
	}
}

func (t *Transport) runPeer(id raft.NodeID, p *peerConn) {
	backoff := 10 * time.Millisecond
	for {
		select {
		case <-p.stop:
			return
		default:
		}
		conn, err := grpc.NewClient(p.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			err = t.stream(conn, p)
			conn.Close()
		}
		select {
		case <-p.stop:
			return
		case <-time.After(backoff):
		}
		if backoff < time.Second {
			backoff *= 2
		}
		_ = err
	}
}

func (t *Transport) stream(conn *grpc.ClientConn, p *peerConn) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st, err := pb.NewRaftClient(conn).Stream(ctx)
	if err != nil {
		return err
	}
	for {
		select {
		case <-p.stop:
			st.CloseSend()
			return nil
		case m := <-p.ch:
			pm := toPB(m)
			pm.FromAddr = t.addr
			if err := st.Send(pm); err != nil {
				return err
			}
		}
	}
}

// Close stops all peer connections.
func (t *Transport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	for id, p := range t.peer {
		close(p.stop)
		delete(t.peer, id)
	}
}

// raftService receives peer streams.
type raftService struct {
	pb.UnimplementedRaftServer
	recv  func(raft.Message)
	learn func(raft.NodeID, string)
}

func (s *raftService) Stream(st pb.Raft_StreamServer) error {
	for {
		m, err := st.Recv()
		if err == io.EOF {
			return st.SendAndClose(&pb.Empty{})
		}
		if err != nil {
			return err
		}
		if m.FromAddr != "" {
			s.learn(m.From, m.FromAddr)
		}
		s.recv(fromPB(m))
	}
}

// ---------- conversions ----------

func toPB(m raft.Message) *pb.RaftMessage {
	out := &pb.RaftMessage{
		Type: uint32(m.Type), From: m.From, To: m.To, Term: m.Term,
		LastLogIndex: m.LastLogIndex, LastLogTerm: m.LastLogTerm, Granted: m.Granted,
		PrevLogIndex: m.PrevLogIndex, PrevLogTerm: m.PrevLogTerm, LeaderCommit: m.LeaderCommit,
		Success: m.Success, MatchIndex: m.MatchIndex, ConflictIndex: m.ConflictIndex, ConflictTerm: m.ConflictTerm,
	}
	if len(m.Entries) > 0 {
		out.Entries = make([]*pb.Entry, len(m.Entries))
		for i, e := range m.Entries {
			out.Entries[i] = &pb.Entry{Term: e.Term, Index: e.Index, Type: uint32(e.Type), Data: e.Data}
		}
	}
	if s := m.Snapshot; s != nil {
		ps := &pb.Snapshot{Index: s.Index, Term: s.Term, Data: s.Data}
		for _, mm := range s.Members {
			ps.Members = append(ps.Members, &pb.Member{Id: mm.ID, Addr: mm.Addr})
		}
		out.Snapshot = ps
	}
	return out
}

func fromPB(m *pb.RaftMessage) raft.Message {
	out := raft.Message{
		Type: raft.MsgType(m.Type), From: m.From, To: m.To, Term: m.Term,
		LastLogIndex: m.LastLogIndex, LastLogTerm: m.LastLogTerm, Granted: m.Granted,
		PrevLogIndex: m.PrevLogIndex, PrevLogTerm: m.PrevLogTerm, LeaderCommit: m.LeaderCommit,
		Success: m.Success, MatchIndex: m.MatchIndex, ConflictIndex: m.ConflictIndex, ConflictTerm: m.ConflictTerm,
	}
	if len(m.Entries) > 0 {
		out.Entries = make([]raft.Entry, len(m.Entries))
		for i, e := range m.Entries {
			out.Entries[i] = raft.Entry{Term: e.Term, Index: e.Index, Type: raft.EntryType(e.Type), Data: e.Data}
		}
	}
	if s := m.Snapshot; s != nil {
		rs := &raft.Snapshot{Index: s.Index, Term: s.Term, Data: s.Data}
		for _, mm := range s.Members {
			rs.Members = append(rs.Members, raft.Member{ID: mm.Id, Addr: mm.Addr})
		}
		out.Snapshot = rs
	}
	return out
}
