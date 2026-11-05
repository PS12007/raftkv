// Package server runs a raftkv node: the Raft state machine driven by a
// single event-loop goroutine, durable file storage, a gRPC peer transport
// and the client-facing gRPC KV API.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ps12007/raftkv/kv"
	pb "github.com/ps12007/raftkv/proto/raftkvpb"
	"github.com/ps12007/raftkv/raft"
	"github.com/ps12007/raftkv/rsm"
	"google.golang.org/grpc"
)

// Config configures a Node.
type Config struct {
	ID raft.NodeID
	// Addr is the gRPC listen address (peers and clients).
	Addr string
	// Peers is the bootstrap membership (including this node). Leave empty
	// to start a node that will be added to an existing cluster.
	Peers   []raft.Member
	DataDir string
	// HTTPAddr, if set, serves /status (JSON) for the dashboard.
	HTTPAddr       string
	TickInterval   time.Duration
	ElectionTicks  int
	HeartbeatTicks int
	MaxLogBytes    int  // snapshot threshold
	NoSync         bool // skip fsync (benchmarks only)
	DisablePreVote bool
	// DisableReadIndex sends Gets through the log instead of ReadIndex.
	DisableReadIndex bool
	RequestTimeout   time.Duration
	Logger           *log.Logger
}

func (c *Config) defaults() {
	if c.TickInterval == 0 {
		c.TickInterval = 10 * time.Millisecond
	}
	if c.ElectionTicks == 0 {
		c.ElectionTicks = 15
	}
	if c.HeartbeatTicks == 0 {
		c.HeartbeatTicks = 3
	}
	if c.MaxLogBytes == 0 {
		c.MaxLogBytes = 8 << 20
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 2 * time.Second
	}
}

// NodeStatus is published for the dashboard.
type NodeStatus struct {
	raft.Status
	Addr      string `json:"addr"`
	Keys      int    `json:"keys"`
	AppliedSM uint64 `json:"appliedSM"`
	Proposals uint64 `json:"proposals"`
	Time      int64  `json:"time"` // unix millis
}

// Node is a running raftkv server.
type Node struct {
	pb.UnimplementedKVServer
	cfg       Config
	storage   *raft.FileStorage
	core      *rsm.Core
	store     *kv.Store
	transport *Transport

	recvC chan raft.Message
	reqC  chan func()
	stopC chan struct{}
	doneC chan struct{}

	grpc     *grpc.Server
	lis      net.Listener
	httpSrv  *http.Server
	status   atomic.Pointer[NodeStatus]
	proposed atomic.Uint64
	stopOnce sync.Once
}

// Start opens storage, binds the listener and starts the event loop.
func Start(cfg Config) (*Node, error) {
	cfg.defaults()
	fs, err := raft.OpenFileStorage(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	fs.NoSync = cfg.NoSync
	r, err := raft.New(raft.Config{
		ID: cfg.ID, Peers: cfg.Peers, Storage: fs,
		ElectionTicks: cfg.ElectionTicks, HeartbeatTicks: cfg.HeartbeatTicks,
		PreVote: !cfg.DisablePreVote,
		Rand:    rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(cfg.ID))),
	})
	if err != nil {
		fs.Close()
		return nil, err
	}
	store := kv.NewStore()
	n := &Node{
		cfg: cfg, storage: fs, store: store,
		core:  rsm.New(r, store, cfg.MaxLogBytes),
		recvC: make(chan raft.Message, 8192),
		reqC:  make(chan func(), 8192),
		stopC: make(chan struct{}),
		doneC: make(chan struct{}),
	}
	n.lis, err = net.Listen("tcp", cfg.Addr)
	if err != nil {
		fs.Close()
		return nil, err
	}
	n.transport = NewTransport(cfg.ID, n.lis.Addr().String())
	n.grpc = grpc.NewServer(grpc.MaxRecvMsgSize(256 << 20))
	pb.RegisterRaftServer(n.grpc, &raftService{recv: n.receive, learn: n.transport.Learn})
	pb.RegisterKVServer(n.grpc, n)
	go n.grpc.Serve(n.lis)
	if cfg.HTTPAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/status", n.serveStatus)
		n.httpSrv = &http.Server{Addr: cfg.HTTPAddr, Handler: mux}
		go n.httpSrv.ListenAndServe()
	}
	n.transport.UpdatePeers(r.Members())
	n.publishStatus()
	go n.run()
	return n, nil
}

// Addr returns the bound gRPC address.
func (n *Node) Addr() string { return n.lis.Addr().String() }

// ID returns the node ID.
func (n *Node) ID() raft.NodeID { return n.cfg.ID }

// Transport exposes the transport (for fault injection).
func (n *Node) Transport() *Transport { return n.transport }

func (n *Node) receive(m raft.Message) {
	select {
	case n.recvC <- m:
	case <-n.stopC:
	}
}

func (n *Node) logf(format string, args ...any) {
	if n.cfg.Logger != nil {
		n.cfg.Logger.Printf("[node %d] "+format, append([]any{n.cfg.ID}, args...)...)
	}
}

// run is the event loop. It is the only goroutine that touches Raft.
func (n *Node) run() {
	defer close(n.doneC)
	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()
	statusEvery := time.NewTicker(50 * time.Millisecond)
	defer statusEvery.Stop()
	r := n.core.R
	members := fmt.Sprint(r.Members())
	for {
		select {
		case <-n.stopC:
			n.core.Close()
			return
		case <-ticker.C:
			r.Tick()
		case <-statusEvery.C:
			n.publishStatus()
			continue
		case m := <-n.recvC:
			r.Step(m)
			// Batch: drain whatever else is queued before persisting once.
		drain:
			for i := 0; i < 512; i++ {
				select {
				case m := <-n.recvC:
					r.Step(m)
				case f := <-n.reqC:
					f()
				default:
					break drain
				}
			}
		case f := <-n.reqC:
			f()
		drain2:
			for i := 0; i < 512; i++ {
				select {
				case f := <-n.reqC:
					f()
				case m := <-n.recvC:
					r.Step(m)
				default:
					break drain2
				}
			}
		}
		msgs := n.core.Flush()
		if ms := r.Members(); fmt.Sprint(ms) != members {
			members = fmt.Sprint(ms)
			n.transport.UpdatePeers(ms)
			n.logf("membership is now %v", ms)
		}
		n.transport.Send(msgs)
	}
}

// do runs f on the event loop and waits for it.
func (n *Node) do(ctx context.Context, f func()) error {
	done := make(chan struct{})
	select {
	case n.reqC <- func() { f(); close(done) }:
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopC:
		return errors.New("node stopped")
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopC:
		return errors.New("node stopped")
	}
}

func (n *Node) publishStatus() {
	st := &NodeStatus{
		Status: n.core.R.Status(), Addr: n.cfg.Addr, Keys: n.store.Len(),
		AppliedSM: n.core.Applied(), Proposals: n.proposed.Load(), Time: time.Now().UnixMilli(),
	}
	if n.lis != nil {
		st.Addr = n.lis.Addr().String()
	}
	n.status.Store(st)
}

// Info returns the latest published status.
func (n *Node) Info() *NodeStatus { return n.status.Load() }

// FreshInfo returns status computed on the event loop right now.
func (n *Node) FreshInfo(ctx context.Context) *NodeStatus {
	n.do(ctx, n.publishStatus)
	return n.Info()
}

func (n *Node) serveStatus(w http.ResponseWriter, req *http.Request) {
	n.FreshInfo(req.Context())
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(n.Info())
}

// Stop shuts the node down (like a crash, but closing files cleanly).
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		close(n.stopC)
		<-n.doneC
		n.transport.Close()
		n.grpc.Stop()
		if n.httpSrv != nil {
			n.httpSrv.Close()
		}
		n.storage.Close()
	})
}

// ---------- KV gRPC service ----------

type result struct {
	res    []byte
	err    error
	leader raft.NodeID
	addr   string
}

func (n *Node) submit(ctx context.Context, f func(cb rsm.Callback)) result {
	ch := make(chan result, 1)
	err := n.do(ctx, func() {
		f(func(res []byte, err error) {
			l, a := n.core.LeaderHint()
			ch <- result{res: res, err: err, leader: l, addr: a}
		})
	})
	if err != nil {
		return result{err: err}
	}
	select {
	case r := <-ch:
		return r
	case <-ctx.Done():
		return result{err: ctx.Err()}
	}
}

func errString(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, rsm.ErrWrongLeader):
		return "WrongLeader"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return "Timeout"
	}
	return err.Error()
}

// Do implements the KV service.
func (n *Node) Do(ctx context.Context, req *pb.KVRequest) (*pb.KVReply, error) {
	ctx, cancel := context.WithTimeout(ctx, n.cfg.RequestTimeout)
	defer cancel()
	op := kv.Op{Type: kv.OpType(req.Op), Key: req.Key, Value: req.Value, ClientID: req.ClientId, Seq: req.Seq}
	cmd := kv.EncodeOp(op)
	n.proposed.Add(1)
	r := n.submit(ctx, func(cb rsm.Callback) {
		if op.Type == kv.OpGet && !n.cfg.DisableReadIndex {
			n.core.SubmitRead(cmd, cb)
		} else {
			n.core.Submit(cmd, cb)
		}
	})
	reply := &pb.KVReply{Err: errString(r.err), LeaderHint: r.leader, LeaderAddr: r.addr}
	if r.err == nil {
		res, err := kv.DecodeResult(r.res)
		if err != nil {
			return nil, err
		}
		reply.Value, reply.Found = res.Value, res.Found
	}
	return reply, nil
}

// ChangeMembership implements the KV service.
func (n *Node) ChangeMembership(ctx context.Context, req *pb.MembershipRequest) (*pb.MembershipReply, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	m := raft.Member{ID: req.Id, Addr: req.Addr, Learner: req.Add && !req.Voter}
	r := n.submit(ctx, func(cb rsm.Callback) { n.core.SubmitConfChange(req.Add, m, cb) })
	return &pb.MembershipReply{Err: errString(r.err), LeaderHint: r.leader, LeaderAddr: r.addr}, nil
}

// Status implements the KV service (JSON-encoded NodeStatus).
func (n *Node) Status(ctx context.Context, _ *pb.StatusRequest) (*pb.StatusReply, error) {
	b, err := json.Marshal(n.FreshInfo(ctx))
	return &pb.StatusReply{Json: b}, err
}
