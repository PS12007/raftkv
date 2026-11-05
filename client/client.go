// Package client is the raftkv client library.
//
// A Clerk issues one operation at a time. Every operation carries the
// Clerk's random client ID and a sequence number; retries reuse the same
// (ID, seq) so the servers execute each write exactly once even when a
// request is retried against a new leader after a timeout.
package client

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"sync"
	"time"

	pb "github.com/ps12007/raftkv/proto/raftkvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ErrNoServers is returned when the server list is empty.
var ErrNoServers = errors.New("client: no servers")

// Clerk is a raftkv client. Its methods are safe for concurrent use, but
// operations are serialised; use several Clerks for concurrency.
type Clerk struct {
	mu      sync.Mutex
	servers []string
	conns   map[string]*grpc.ClientConn
	leader  int
	id      uint64
	seq     uint64

	// AttemptTimeout bounds a single RPC attempt.
	AttemptTimeout time.Duration
	// Retries counts attempts beyond the first (for metrics).
	Retries uint64
}

// New returns a Clerk for the given server addresses.
func New(servers []string) *Clerk {
	var b [8]byte
	rand.Read(b[:])
	return &Clerk{
		servers:        append([]string(nil), servers...),
		conns:          map[string]*grpc.ClientConn{},
		id:             binary.LittleEndian.Uint64(b[:]) | 1,
		AttemptTimeout: time.Second,
	}
}

// ClientID returns the Clerk's deduplication ID.
func (c *Clerk) ClientID() uint64 { return c.id }

func (c *Clerk) conn(addr string) (pb.KVClient, error) {
	cc := c.conns[addr]
	if cc == nil {
		var err error
		cc, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		c.conns[addr] = cc
	}
	return pb.NewKVClient(cc), nil
}

func (c *Clerk) setLeader(addr string) {
	for i, s := range c.servers {
		if s == addr {
			c.leader = i
			return
		}
	}
	c.servers = append(c.servers, addr)
	c.leader = len(c.servers) - 1
}

// call retries fn against servers until it reports success or ctx expires.
func (c *Clerk) call(ctx context.Context, fn func(ctx context.Context, kc pb.KVClient) (errStr, leaderAddr string, err error)) error {
	if len(c.servers) == 0 {
		return ErrNoServers
	}
	backoff := 5 * time.Millisecond
	tried := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		addr := c.servers[c.leader]
		kc, err := c.conn(addr)
		var errStr, hint string
		if err == nil {
			actx, cancel := context.WithTimeout(ctx, c.AttemptTimeout)
			errStr, hint, err = fn(actx, kc)
			cancel()
		}
		if err == nil && errStr == "" {
			return nil
		}
		if err == nil && errStr != "WrongLeader" && errStr != "Timeout" {
			return errors.New(errStr) // permanent error
		}
		c.Retries++
		if hint != "" && hint != addr {
			c.setLeader(hint)
			continue
		}
		c.leader = (c.leader + 1) % len(c.servers)
		tried++
		if tried%len(c.servers) == 0 {
			// Went round every server: probably mid-election. Back off.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			if backoff < 200*time.Millisecond {
				backoff *= 2
			}
		}
	}
}

func (c *Clerk) do(ctx context.Context, op pb.OpType, key, value string) (*pb.KVReply, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	req := &pb.KVRequest{Op: op, Key: key, Value: value, ClientId: c.id, Seq: c.seq}
	var reply *pb.KVReply
	err := c.call(ctx, func(ctx context.Context, kc pb.KVClient) (string, string, error) {
		r, err := kc.Do(ctx, req)
		if err != nil {
			return "", "", err
		}
		reply = r
		return r.Err, r.LeaderAddr, nil
	})
	return reply, err
}

// Get returns the value of key and whether it exists. Reads go through the
// Raft log, so they are linearizable.
func (c *Clerk) Get(ctx context.Context, key string) (string, bool, error) {
	r, err := c.do(ctx, pb.OpType_OP_GET, key, "")
	if err != nil {
		return "", false, err
	}
	return r.Value, r.Found, nil
}

// Put sets key to value.
func (c *Clerk) Put(ctx context.Context, key, value string) error {
	_, err := c.do(ctx, pb.OpType_OP_PUT, key, value)
	return err
}

// Append appends value to key's value.
func (c *Clerk) Append(ctx context.Context, key, value string) error {
	_, err := c.do(ctx, pb.OpType_OP_APPEND, key, value)
	return err
}

// Delete removes key. It reports whether the key existed.
func (c *Clerk) Delete(ctx context.Context, key string) (bool, error) {
	r, err := c.do(ctx, pb.OpType_OP_DELETE, key, "")
	if err != nil {
		return false, err
	}
	return r.Found, nil
}

// AddServer adds a server to the cluster (single-server change).
func (c *Clerk) AddServer(ctx context.Context, id uint64, addr string) error {
	return c.changeMembership(ctx, &pb.MembershipRequest{Add: true, Id: id, Addr: addr})
}

// RemoveServer removes a server from the cluster.
func (c *Clerk) RemoveServer(ctx context.Context, id uint64) error {
	return c.changeMembership(ctx, &pb.MembershipRequest{Add: false, Id: id})
}

func (c *Clerk) changeMembership(ctx context.Context, req *pb.MembershipRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.call(ctx, func(ctx context.Context, kc pb.KVClient) (string, string, error) {
		r, err := kc.ChangeMembership(ctx, req)
		if err != nil {
			return "", "", err
		}
		e := r.Err
		if e == "raft: a membership change is already in progress" || e == "raft: leader has not committed an entry in its term yet" {
			e = "Timeout" // transient: retry
		}
		return e, r.LeaderAddr, nil
	})
}

// Status fetches a server's status as a generic JSON map.
func (c *Clerk) Status(ctx context.Context, addr string) (map[string]any, error) {
	c.mu.Lock()
	kc, err := c.conn(addr)
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	r, err := kc.Status(ctx, &pb.StatusRequest{})
	if err != nil {
		return nil, err
	}
	var m map[string]any
	return m, json.Unmarshal(r.Json, &m)
}

// Close releases connections.
func (c *Clerk) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cc := range c.conns {
		cc.Close()
	}
	c.conns = map[string]*grpc.ClientConn{}
}
