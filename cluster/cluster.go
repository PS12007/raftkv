// Package cluster runs a real raftkv cluster (real gRPC over localhost and
// real file storage) inside one process, with fault injection: crash and
// restart nodes, partition the network, drop and delay messages. It backs
// the integration tests, the benchmarks and the dashboard demo.
package cluster

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/ps12007/raftkv/raft"
	"github.com/ps12007/raftkv/server"
)

// Options configures a Cluster.
type Options struct {
	Dir          string // data directory root (a temp dir if empty)
	TickInterval time.Duration
	MaxLogBytes  int
	NoSync       bool
	Logger       *log.Logger
}

// Cluster is a set of in-process nodes.
type Cluster struct {
	opts   Options
	mu     sync.Mutex
	nodes  map[raft.NodeID]*server.Node
	cfgs   map[raft.NodeID]server.Config
	next   raft.NodeID
	ownDir bool

	// fault state, read by transport filters
	fmu      sync.Mutex
	group    map[raft.NodeID]int
	dropRate float64
	delay    time.Duration
	jitter   time.Duration
	rng      *rand.Rand
}

func freeAddr() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer l.Close()
	return l.Addr().String(), nil
}

// Start launches an n-node cluster.
func Start(n int, opts Options) (*Cluster, error) {
	c := &Cluster{opts: opts, nodes: map[raft.NodeID]*server.Node{}, cfgs: map[raft.NodeID]server.Config{},
		group: map[raft.NodeID]int{}, rng: rand.New(rand.NewSource(time.Now().UnixNano()))}
	if c.opts.Dir == "" {
		d, err := os.MkdirTemp("", "raftkv-cluster-")
		if err != nil {
			return nil, err
		}
		c.opts.Dir, c.ownDir = d, true
	}
	var peers []raft.Member
	for i := 1; i <= n; i++ {
		addr, err := freeAddr()
		if err != nil {
			return nil, err
		}
		peers = append(peers, raft.Member{ID: raft.NodeID(i), Addr: addr})
	}
	c.next = raft.NodeID(n + 1)
	for _, p := range peers {
		c.cfgs[p.ID] = c.nodeConfig(p.ID, p.Addr, peers)
		if err := c.Restart(p.ID); err != nil {
			c.Close()
			return nil, err
		}
	}
	return c, nil
}

func (c *Cluster) nodeConfig(id raft.NodeID, addr string, peers []raft.Member) server.Config {
	return server.Config{
		ID: id, Addr: addr, Peers: peers,
		DataDir:      filepath.Join(c.opts.Dir, fmt.Sprintf("node-%d", id)),
		TickInterval: c.opts.TickInterval, MaxLogBytes: c.opts.MaxLogBytes, NoSync: c.opts.NoSync,
		Logger: c.opts.Logger,
	}
}

func (c *Cluster) filter(from, to raft.NodeID) (bool, time.Duration) {
	c.fmu.Lock() // rng is not goroutine-safe
	defer c.fmu.Unlock()
	if c.group[from] != c.group[to] {
		return true, 0
	}
	if c.dropRate > 0 && c.rng.Float64() < c.dropRate {
		return true, 0
	}
	d := c.delay
	if c.jitter > 0 {
		d += time.Duration(c.rng.Int63n(int64(c.jitter)))
	}
	return false, d
}

// Restart starts (or restarts after Crash) node id from its data directory.
func (c *Cluster) Restart(id raft.NodeID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.nodes[id] != nil {
		return nil
	}
	cfg, ok := c.cfgs[id]
	if !ok {
		return fmt.Errorf("unknown node %d", id)
	}
	n, err := server.Start(cfg)
	if err != nil {
		return err
	}
	n.Transport().SetFilter(c.filter)
	c.nodes[id] = n
	return nil
}

// Crash stops node id abruptly (its data directory is kept).
func (c *Cluster) Crash(id raft.NodeID) {
	c.mu.Lock()
	n := c.nodes[id]
	delete(c.nodes, id)
	c.mu.Unlock()
	if n != nil {
		n.Stop()
	}
}

// Partition splits nodes into groups; nodes not listed join group 0.
func (c *Cluster) Partition(groups ...[]raft.NodeID) {
	c.fmu.Lock()
	defer c.fmu.Unlock()
	c.group = map[raft.NodeID]int{}
	for i, g := range groups {
		for _, id := range g {
			c.group[id] = i
		}
	}
}

// Heal removes all partitions.
func (c *Cluster) Heal() { c.Partition() }

// SetNetwork sets the message drop rate and delay (base + random jitter).
func (c *Cluster) SetNetwork(dropRate float64, delay, jitter time.Duration) {
	c.fmu.Lock()
	defer c.fmu.Unlock()
	c.dropRate, c.delay, c.jitter = dropRate, delay, jitter
}

// IDs returns all configured node IDs (running or crashed).
func (c *Cluster) IDs() []raft.NodeID {
	c.mu.Lock()
	defer c.mu.Unlock()
	var ids []raft.NodeID
	for id := range c.cfgs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// Addr returns node id's gRPC address.
func (c *Cluster) Addr(id raft.NodeID) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cfgs[id].Addr
}

// Addrs returns every node's gRPC address.
func (c *Cluster) Addrs() []string {
	var out []string
	for _, id := range c.IDs() {
		out = append(out, c.Addr(id))
	}
	return out
}

// Node returns the running node id, or nil if crashed.
func (c *Cluster) Node(id raft.NodeID) *server.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

// Statuses returns the status of every running node.
func (c *Cluster) Statuses() map[raft.NodeID]*server.NodeStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[raft.NodeID]*server.NodeStatus{}
	for id, n := range c.nodes {
		out[id] = n.Info()
	}
	return out
}

// Leader returns the ID of the running leader with the highest term, or 0.
func (c *Cluster) Leader() raft.NodeID {
	var best raft.NodeID
	var term uint64
	for id, s := range c.Statuses() {
		if s.State == "leader" && s.Term >= term {
			best, term = id, s.Term
		}
	}
	return best
}

// WaitLeader waits for a leader to emerge.
func (c *Cluster) WaitLeader(timeout time.Duration) (raft.NodeID, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := c.Leader(); l != 0 {
			return l, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return 0, fmt.Errorf("no leader after %v", timeout)
}

// NewNode prepares (and starts) a fresh, empty node that can then be added
// with Clerk.AddServer.
func (c *Cluster) NewNode() (raft.NodeID, string, error) {
	addr, err := freeAddr()
	if err != nil {
		return 0, "", err
	}
	c.mu.Lock()
	id := c.next
	c.next++
	c.cfgs[id] = c.nodeConfig(id, addr, nil)
	c.mu.Unlock()
	return id, addr, c.Restart(id)
}

// Forget stops a node and removes it from the cluster's bookkeeping (after
// it has been removed from the Raft configuration).
func (c *Cluster) Forget(id raft.NodeID) {
	c.Crash(id)
	c.mu.Lock()
	delete(c.cfgs, id)
	c.mu.Unlock()
}

// Close stops all nodes and deletes the data directory if it was temporary.
func (c *Cluster) Close() {
	c.mu.Lock()
	nodes := c.nodes
	c.nodes = map[raft.NodeID]*server.Node{}
	c.mu.Unlock()
	for _, n := range nodes {
		n.Stop()
	}
	if c.ownDir {
		os.RemoveAll(c.opts.Dir)
	}
}
