// Package dashboard serves a live web view of a raftkv cluster: node roles,
// terms, log replication (per-node log strips coloured by term, commit and
// snapshot markers, leader match indexes) and an event feed. In demo mode it
// drives an in-process cluster and exposes fault-injection controls.
package dashboard

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ps12007/raftkv/client"
	"github.com/ps12007/raftkv/cluster"
	"github.com/ps12007/raftkv/raft"
	"github.com/ps12007/raftkv/server"
)

//go:embed index.html
var static embed.FS

// NodeView is one node as shown on the dashboard.
type NodeView struct {
	ID       uint64             `json:"id"`
	Up       bool               `json:"up"`
	Isolated bool               `json:"isolated"`
	Status   *server.NodeStatus `json:"status,omitempty"`
}

// State is what the browser receives on every update.
type State struct {
	Time       int64      `json:"time"`
	Demo       bool       `json:"demo"`
	Nodes      []NodeView `json:"nodes"`
	Events     []Event    `json:"events"`
	Throughput float64    `json:"throughput"` // client ops/s (demo load)
	Load       bool       `json:"load"`
	Lossy      bool       `json:"lossy"`
}

// Event is an entry in the event feed.
type Event struct {
	Time int64  `json:"time"`
	Text string `json:"text"`
	Kind string `json:"kind"`
}

// Source provides node statuses.
type Source interface {
	Nodes(ctx context.Context) []NodeView
}

// RemoteSource polls the /status endpoints of running servers.
type RemoteSource struct {
	URLs []string // e.g. http://localhost:8001/status
	HTTP *http.Client
}

// Nodes implements Source.
func (r *RemoteSource) Nodes(ctx context.Context) []NodeView {
	out := make([]NodeView, len(r.URLs))
	var wg sync.WaitGroup
	for i, u := range r.URLs {
		wg.Add(1)
		go func(i int, u string) {
			defer wg.Done()
			out[i] = NodeView{ID: uint64(i + 1)}
			req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
			resp, err := r.HTTP.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			var st server.NodeStatus
			if json.NewDecoder(resp.Body).Decode(&st) == nil {
				out[i] = NodeView{ID: st.ID, Up: true, Status: &st}
			}
		}(i, u)
	}
	wg.Wait()
	return out
}

// Server is the dashboard HTTP server.
type Server struct {
	src     Source
	cluster *cluster.Cluster // demo mode only

	mu       sync.Mutex
	events   []Event
	prev     map[uint64]NodeView
	isolated map[raft.NodeID]bool
	lossy    bool

	load    atomic.Bool
	ops     atomic.Uint64
	opsPrev uint64
	opsAt   time.Time
	tput    float64
}

// New creates a dashboard over a source.
func New(src Source) *Server {
	return &Server{src: src, prev: map[uint64]NodeView{}, isolated: map[raft.NodeID]bool{}, opsAt: time.Now()}
}

// NewDemo creates a dashboard that runs its own in-process cluster and a
// background client load.
func NewDemo(c *cluster.Cluster, clients int) *Server {
	s := New(&demoSource{c: c})
	s.cluster = c
	s.load.Store(true)
	for i := 0; i < clients; i++ {
		go s.runClient(i)
	}
	return s
}

type demoSource struct{ c *cluster.Cluster }

func (d *demoSource) Nodes(ctx context.Context) []NodeView {
	var out []NodeView
	for _, id := range d.c.IDs() {
		v := NodeView{ID: id}
		if n := d.c.Node(id); n != nil {
			v.Up = true
			v.Status = n.FreshInfo(ctx)
		}
		out = append(out, v)
	}
	return out
}

func (s *Server) runClient(i int) {
	ck := client.New(s.cluster.Addrs())
	ck.AttemptTimeout = 300 * time.Millisecond
	for j := 0; ; j++ {
		if !s.load.Load() {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := ck.Put(ctx, fmt.Sprintf("key-%d", j%50), fmt.Sprint(j))
		cancel()
		if err == nil {
			s.ops.Add(1)
		}
		time.Sleep(20 * time.Millisecond) // keep the log readable on screen
	}
}

func (s *Server) addEvent(kind, format string, args ...any) {
	s.events = append(s.events, Event{Time: time.Now().UnixMilli(), Kind: kind, Text: fmt.Sprintf(format, args...)})
	if len(s.events) > 60 {
		s.events = s.events[len(s.events)-60:]
	}
}

func (s *Server) state(ctx context.Context) State {
	nodes := s.src.Nodes(ctx)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range nodes {
		n := &nodes[i]
		n.Isolated = s.isolated[n.ID]
		p, seen := s.prev[n.ID]
		switch {
		case !seen:
		case p.Up && !n.Up:
			s.addEvent("down", "node %d is down", n.ID)
		case !p.Up && n.Up:
			s.addEvent("up", "node %d is back (term %d)", n.ID, n.Status.Term)
		case p.Up && n.Up:
			if n.Status.State == "leader" && p.Status.State != "leader" {
				s.addEvent("leader", "node %d elected leader for term %d", n.ID, n.Status.Term)
			}
			if p.Status.State == "leader" && n.Status.State != "leader" {
				s.addEvent("stepdown", "node %d stepped down (term %d)", n.ID, n.Status.Term)
			}
			// Snapshots a node takes itself are routine; report only those
			// installed from the leader (the node was behind the snapshot).
			if n.Status.SnapIndex > p.Status.SnapIndex && p.Status.AppliedSM < n.Status.SnapIndex &&
				n.Status.SnapIndex-p.Status.AppliedSM > 50 {
				s.addEvent("snapshot", "node %d installed a snapshot from the leader (jumped %d → %d)", n.ID, p.Status.AppliedSM, n.Status.SnapIndex)
			}
			if fmt.Sprint(n.Status.Members) != fmt.Sprint(p.Status.Members) {
				var ids []uint64
				for _, m := range n.Status.Members {
					ids = append(ids, m.ID)
				}
				s.addEvent("config", "node %d now sees members %v", n.ID, ids)
			}
		}
		s.prev[n.ID] = *n
	}
	now := time.Now()
	if dt := now.Sub(s.opsAt).Seconds(); dt >= 0.5 {
		cur := s.ops.Load()
		s.tput = float64(cur-s.opsPrev) / dt
		s.opsPrev, s.opsAt = cur, now
	}
	return State{Time: now.UnixMilli(), Demo: s.cluster != nil, Nodes: nodes,
		Events: append([]Event{}, s.events...), Throughput: s.tput, Load: s.load.Load(), Lossy: s.lossy}
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		b, _ := static.ReadFile("index.html")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(b)
	})
	mux.HandleFunc("/api/state", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s.state(r.Context()))
	})
	mux.HandleFunc("/api/stream", s.stream)
	mux.HandleFunc("/api/control", s.control)
	return mux
}

// stream pushes state as Server-Sent Events.
func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	t := time.NewTicker(150 * time.Millisecond)
	defer t.Stop()
	for {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		b, _ := json.Marshal(s.state(ctx))
		cancel()
		if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
			return
		}
		fl.Flush()
		select {
		case <-r.Context().Done():
			return
		case <-t.C:
		}
	}
}

func (s *Server) control(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if s.cluster == nil {
		http.Error(w, "controls are only available in demo mode", http.StatusBadRequest)
		return
	}
	c := s.cluster
	action := r.URL.Query().Get("action")
	id64, _ := strconv.ParseUint(r.URL.Query().Get("id"), 10, 64)
	id := raft.NodeID(id64)
	s.mu.Lock()
	defer s.mu.Unlock()
	var err error
	switch action {
	case "crash":
		c.Crash(id)
		s.addEvent("fault", "crashed node %d", id)
	case "restart":
		err = c.Restart(id)
		s.addEvent("fault", "restarting node %d", id)
	case "isolate":
		s.isolated[id] = !s.isolated[id]
		s.applyPartition()
		s.addEvent("fault", "node %d isolated=%v", id, s.isolated[id])
	case "heal":
		s.isolated = map[raft.NodeID]bool{}
		s.applyPartition()
		s.lossy = false
		c.SetNetwork(0, 0, 0)
		s.addEvent("fault", "network healed")
	case "lossy":
		s.lossy = !s.lossy
		if s.lossy {
			c.SetNetwork(0.2, 2*time.Millisecond, 20*time.Millisecond)
		} else {
			c.SetNetwork(0, 0, 0)
		}
		s.addEvent("fault", "lossy network=%v (20%% drops, 2-22ms delay)", s.lossy)
	case "crash-leader":
		if l := c.Leader(); l != 0 {
			c.Crash(l)
			s.addEvent("fault", "crashed leader %d", l)
		}
	case "load":
		s.load.Store(!s.load.Load())
	case "add":
		go func() {
			nid, addr, err := c.NewNode()
			if err == nil {
				ck := client.New(c.Addrs())
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				err = ck.AddServer(ctx, nid, addr)
				cancel()
				ck.Close()
			}
			s.mu.Lock()
			s.addEvent("config", "add server %d: %v", nid, errText(err))
			s.mu.Unlock()
		}()
	case "remove":
		go func() {
			ck := client.New(c.Addrs())
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := ck.RemoveServer(ctx, id)
			cancel()
			ck.Close()
			if err == nil {
				c.Forget(id)
			}
			s.mu.Lock()
			s.addEvent("config", "remove server %d: %v", id, errText(err))
			s.mu.Unlock()
		}()
	default:
		err = fmt.Errorf("unknown action %q", action)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func errText(err error) string {
	if err == nil {
		return "ok"
	}
	return err.Error()
}

// applyPartition puts every isolated node in its own group.
func (s *Server) applyPartition() {
	groups := [][]raft.NodeID{nil}
	for _, id := range s.cluster.IDs() {
		if s.isolated[id] {
			groups = append(groups, []raft.NodeID{id})
		} else {
			groups[0] = append(groups[0], id)
		}
	}
	s.cluster.Partition(groups...)
}
