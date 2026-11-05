// Command raftkv-server runs one raftkv node.
//
// Bootstrap a 3-node cluster:
//
//	raftkv-server -id 1 -addr :7001 -http :8001 -peers 1=localhost:7001,2=localhost:7002,3=localhost:7003 -data data/1
//	raftkv-server -id 2 -addr :7002 -http :8002 -peers ... -data data/2
//	raftkv-server -id 3 -addr :7003 -http :8003 -peers ... -data data/3
//
// Add a fourth node: start it with -join (no -peers), then run
// `raftkv-cli -servers localhost:7001 add-server 4 localhost:7004`.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ps12007/raftkv/raft"
	"github.com/ps12007/raftkv/server"
)

func parsePeers(s string) ([]raft.Member, error) {
	var out []raft.Member
	for _, part := range strings.Split(s, ",") {
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("bad peer %q (want id=host:port)", part)
		}
		n, err := strconv.ParseUint(id, 10, 64)
		if err != nil {
			return nil, err
		}
		out = append(out, raft.Member{ID: n, Addr: addr})
	}
	return out, nil
}

func main() {
	id := flag.Uint64("id", 0, "node ID (non-zero)")
	addr := flag.String("addr", ":7001", "gRPC listen address")
	peers := flag.String("peers", "", "bootstrap membership: id=host:port,...")
	join := flag.Bool("join", false, "start empty and wait to be added to an existing cluster")
	data := flag.String("data", "", "data directory (default data/<id>)")
	httpAddr := flag.String("http", "", "HTTP status address for the dashboard, e.g. :8001")
	tick := flag.Duration("tick", 10*time.Millisecond, "Raft tick interval")
	maxLog := flag.Int("max-log-bytes", 8<<20, "snapshot when the log exceeds this size")
	nosync := flag.Bool("nosync", false, "disable fsync (unsafe; benchmarking only)")
	verbose := flag.Bool("v", false, "log membership changes")
	flag.Parse()
	if *id == 0 {
		log.Fatal("-id is required")
	}
	ms, err := parsePeers(*peers)
	if err != nil {
		log.Fatal(err)
	}
	if !*join && len(ms) == 0 {
		log.Fatal("either -peers or -join is required")
	}
	if *data == "" {
		*data = fmt.Sprintf("data/%d", *id)
	}
	cfg := server.Config{
		ID: *id, Addr: *addr, Peers: ms, DataDir: *data, HTTPAddr: *httpAddr,
		TickInterval: *tick, MaxLogBytes: *maxLog, NoSync: *nosync,
	}
	if *join {
		cfg.Peers = nil
	}
	if *verbose {
		cfg.Logger = log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)
	}
	n, err := server.Start(cfg)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("raftkv node %d listening on %s (data %s)", *id, n.Addr(), *data)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	n.Stop()
}
