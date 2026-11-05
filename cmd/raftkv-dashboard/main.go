// Command raftkv-dashboard serves a live web view of a raftkv cluster.
//
// Demo mode (default) starts an in-process 5-node cluster with client load
// and fault-injection buttons:
//
//	raftkv-dashboard -listen :8080 -demo 5
//
// Or point it at running servers' HTTP status endpoints:
//
//	raftkv-dashboard -nodes http://localhost:8001/status,http://localhost:8002/status
package main

import (
	"flag"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/ps12007/raftkv/cluster"
	"github.com/ps12007/raftkv/dashboard"
)

func main() {
	listen := flag.String("listen", ":8080", "HTTP listen address")
	nodes := flag.String("nodes", "", "comma-separated /status URLs of running servers")
	demo := flag.Int("demo", 5, "if -nodes is empty, run an in-process cluster of this size")
	clients := flag.Int("clients", 4, "demo client load generators")
	maxLog := flag.Int("max-log-bytes", 16<<10, "demo snapshot threshold (small, so snapshots are visible)")
	flag.Parse()

	var srv *dashboard.Server
	if *nodes != "" {
		srv = dashboard.New(&dashboard.RemoteSource{URLs: strings.Split(*nodes, ","), HTTP: &http.Client{Timeout: time.Second}})
	} else {
		c, err := cluster.Start(*demo, cluster.Options{TickInterval: 20 * time.Millisecond, MaxLogBytes: *maxLog})
		if err != nil {
			log.Fatal(err)
		}
		defer c.Close()
		srv = dashboard.NewDemo(c, *clients)
		log.Printf("demo cluster of %d nodes: %s", *demo, strings.Join(c.Addrs(), ","))
	}
	log.Printf("dashboard on http://localhost%s", *listen)
	log.Fatal(http.ListenAndServe(*listen, srv.Handler()))
}
