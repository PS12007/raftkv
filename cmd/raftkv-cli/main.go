// Command raftkv-cli talks to a raftkv cluster.
//
//	raftkv-cli -servers localhost:7001,localhost:7002 put k v
//	raftkv-cli -servers ... get k
//	raftkv-cli -servers ... append k v
//	raftkv-cli -servers ... delete k
//	raftkv-cli -servers ... status
//	raftkv-cli -servers ... add-server <id> <addr>
//	raftkv-cli -servers ... remove-server <id>
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ps12007/raftkv/client"
)

func main() {
	servers := flag.String("servers", "localhost:7001,localhost:7002,localhost:7003", "comma-separated server addresses")
	timeout := flag.Duration("timeout", 10*time.Second, "overall timeout")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: raftkv-cli [-servers a,b,c] get|put|append|delete|status|add-server|remove-server args...")
		flag.PrintDefaults()
	}
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}
	addrs := strings.Split(*servers, ",")
	ck := client.New(addrs)
	defer ck.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	need := func(n int) {
		if len(args) != n+1 {
			flag.Usage()
			os.Exit(2)
		}
	}
	var err error
	switch args[0] {
	case "get":
		need(1)
		var v string
		var ok bool
		v, ok, err = ck.Get(ctx, args[1])
		if err == nil {
			if !ok {
				fmt.Println("(not found)")
			} else {
				fmt.Println(v)
			}
		}
	case "put":
		need(2)
		err = ck.Put(ctx, args[1], args[2])
	case "append":
		need(2)
		err = ck.Append(ctx, args[1], args[2])
	case "delete":
		need(1)
		_, err = ck.Delete(ctx, args[1])
	case "add-server":
		need(2)
		var id uint64
		id, err = strconv.ParseUint(args[1], 10, 64)
		if err == nil {
			err = ck.AddServer(ctx, id, args[2])
		}
	case "remove-server":
		need(1)
		var id uint64
		id, err = strconv.ParseUint(args[1], 10, 64)
		if err == nil {
			err = ck.RemoveServer(ctx, id)
		}
	case "status":
		fmt.Printf("%-18s %-4s %-14s %5s %7s %7s %6s\n", "ADDRESS", "ID", "ROLE", "TERM", "COMMIT", "LAST", "SNAP")
		for _, a := range addrs {
			st, e := ck.Status(ctx, a)
			if e != nil {
				fmt.Printf("%-18s %-4s %s\n", a, "-", "unreachable")
				continue
			}
			num := func(k string) int64 { v, _ := st[k].(float64); return int64(v) }
			role, _ := st["state"].(string)
			if num("leader") == num("id") && role == "leader" {
				role = "leader *"
			}
			fmt.Printf("%-18s %-4d %-14s %5d %7d %7d %6d\n", a, num("id"), role, num("term"), num("commit"), num("lastIndex"), num("snapIndex"))
		}
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
