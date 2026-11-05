# Benchmarks

All numbers come from `cmd/raftkv-bench`. It runs a real cluster: every node is a full server with gRPC over localhost and durable WAL storage. All nodes share one 4-vCPU VM (Intel Xeon @ 2.1 GHz). Because every node competes for the same 4 cores, these numbers show **relative** costs (cluster size, faults, fsync). They are not absolute capacity figures.

Workload: 32 closed-loop clients, 50% `Put` with 128-byte values and 50% `Get`. Every read goes through the Raft log, so reads and writes cost about the same. Each run lasts 6 s. For fault scenarios, the fault starts at 2 s and is undone at 4 s.

- **longest stall** is the longest window in which *no* client completed an operation (that is, unavailability).
- **client retries** counts attempts that hit a non-leader or timed out. A no-fault run with 32 retries means each client's very first request went to a follower and was redirected.

Reproduce:

```bash
go build -o bin/ ./cmd/...
```

```bash
./bin/raftkv-bench -sizes 1,3,5,7 -faults none,leader-crash,follower-crash,partition,lossy,slow -duration 6s
```

## Throughput and latency vs cluster size (fsync on)

| nodes | ops/s | p50 ms | p90 ms | p99 ms |
|---:|---:|---:|---:|---:|
| 1 | 23,561 | 1.22 | 2.02 | 3.90 |
| 3 | 11,020 | 2.65 | 4.16 | 7.27 |
| 5 | 10,214 | 2.91 | 4.21 | 7.69 |
| 7 | 8,529 | 3.49 | 5.07 | 8.85 |

Same workload with fsync disabled (`-nosync`), which isolates the protocol from the disk:

| nodes | ops/s | p50 ms | p99 ms |
|---:|---:|---:|---:|
| 1 | 46,391 | 0.59 | 3.05 |
| 3 | 25,443 | 1.12 | 4.56 |
| 5 | 21,441 | 1.34 | 5.04 |
| 7 | 18,804 | 1.54 | 5.38 |

Going from 1 to 3 nodes roughly halves throughput, because every commit now needs a network round trip plus a follower fsync. After that, extra nodes cost less: the leader only waits for the fastest majority. The leader's CPU (encoding and sending AppendEntries to N−1 peers) is what slowly drags 5 and 7 nodes down. fsync costs about 2× throughput at every size.

Concurrency sweep (3 nodes, fsync on). Batching means more clients mainly buy throughput at the cost of latency:

| clients | ops/s | p50 ms | p99 ms |
|---:|---:|---:|---:|
| 1 | 1,537 | 0.61 | 1.23 |
| 8 | 5,899 | 1.27 | 3.06 |
| 32 | 11,835 | 2.47 | 6.86 |
| 128 | 14,459 | 7.71 | 21.96 |

## Under faults (fsync on)

| nodes | fault | ops/s | p50 ms | p99 ms | max ms | longest stall ms | client retries |
|---:|---|---:|---:|---:|---:|---:|---:|
| 3 | leader crash | 9,304 | 2.74 | 11.35 | 350 | 328 | 1,088 |
| 5 | leader crash | 5,430 | 4.68 | 19.09 | 357 | 195 | 1,786 |
| 7 | leader crash | 5,120 | 4.99 | 21.08 | 216 | 181 | 2,280 |
| 3 | follower crash | 8,516 | 3.18 | 11.49 | 42 | 19 | 0 |
| 5 | follower crash | 7,381 | 3.81 | 12.30 | 26 | 14 | 32 |
| 7 | follower crash | 6,294 | 4.43 | 14.89 | 34 | 23 | 0 |
| 3 | leader partitioned away | 10,661 | 2.60 | 8.26 | 293 | 250 | 669 |
| 5 | leader partitioned away | 10,234 | 2.75 | 8.58 | 238 | 231 | 96 |
| 7 | leader partitioned away | 8,164 | 3.46 | 10.04 | 231 | 225 | 96 |
| 3 | 5% message loss | 9,378 | 2.59 | 25.80 | 60 | 59 | 32 |
| 5 | 5% message loss | 8,187 | 2.90 | 25.98 | 37 | 28 | 32 |
| 7 | 5% message loss | 7,285 | 3.27 | 26.86 | 32 | 30 | 32 |
| 3 | +2–5 ms link delay | 8,580 | 2.87 | 9.86 | 29 | 23 | 32 |
| 5 | +2–5 ms link delay | 8,039 | 3.10 | 10.57 | 20 | 13 | 0 |
| 7 | +2–5 ms link delay | 6,972 | 3.62 | 12.16 | 29 | 19 | 32 |

What the fault runs show:

- **Leader crash** costs one election, about 180–330 ms of unavailability. The election timeout is 150–300 ms (15–30 ticks of 10 ms). Clients see a refused connection, rotate to another server, and follow its leader hint.
- **Leader partitioned away**: the isolated leader steps down through *check-quorum* after one election timeout. The majority side elects a new leader. Total stall is about 230–290 ms.
- **Follower crash and restart is invisible** (14–23 ms worst stall). Before Pre-Vote was added, this scenario stalled for about 650 ms. The restarted follower timed out before the leader's reconnect backoff expired, bumped its term, and forced the healthy leader to step down. Pre-Vote (dissertation §9.6) fixed it.
- **Message loss** leaves the median alone but raises p99 to about 26 ms. A lost AppendEntries is only retransmitted on the next heartbeat (30 ms).
