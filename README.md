# minikv

A 3-node replicated key-value store in Go, built on Raft (`hashicorp/raft`). I wrote a fault-injecting load tester for it, then measured failover, exactly-once writes and admission control under load. Along the way I found and fixed a client bug that made failover 7x slower than it needed to be.

**Stack:** Go, hashicorp/raft, BoltDB, HTTP/JSON

## Results at a glance

| What I tested | Result |
|---|---|
| Kill the leader under load | New leader in ~2 s; p99 stayed at 22 ms; 30,000/30,000 writes applied exactly once |
| Client bug fix (leader-cache eviction) | Failover recovery cut from ~20 s to ~2 s; max latency 11.1 s → 2.3 s |
| 5% lost acks, with vs. without idempotency keys | 0 duplicates vs. 189 duplicates (one per retry) |
| Admission-control sweep at 200 concurrent clients | An over-tight limit cut throughput 30x; limits must come from measured capacity |

All numbers come from a 3-node cluster on one Windows laptop over localhost. They show relative behavior, not production network numbers.

## How it works

```
             ┌──────────────┐
  client ───▶│  n1 (leader) │──── replicates log ────┐
  (retries,  └──────────────┘                        ▼
   idem keys)        ▲                      ┌──────────────┐ ┌──────────────┐
      │              │ 421 + leader hint    │ n2 (follower)│ │ n3 (follower)│
      └──────────────┘                      └──────────────┘ └──────────────┘
```

- **Replication:** every write is a Raft log entry. It's acknowledged only after a majority (2 of 3) has persisted it, so losing any one node loses no data.
- **Leader routing:** only the leader accepts writes. Followers return HTTP 421 with the leader's address, and the client follows the hint.
- **Linearizable reads:** before answering a read, the leader confirms with a quorum that it's still leader (`VerifyLeader`), so a deposed leader can't serve stale data. `?stale=true` opts into fast local reads instead.
- **Idempotency:** every write carries an `Idempotency-Key`. The dedupe table lives inside the Raft state machine, so it's replicated too. A retry that reaches a new leader after failover is still recognized.
- **Backpressure:** each node caps in-flight writes (`-max-inflight`) and rejects excess with 429. The client backs off with exponential backoff and full jitter.
- **Durability:** the Raft log and metadata live in BoltDB, and state machine snapshots compact the log.

| File | Purpose |
|---|---|
| `main.go` | Node startup: Raft config, storage, cluster bootstrap |
| `fsm.go` | Replicated state machine: KV map + dedupe table, snapshots |
| `server.go` | HTTP API, leader redirects, admission control, lost-ack fault injection |
| `cmd/loadtest/main.go` | Retrying client + load generator + exactly-once verifier |

### The verifier

The load tester sends N `INCR` operations to a fresh counter. `INCR` is not idempotent on its own, so a duplicate shows up directly as an over-count, and a lost write shows up as an under-count. When the run ends, a linearizable read of the counter is compared against the number of acknowledged operations:

```
counter == acked              → OK
counter >  acked + gave_up    → DUPLICATES
counter <  acked              → LOST WRITES
```

## Experiments

### 1. Leader failure under load

I ran 30,000 writes from 32 concurrent clients and killed the leader mid-run.

| | Baseline (no kill) | Leader killed |
|---|---|---|
| Throughput | 2,157 ops/s | 1,982 ops/s |
| p50 | 14.5 ms | 14.0 ms |
| p99 | 22.5 ms | 22.0 ms |
| p99.9 | 36 ms | 1.40 s |
| Max | 105 ms | 2.27 s |
| Verdict | OK | OK |

Throughput dropped to near zero for about 2 seconds while n3 won the election, then returned to full speed with the old leader still dead. The p99 matched the baseline, so only the requests in flight during the election window saw the hit. That window is the p99.9 and max. I restarted the dead node afterward, and it caught up to the same applied index (60,007) as the other two.

### 2. The bug I found: one dead node slowed every client

The first time I ran the failover test, recovery took 20 seconds instead of 2:

```
t=  4s  acked=   802 (+ 402/s)  leader=127.0.0.1:8002
t=  5s  acked=  1140 (+ 338/s)  leader=?
t=  6s  acked=  1478 (+ 338/s)  leader=127.0.0.1:8002
t=  7s  acked=  1801 (+ 323/s)  leader=?        ← flapping, stuck at ~330/s
```

The cluster was healthy: n2 had become leader within a couple of seconds. The problem was in the client.

**Root cause.** All 32 workers share one cached leader address. When any worker's request failed with a connection error, it cleared that cache unconditionally. Some workers were still sending to the dead node, and each of their failures wiped out the correct leader address that the other workers had just learned. Everyone fell back to guessing, starting with node 1, which was the dead one.

On Windows, a connection to a closed localhost port doesn't fail instantly. TCP retries the SYN for about 2 seconds before giving up. So each wrong guess cost 2 seconds, and the cache kept getting cleared faster than it could be repopulated. Throughput fell to about 15% of normal for roughly 20 seconds.

**Fix.** Evict the cached leader only if it's the node that just failed (a compare-and-swap instead of a blind store), and pick a random node rather than always node 1 when no leader is known.

```go
c.leader.CompareAndSwap(node, "") // was: c.leader.Store("")
```

| Leader killed, left dead | Before fix | After fix |
|---|---|---|
| Duration (30k ops) | 33.7 s | 15.1 s |
| Throughput | 890 ops/s | 1,982 ops/s |
| Retries | 6,505 | 369 |
| p99 | 353 ms | 22 ms |
| Max latency | 11.1 s | 2.3 s |

**Takeaway.** Shared client state needs the same care as server state. A failure observed by one caller should only invalidate what that caller actually knows is wrong. I also wouldn't have caught this on Linux, where refused connections fail in microseconds. The cost of a bad retry decision depends on the platform underneath.

### 3. Lost acknowledgements and exactly-once writes

The `-lose-acks 0.05` flag makes each node apply 5% of writes but tell the client they failed. That's what a network drop after commit looks like. I ran 3,000 increments with 16 clients, with and without idempotency keys.

| | With idempotency keys | Without |
|---|---|---|
| Retries | 167 | 189 |
| Final counter | 3,000 | 3,189 |
| Duplicates | **0** | **189** |
| Verdict | OK | DUPLICATES |

Without keys, every single retry became a duplicate write: 189 retries, 189 extra increments. With keys, the state machine recognized each retry and returned the original result. Retries are only safe when the server can tell them apart from new requests, and the dedupe has to be replicated state, not a per-node cache, or failover would break it.

### 4. Admission control sweep

I sent 5,000 writes from 200 concurrent clients at three in-flight limits.

| `-max-inflight` | Throughput | p50 | p99 | 429 rejections |
|---|---|---|---|---|
| Unbounded | **9,578 ops/s** | 19 ms | **39 ms** | 0 |
| 64 | 2,406 ops/s | 14 ms | 1.28 s | 1,563 |
| 4 | 316 ops/s | 12 ms | 12.96 s | 8,373 |

I expected this to show backpressure protecting the cluster. It showed something more useful: 200 concurrent clients wasn't overload, and every limit just rejected work the cluster could have done.

- **Tight limits throttle Raft's batching.** With many proposals in flight, Raft commits them together in one disk write and one round of replication. Capping in-flight writes at 4 throws that away, which explains the 30x throughput drop.
- **The limit did its one job.** p50 stayed flat at 12–19 ms in every run. Requests the server admitted were always fast, and the excess waited on the client side instead of piling up inside the server.
- **The limit was mis-sized.** A default of 64 was a guess, and here it cost 4x throughput. An admission limit should come from measured capacity: increase concurrency until throughput stops rising and latency starts climbing, then set the limit near that knee.

Next step: find the actual saturation point (where unbounded throughput plateaus and p99 climbs) and set the default from that.

## Service level objectives

These are draft SLOs derived from the measurements above, for this cluster on this hardware.

| SLI | SLO | Measured |
|---|---|---|
| Write success (acked within the 20 s client deadline) | 99.9% | 100% in every run, including leader failure |
| Write p99 latency, steady state | < 50 ms | 22 ms (32 clients), 39 ms (200 clients) |
| Failover: time to restore write throughput after leader loss | < 5 s | ~2 s |
| Correctness: duplicate or lost acknowledged writes | 0 | 0 in every run with idempotency keys |

## Runbook

**Leader lost.**
- *Symptoms:* the client logs `leader=?`, throughput dips, and the surviving nodes log heartbeat failures to the dead peer.
- *Expected:* a new leader is elected in about 2 s with no action needed. Confirm with `curl localhost:800X/status`: there should be one `Leader` and the remaining nodes `Follower`.
- *Action:* restart the dead node with the same `-id`. It rejoins from its BoltDB log and catches up. Verify all nodes report the same `applied` index.

**Two nodes down (lost quorum).**
- *Symptoms:* all writes and linearizable reads return 503 or 421 with an empty leader hint. Stale reads still work.
- *Action:* this is by design, since a minority can't safely accept writes. Restore at least one node to regain a majority. Do not delete `data/` to force a fresh cluster, because that discards committed writes.

**Sustained 429s.**
- *Check:* whether `inflight` in `/status` sits at `max_inflight` while throughput is well below capacity.
- *Action:* if so, the limit is too tight (see Experiment 4), so raise `-max-inflight`. If the node is genuinely saturated, clients backing off is the correct behavior. Add capacity rather than removing the limit.

**Dedupe table growth.**
- The dedupe table keeps every request ID forever. It's snapshotted, so it survives restarts, but memory grows with the total number of writes. A production version would expire entries after the client's maximum retry window.

## Running it

You need Go 1.22 or newer.

```
go mod tidy
go run . -id n1        # terminal 1
go run . -id n2        # terminal 2
go run . -id n3        # terminal 3
go run ./cmd/loadtest -ops 30000 -workers 32   # terminal 4
```

| Node | Raft | HTTP |
|---|---|---|
| n1 | 7001 | 8001 |
| n2 | 7002 | 8002 |
| n3 | 7003 | 8003 |

### API

```
GET    /kv/{key}             linearizable read (leader only); ?stale=true for local read
PUT    /kv/{key}             body = value
DELETE /kv/{key}
POST   /kv/{key}/cas         {"expected": "old", "value": "new"}  → 409 on mismatch
POST   /kv/{key}/incr
GET    /status               role, leader, term, applied index, in-flight writes
```

Writes accept an `Idempotency-Key` header.

### Node flags

| Flag | Default | Purpose |
|---|---|---|
| `-id` | (required) | n1, n2 or n3 |
| `-max-inflight` | 64 | admission limit; excess writes get 429 |
| `-lose-acks` | 0 | fault injection: fraction of committed writes reported as failed |
| `-log` | warn | Raft log level |

### Load tester flags

| Flag | Default | Purpose |
|---|---|---|
| `-ops` | 5000 | total INCR operations |
| `-workers` | 16 | concurrent clients |
| `-no-idem` | false | omit idempotency keys |
| `-op-timeout` | 20s | per-operation retry deadline |

To reset all state, stop the nodes and delete the `data/` folder.

## What I'd do next

- Find the saturation point and derive `-max-inflight` from it, ideally adaptively (AIMD on observed latency).
- Add a client retry budget so a failover can't amplify into a retry storm.
- Expire dedupe entries after the maximum retry window.
- Record full operation histories and check them with Porcupine for linearizability, not just counter totals.
- Shard keys across multiple Raft groups behind a hash router.
