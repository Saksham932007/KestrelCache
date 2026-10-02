# KestrelCache

A persistent key-value store written from scratch in C#, with two storage engines, a Redis-protocol
server, and Raft replication.

```bash
docker compose -f deploy/docker-compose.yml up -d --build

redis-cli -p 6380 set hello world
redis-cli -p 6380 get hello
redis-benchmark -p 6380 -t set,get -n 100000 -P 16
open http://localhost:3000          # Grafana, dashboard pre-provisioned
```

It speaks the Redis wire protocol, so `redis-cli` and `redis-benchmark` work against it unmodified.

---

## What is in here

| | |
| --- | --- |
| **Two storage engines** | A Bitcask-style append-only log with an in-memory hash index, and a log-structured merge tree with a write-ahead log, memtable, levelled SSTables, Bloom filters and a block cache. Both behind one interface, so their trade-offs are measured rather than asserted. |
| **Real durability** | Three fsync policies with the cost of each measured, group commit to amortise it, and crash tests that `SIGKILL` a real process and verify every acknowledged write came back. |
| **A server** | RESP over `System.IO.Pipelines`, verified against the genuine `redis-cli` and `redis-benchmark`. |
| **Replication** | Raft — leader election, log replication, persistent state — with a three-node cluster that survives `kill -9` on its leader. |
| **Observability** | Prometheus metrics with a latency histogram and engine internals, a provisioned Grafana dashboard, and six alert rules. |
| **250 tests** | Including a model-based fuzzer, single-bit-flip corruption sweeps, SIGKILL crash consistency, and a Raft Log Matching verifier. |

**[docs/DESIGN.md](docs/DESIGN.md) is the interesting document** — why each decision was made, what
was measured, which bugs the measurements found, and what I would do differently.

---

## Numbers

Measured on a 2-core i3-10110U laptop with NVMe/btrfs. Absolute figures would be much higher on
server hardware; the ratios are the point. See [Measurement conditions](docs/DESIGN.md#measurement-conditions).

### Against Redis 7

`redis-benchmark`, 100k ops, 50 clients, pipeline depth 16, Redis configured with
`appendfsync everysec`:

| | KestrelCache | Redis 7 | ratio |
| --- | ---: | ---: | ---: |
| GET | 662,252/s | 925,926/s | **1.4x** |
| SET | 82,440/s | 543,478/s | 6.6x |
| INCR | 70,225/s | 694,444/s | 9.9x |

### The two engines, head to head

Identical workloads, 100k keys of 128 bytes:

| scenario | bitcask | lsm | winner |
| --- | ---: | ---: | --- |
| sequential put | 97,716/s | 77,111/s | bitcask, 1.3x |
| random get (hit) | 234,268/s | 317,542/s | lsm, 1.4x |
| random get (miss) | 3,786,789/s | 2,467,728/s | bitcask, 1.5x |
| open after 100k writes | 126.7 ms | 13.5 ms | **lsm, 9.4x** |
| disk after 40 rewrites of 2k keys | 1.0 MiB | 0.2 MiB | **lsm, 6.3x** |
| full ordered scan | not possible | 1,338,238 keys/s | lsm |

Each line is a design consequence. Bitcask wins writes because an append needs no sorted structure,
and misses because a hash index answers them with no I/O. The LSM engine wins startup because it
reads a manifest instead of replaying a log, wins space because merging reclaims more thoroughly,
and is alone in being able to scan — a hash index cannot produce sorted output.

### Tail latency, not just throughput

p99.9 is reported because a mean hides the request that waited behind an fsync or a compaction:

| scenario | ops/sec | p50 | p99 | p99.9 |
| --- | ---: | ---: | ---: | ---: |
| put, no fsync | 99,843 | 8.3 µs | 15.5 µs | 36.2 µs |
| put, fsync every write | 1,040 | 917 µs | 1,435 µs | 7,202 µs |
| put, fsync every write, batches of 1000 | 398,334 | 1.5 µs | 11.2 µs | 11.2 µs |
| get, random | 291,588 | 2.7 µs | 4.4 µs | 14.2 µs |
| get, during compaction | 259,047 | 3.2 µs | 4.8 µs | 12.6 µs |
| get, miss | 2,877,714 | 0.2 µs | 0.5 µs | 0.7 µs |

An fsync costs about **1,170 µs** here, which is why batching matters: one batch is one append and
one fsync regardless of how many keys it touches. Across batch sizes 1 to 1000 under the strictest
durability policy, per-key throughput goes from 1,015/s to 398,334/s — **392x**.

Reads take no locks at all, and scale to the hardware limit:

| threads | 1 | 2 | 4 | 8 |
| --- | ---: | ---: | ---: | ---: |
| reads/sec | 291,588 | 594,450 | 903,888 | 999,468 |

---

## Architecture

```
                        redis-cli, redis-benchmark, any Redis client
                                          │
                                     RESP / TCP
                                          │
                        ┌─────────────────▼──────────────────┐
                        │   KestrelCache.Server              │
                        │   System.IO.Pipelines, commands,   │
                        │   Prometheus metrics endpoint       │
                        └─────────────────┬──────────────────┘
                                          │  IStorageEngine
                     ┌────────────────────┼────────────────────┐
                     │                    │                    │
          ┌──────────▼─────────┐          │       ┌────────────▼────────────┐
          │  BitcaskEngine     │          │       │  ReplicatedEngine       │
          │  append-only log   │          │       │  (Raft)                 │
          │  + RAM hash index  │          │       │  election, replication, │
          └────────────────────┘          │       │  commitment  ──► peers  │
                                          │       └────────────┬────────────┘
                              ┌───────────▼──────────┐         │
                              │  LsmEngine           │◄────────┘
                              │                      │   applies committed
                              │  WAL ─► memtable     │   entries locally
                              │          │ flush      │
                              │          ▼            │
                              │  L0  [sst][sst][sst]  │  overlapping
                              │          │ compact    │
                              │  L1  [ sst ][ sst ]   │  disjoint, 10x
                              │          │            │
                              │  L2  [   sst     ]... │  disjoint, 100x
                              │                      │
                              │  bloom filters,       │
                              │  block cache,         │
                              │  MVCC snapshots       │
                              └──────────────────────┘
```

Replication sits at the `IStorageEngine` seam, so the entire server above it is unchanged whether
it serves one node or three.

---

## Layout

```
src/KestrelCache/            the engines: Bitcask, LSM, shared abstractions
src/KestrelCache.Server/     RESP server, command table, metrics endpoint
src/KestrelCache.Raft/       consensus: log, state, transport, node
src/KestrelCache.Cli/        a CLI, and the crash-test harness
tests/KestrelCache.Tests/    250 tests
benchmarks/                  latency percentiles and cross-engine comparison
deploy/                      compose stacks, Prometheus config, Grafana dashboard
docs/DESIGN.md               why, what was measured, and what is still missing
```

---

## Running it

### Without Docker

```bash
dotnet run --project src/KestrelCache.Server -- --port 6380 --data ./kc-data
dotnet run --project src/KestrelCache.Cli -- demo
dotnet test
```

### Benchmarks

```bash
dotnet build -c Release
dotnet benchmarks/KestrelCache.Benchmarks/bin/Release/net9.0/kestrel-bench.dll latency --ops 100000
dotnet benchmarks/KestrelCache.Benchmarks/bin/Release/net9.0/kestrel-bench.dll compare --ops 100000
```

The benchmark warns if its scratch directory is memory-backed, because `fsync` is a no-op on tmpfs
and every durability figure would be meaningless. (This is not hypothetical — the test suite
originally ran in `/tmp`, so every durability test was passing against a RAM disk.)

### A three-node cluster

```bash
docker compose -f deploy/docker-compose.cluster.yml up -d --build

for p in 6381 6382 6383; do redis-cli -p $p cluster info | grep -E 'role|leader'; done

redis-cli -p 6381 set replicated yes      # a follower answers -NOTLEADER and names the leader
redis-cli -p 6382 get replicated          # followers serve reads

docker compose -f deploy/docker-compose.cluster.yml kill kc1
redis-cli -p 6382 cluster info            # a new leader within an election timeout
```

### Using it as a library

```csharp
await using var db = await KestrelDb.OpenAsync("./my-data", EngineKind.Lsm);

await db.PutAsync("user:1", """{"name":"Alice"}""");
Console.WriteLine(await db.GetAsync("user:1"));

// One append, one fsync, applied atomically
await db.WriteAsync(new WriteBatch()
    .Put("order:1", """{"total":42.00}""")
    .Put("order:2", """{"total":17.50}""")
    .Delete("user:1"));

// Ordered iteration (LSM only — a hash index cannot do this)
await foreach (var (key, value) in db.ScanPrefixAsync("order:"))
{
    Console.WriteLine($"{key} = {value}");
}
```

---

## Some bugs worth reading about

The measurements and tests here found real bugs, and the ones that were hardest to find are
described in full in [docs/DESIGN.md](docs/DESIGN.md). Three that stand out:

**A Bloom filter at 14% false positives against a predicted 0.8%.** Unit tests measured 0.74% and
were right — they used keys with distinct prefixes, while the benchmark used dense integers. Two
measurements isolated it: more bits per key barely helped (ruling out size), and a stronger
finalizer did not help either (ruling out avalanche), leaving genuine hash collisions, which no
post-mixing can undo. The LevelDB-style hash is affine in the key's trailing bytes, so
auto-increment keys — among the most common there are — collapsed onto far too few distinct values.
Now 0.78%.
[Details](docs/DESIGN.md#bloom-filters-and-the-bug-they-hid)

**Three Raft safety bugs, each reporting success for a write that never happened.** Every one left
a cluster that looked perfectly healthy from outside — one leader, matching commit indices, writes
returning `OK` — while the logs underneath had diverged. All three were found by one test that
compares every node's log entry by entry, and none was visible to any test that checked the cluster
from outside.
[Details](docs/DESIGN.md#three-safety-bugs-one-test-found)

**A write path that could not be made faster by pipelining.** Throughput was flat at ~49,000
writes/s whether fsync was on or off, while reads scaled 4.7x with pipelining. Reads scaling and
writes not is the signature of a serialised path rather than a slow device: every writer took the
same lock and issued its own syscall, so the lock *was* the queue. Group commit took SET from
49,456 to 82,440/s while simultaneously moving from no fsync to interval fsync.
[Details](docs/DESIGN.md#writes-serialise-and-group-commit-makes-that-cheap)

---

## Status and limitations

Working and tested: both engines, durability policies, compaction, MVCC snapshots, ordered scans,
the RESP server, Prometheus metrics, and Raft replication with failover and catch-up.

Known gaps, stated plainly:

- **Reads on a follower may be stale.** Read-your-writes holds against the leader; cluster-wide
  linearizability would need ReadIndex.
- **Cluster membership is fixed at startup.** Adding or removing a node needs Raft's
  joint-consensus protocol.
- **The Raft log grows without bound.** No snapshotting or log truncation yet.
- **No collection types and no `EXPIRE`.** The engine stores opaque bytes and no record carries a
  timestamp. Unsupported commands and options are refused rather than silently ignored.
- **This is a learning project, not production software.** It has not been run in anger, fuzzed by
  anyone but me, or audited.

---

## Author

**Saksham Kapoor** — [@Saksham932007](https://github.com/Saksham932007)

Licensed under the MIT License.
