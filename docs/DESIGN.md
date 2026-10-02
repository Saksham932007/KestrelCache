# KestrelCache: design notes

This document is about *why*. The code explains what it does; this explains the decisions
behind it, the trade-offs each one makes, what was measured, and what I would do differently.

Where a number appears it was measured on the machine described under
[Measurement conditions](#measurement-conditions), not estimated.

---

## 1. The problem, and why there are two engines

A key-value store has to answer three questions: where do writes go, how are reads found, and
how is space reclaimed. Almost every design decision below follows from one observation:
**sequential I/O is dramatically cheaper than random I/O**, on spinning disks for obvious
mechanical reasons and on SSDs because of erase-block granularity and write amplification in the
flash translation layer.

Both engines here are log-structured — they only ever append — and they differ in how they index
what they have written. Keeping both, behind one interface, is deliberate: it turns every claimed
trade-off into something measurable rather than asserted.

### Bitcask: an append-only log with a hash index in RAM

Every write appends to one file. Every live key sits in an in-memory hash table pointing at a
byte offset. A read is one hash lookup and exactly one `pread`.

**What it buys.** Reads cost precisely one seek, never more — a guarantee a B-tree cannot make,
since a B-tree lookup walks a tree of depth proportional to the data size. Writes cost one
sequential append. The implementation is small enough to hold in your head.

**What it costs.** Three things, all structural:

- **Memory is proportional to the key count.** Every live key must be resident. At a hundred
  million keys this is tens of gigabytes before any value data.
- **Startup is proportional to the record count.** The index exists only in RAM, so every restart
  replays the whole log to rebuild it. Measured: 13.8 ms for 10,000 records, 104 ms for 100,000,
  585 ms for 500,000 — linear, about 1.2 µs per record. Fine at a million keys, ruinous at a
  billion.
- **The keyspace is unordered.** A hash index cannot produce sorted output without sorting
  everything first, so range scans are impossible. This is expressed in the type system:
  `BitcaskEngine` implements `IStorageEngine` but not `IScannableStorageEngine`, so the limitation
  is a compile error rather than a runtime surprise.

### LSM tree: sorted runs merged in levels

Writes go to a write-ahead log for durability and a sorted in-memory memtable for visibility.
When the memtable fills it is written out, already sorted, as an immutable SSTable. Tables
accumulate and are merged downwards through levels.

**What it buys.** Startup is constant — read a manifest, open a handful of files. Memory holds
per-table metadata rather than per-key entries. Everything is sorted at every level, so ordered
iteration is a merge of a few cursors.

**What it costs.** A key can now exist in several places at once, so a read may consult more than
one, and old versions accumulate until compaction removes them. Most of the engine is machinery
for containing those two costs: sequence numbers so the newest version is found first, Bloom
filters so absent keys cost no I/O, a block cache so hot blocks cost no I/O, and levelled
compaction so the number of places to look stays logarithmic.

### Measured head to head

Identical workloads, same device, 100,000 keys of 128 bytes:

| scenario | bitcask | lsm | winner |
| --- | ---: | ---: | --- |
| sequential put | 97,716/s | 77,111/s | bitcask, 1.3x |
| random get (hit) | 234,268/s | 317,542/s | lsm, 1.4x |
| random get (interleaved miss) | 3,786,789/s | 2,467,728/s | bitcask, 1.5x |
| open after 100,000 writes | 126.7 ms | 13.5 ms | **lsm, 9.4x** |
| disk after 40 rewrites of 2,000 keys | 1.0 MiB | 0.2 MiB | **lsm, 6.3x** |
| full ordered scan | not possible | 1,338,238 keys/s | lsm |

Every line is a design consequence, not an implementation detail. Bitcask wins writes because an
append needs no sorted structure. It wins misses because a hash index answers them with no I/O at
all, whereas the LSM engine must consult key ranges and Bloom filters. The LSM engine wins startup
because it reads a manifest instead of replaying a log, wins space because merging reclaims more
thoroughly than a whole-file rewrite, and is alone in being able to scan.

The LSM engine is the default because the two numbers it wins by a large margin — startup time and
space amplification — are the ones that get worse as a database grows, while the ones it loses are
within 1.5x and roughly constant.

---

## 2. Durability: what "durable" actually means

The original version of this project called itself durable and called `FlushAsync()`. Those two
things are not compatible, and the distinction is worth stating precisely because it is easy to get
wrong and invisible when you do.

`FileStream.Flush()` and `FlushAsync()` push bytes from the managed buffer into the operating
system's page cache. The data is then visible to other processes and survives the writing process
being killed — but it is **not on the device**. A power cut or kernel panic loses it. Reaching
stable storage requires an explicit `fsync`, which in .NET means `Flush(flushToDisk: true)`.

Measured on this machine: **about 1,170 µs per fsync** on btrfs over NVMe. Three orders of
magnitude more than the in-memory work around it, which makes this a real trade rather than a
free safety switch. Hence three policies:

| policy | cost | guarantee |
| --- | --- | --- |
| `None` | 99,843 writes/s | survives `kill -9`, not power loss |
| `Interval` | 94,605 writes/s | at most one interval's acknowledged writes lost |
| `EveryWrite` | 1,040 writes/s | an acknowledged write is durable, full stop |

`Interval` is the default because it is what real deployments run, and because the measured cost
over `None` is under 6% while the exposure is bounded.

### Batching is how you afford durability

An fsync costs milliseconds; an append costs microseconds. So the useful lever is not making fsync
cheaper but calling it less often per key. Under the strictest policy:

| batch size | per-key throughput | gain |
| --- | ---: | ---: |
| 1 | 1,015/s | — |
| 10 | 9,757/s | 9.6x |
| 100 | 85,226/s | 84x |
| 1,000 | 398,334/s | **392x** |

One batch is one append and one fsync regardless of how many keys it touches. This is also what
makes a batch atomic, since the batch becomes visible only once its log record is complete.

### Testing durability honestly

Three layers, because each one covers what the others cannot:

1. **Syscall accounting** — assert the fsync count per policy. Catches a build where every
   `Flush(true)` had quietly become `Flush()`.
2. **SIGKILL crash tests** — a child process writes and prints an acknowledgement per write; the
   parent kills it with `SIGKILL` and verifies every acknowledgement survived. Verified at 5,030
   acknowledged writes.
3. **A timing assertion** — confirms fsync has a real cost, which is the only check that can tell
   a genuine fsync from a page-cache flush.

The honest limitation: `SIGKILL` simulates a process crash, not a power cut. The page cache
outlives a killed process, so these tests prove crash consistency and prove the recovery path, but
they cannot prove fsync was issued. The crash test with `SyncPolicy.None` makes this concrete — it
reports *7,061 of 7,061* acknowledged writes surviving without a single fsync. Proving the rest
needs the kernel out of the picture too: a VM reset, or a device-mapper `flakey` target.

### One finding worth repeating

The test suite originally ran its scratch databases in `/tmp`. On Fedora — and most Linux
distributions — `/tmp` is a **tmpfs**, where `fsync` returns immediately without doing anything,
because there is no device to write back to. Every durability test was passing against a RAM disk.
The suite now defaults to a disk-backed directory and skips its timing-sensitive assertions if it
detects otherwise.

---

## 3. Concurrency

### Reads take no locks

The original engine took its write lock on every read, because reads and writes shared one
`FileStream` cursor — and a stream has a single mutable position, so two threads reading at once
interleave their seeks and corrupt each other. The only way to make that safe is to serialise
readers, which meant "fast indexed reads" were in practice single-threaded.

All I/O is now positional (`RandomAccess`, i.e. `pread`/`pwrite`), which takes the offset as an
argument. No shared cursor, so no lock. Measured read scaling on a 2-core / 4-thread CPU:

| threads | throughput | scaling |
| --- | ---: | ---: |
| 1 | 291,588/s | 1.0x |
| 2 | 594,450/s | 2.0x |
| 4 | 903,888/s | 3.1x |
| 8 | 999,468/s | 3.4x |

Near-linear to the hardware limit, then flat — which is the correct shape, since the machine has
four hardware threads.

### Writes serialise, and group commit makes that cheap

Writes must serialise: the log is one file and the memtable takes one writer. Doing them one at a
time meant each paid for its own lock acquisition, its own `pwrite` and its own fsync, and the
measurement said so — throughput was flat at roughly 49,000 writes/s whether fsync was enabled or
disabled, and client pipelining lifted writes by 20% while lifting reads by 4.7x. Reads scaled with
concurrency and writes did not: the lock *was* the queue, and no amount of client pipelining could
widen it.

Group commit fixes the shape. A writer enqueues its batch and then competes for the lock; whoever
wins becomes the leader and commits everything queued — one lock acquisition, one write, one fsync
for the whole group — then wakes the others. Throughput went from 49,456 to 82,440 writes/s *while
moving from no fsync to interval fsync*. The busier the server, the larger the groups and the
better the amortisation, which is the opposite of how the one-at-a-time path degraded.

### Compaction does not disturb readers

Compaction replaces files wholesale, which raises the question of what happens to a read already
in flight. Both engines answer it the same way: the thing a reader works against is immutable and
reference-counted. A reader acquires the current generation (Bitcask) or version (LSM), works
entirely within it, and releases it; compaction publishes a replacement and drops its own
reference, so files are closed only once the last reader has finished.

The alternative — a reader/writer lock around the handle — would reintroduce contention on exactly
the path that is supposed to scale, and could not be held across an `await` anyway, since
`ReaderWriterLockSlim` has thread affinity.

Measured: reads during continuous compaction run at 259,047/s with a p99 of 4.8 µs, against
291,588/s and 4.4 µs idle. A 11% throughput cost and essentially no tail-latency penalty.

### The memtable skip list

A balanced tree rebalances by rotating subtrees, which moves nodes a concurrent reader may be
standing on. A skip list never moves a node: insertion only publishes a new node by redirecting
forward pointers, so a reader sees the pointer either before or after the insert and both are
consistent, complete lists.

Correctness rests on one rule — a node is fully initialised before any pointer to it is visible.
Insertion writes the new node's own pointers with plain stores (no reader can reach it yet), then
publishes it with a release store; readers traverse with acquire loads. Without that pair the CPU
or compiler could make the publishing store visible first, and a reader could follow a pointer into
a node whose key array had not been assigned.

Single-writer is a precondition, not an implementation gap: writes are already serialised behind
the write-ahead log, so supporting concurrent writers would add CAS retry loops to pay for a
concurrency the layer above cannot use.

---

## 4. The LSM engine in detail

### Sequence numbers in the key

An LSM tree never overwrites. A second write to a key is appended with a higher sequence number,
and the older entry survives until compaction drops it. So "the value of key K" is not a location
but a search for the entry with the highest sequence.

Encoding the sequence into the key, and sorting equal user keys **newest first**, makes that search
free: the first entry a scan meets for a user key is already the answer, and everything after it is
provably stale. It also makes snapshots free — a reader that ignores entries above some sequence S
sees the database exactly as it was at S, with no locks and no copying, because the versions it
needs were never destroyed.

56 bits of sequence allows 7.2x10^16 writes: at a million writes a second, about two millennia.

### Block format and prefix compression

Keys within a block arrive sorted, so consecutive keys usually share a long prefix. Each entry
stores how many bytes it shares with its predecessor plus the bytes that differ. Measured on
20,000 keys of the shape `tenant:acme/user/profile/00000001`, with block compression *off*:
840,000 bytes nominal to **335,348 bytes on disk — 40%**, from prefix compression alone.

The catch is that an entry can only be decoded by walking from a known-complete key, which would
make a block a linked list and destroy random access. Restart points solve it: every 16 entries one
key is stored in full and its offset recorded in a table at the end of the block. A seek
binary-searches that table and scans forward at most 16 entries.

Block compression (Deflate) is applied per block, not per file, because a whole-file stream would
have to be decompressed from the beginning to reach any key. A block is stored compressed only if
that saves at least an eighth, since below that the saving does not pay for decompressing on every
read. Measured on highly repetitive values: 1,085,042 bytes to **54,473 — 5%**.

### Bloom filters, and the bug they hid

A read for an absent key must be *proven* absent, which naively means consulting every level. A
Bloom filter turns almost all of those into a memory probe. At the standard 10 bits per key the
false-positive rate is about 1%, costing 1.25 bytes per key — for a million keys, 1.2 MB of RAM to
avoid roughly 99 disk seeks out of every 100 misses.

The first implementation followed LevelDB exactly: its 32-bit hash for the probe start, and a
17-bit rotation of the same value as the stride. Unit tests measured a false-positive rate within
0.1 percentage points of theory. Then the cross-engine benchmark reported **14%** against a
predicted 0.8%, because it used a different key shape: dense zero-padded integers differing only
in their last digit.

Two measurements found the cause:

- Raising the filter to 16 bits per key moved the rate only from 14.1% to 13.1%. An undersized
  filter improves when given more bits, so size was not the problem.
- Adding a murmur3 finalizer did not help either, which rules out weak avalanche.

That leaves genuine collisions, which no post-mixing can undo. The hash is affine in the key's
trailing bytes, so arithmetically-spaced keys map to arithmetically-spaced values that collapse onto
far fewer distinct hashes than chance predicts. And in a Bloom filter a hash collision is not a
near-miss: every probe coincides, so it is a *guaranteed* false positive.

This matters because dense integer keys are not exotic — auto-increment identifiers, timestamps and
sequence numbers are among the most common keys there are, and they were the ones the old hash
handled worst.

Replaced with XxHash3, taking the probe start and the stride from independent halves of one 64-bit
digest rather than deriving one from the other. After:

| key shape | before | after | theory |
| --- | ---: | ---: | ---: |
| dense numeric, 10 bits/key | 14.06% | **0.78%** | 0.84% |
| distinct prefixes, 10 bits/key | 0.58% | 0.88% | 0.84% |
| random, 10 bits/key | 1.09% | 0.84% | 0.84% |
| dense numeric, 16 bits/key | 13.05% | **0.05%** | 0.05% |

The rate now also scales with bits per key as it should — 5.67% at 6 bits, 0.83% at 10, 0.05% at
16 — which is the property a colliding hash cannot have, and which is now a test.

### Levelled compaction, and why not tiered

The two standard strategies trade the same three quantities in opposite directions:

- **Levelled** keeps each level's tables non-overlapping, so a read touches at most one table per
  level. Read and space amplification are low; write amplification is higher, because moving a
  table down means rewriting the overlapping tables beneath it.
- **Tiered** accumulates same-size tables and merges only when several have piled up. Write
  amplification is much lower, but a read may check every table in a level, and old versions
  survive longer.

Levelled, because this engine's reason for existing alongside the Bitcask one is to make reads and
scans cheap at scale. Choosing tiered would improve the number Bitcask already wins (writes) and
worsen the ones it loses (reads, space).

Level 0 is scored by **file count**, not size, because its tables come straight from memtable
flushes and therefore overlap — four tables at level 0 means four lookups per read, whatever they
weigh. Deeper levels are scored by total size against a target growing by the level multiplier,
which keeps the tree's depth logarithmic in the data volume.

The multiplier of 10 is conventional for a real reason: total write amplification is roughly the
multiplier times the number of levels, and the number of levels is `log_multiplier(dataSize)`. A
larger multiplier means fewer levels but more rewriting at each; the product is flat-bottomed
around 10.

### The tombstone rule

The subtlest rule in the engine, and the one where getting it wrong is silent. A tombstone cannot
simply be dropped when encountered: if an older value for the same key survives in a deeper level,
removing the tombstone resurrects it. A tombstone may only go when the compaction output lands in
the deepest level that could hold such a value, and when no live snapshot predates it.

A closely related rule caused a real bug here. Versions arrive newest-first during compaction, and
the first implementation dropped every version below the oldest snapshot. With versions at
sequences 100, 50 and 10 and an open snapshot at 20, the snapshot can see neither 100 nor 50 — so
version 10 is the one it reads, and discarding it for being "old" made the snapshot read return
nothing. Older versions may only go once a *surviving* version is already visible to the oldest
snapshot.

### Crash safety: the ordering that matters

SSTables on disk are meaningless alone; the manifest says which are live. Installing a new manifest
is therefore the moment a flush or compaction becomes real, and it is atomic — write to a temporary
file, fsync, rename. The ordering around it is the whole argument:

1. Write the SSTable and fsync it.
2. Install the manifest naming it, atomically.
3. Only then delete the write-ahead log the data came from.

A crash between any two steps is survivable. After step 1 the table is unreferenced and is
collected as an orphan. After step 2 the data is reachable and the log is merely redundant. The one
ordering that would lose data — deleting the log before the manifest is durable — is the one the
sequence forbids.

---

## 5. The server

### Why RESP

Speaking the Redis protocol rather than inventing one is the highest-leverage decision in the
server. `redis-cli` connects and works; every Redis client library in every language already
speaks to it; and `redis-benchmark` can be pointed at it to produce numbers directly comparable
with Redis itself. A bespoke protocol would have required writing all of that.

Measured against Redis 7 with `appendfsync everysec`, same machine, 100k ops, 50 clients, pipeline
depth 16:

| | KestrelCache | Redis 7 | ratio |
| --- | ---: | ---: | ---: |
| SET | 82,440/s | 543,478/s | 6.6x |
| GET | 662,252/s | 925,926/s | **1.4x** |
| INCR | 70,225/s | 694,444/s | 9.9x |

GET within 1.4x of a mature C implementation is a respectable result for managed code. SET and
INCR are further behind, and the honest reading is that Redis's single-threaded event loop with one
shared output buffer amortises per-operation overhead better than a task-per-connection model with
a sorted memtable underneath. The remaining gap is per-operation cost in the write path, not an
architectural ceiling — group commit already closed the serialisation half of it.

### What is deliberately missing

- **No lists, sets, hashes or sorted sets.** The engine stores opaque byte strings. Emulating
  collections means read-modify-write of a serialised blob per element, which works and is both
  slower and less honest than saying it is unsupported.
- **No `EXPIRE`.** No record in the storage format carries a timestamp. Supporting it needs a new
  record field and an expiry sweep.
- **Unsupported `SET` options are refused, not ignored.** Returning `+OK` for a key the caller
  believes will expire, and which never will, is worse than an error.
- **`HELLO 3` answers "protocol 2".** RESP3's map, set and double types are not implemented, and
  claiming version 3 while speaking version 2 breaks clients worse than declining does.

### `DBSIZE`, and admitting what you cannot measure

Redis answers `DBSIZE` in constant time from its hash table. An LSM tree cannot: it holds every
version of every key spread across a memtable and many files, and cannot know how many are
superseded without merging them. Three options, all imperfect — an estimate, an O(keys) scan, or an
error.

The default is the estimate, because tools call `DBSIZE` freely and expect an integer, and making
the default O(keys) turns a routine monitoring call into a way to stall the server. `DBSIZE EXACT`
scans. `INFO engine` reports `keys_exact:0` so a caller can tell which it got. RocksDB takes the
same line with `estimate-num-keys`.

A related fix: stale ratio was originally computed against total disk bytes, which counted the
write-ahead log as garbage — so a healthy freshly-written database reported **100% stale**. It is
now measured against the data files only, and returns nothing at all on the LSM engine, which
cannot measure live bytes without a merge. Reporting a computed-looking number from an unknown
numerator is worse than reporting none.

---

## 6. Replication

A Raft implementation, with an adapter that presents a replicated cluster as an ordinary
`IStorageEngine`. That seam is what lets the entire server above it be unchanged — the same RESP
layer, command table and metrics endpoint serve a three-node cluster, so replication is a
deployment choice rather than a second codebase.

### The three rules that carry the safety argument

1. **One leader per term.** A node votes at most once per term and persists that vote *before*
   replying, so two candidates cannot both collect a majority in the same term. This is why the
   persistent state fsyncs: a node that forgot its vote could grant a second after restarting, and
   two leaders accepting conflicting writes is the failure Raft exists to prevent.
2. **Only an up-to-date candidate can win.** A voter refuses any candidate whose log is behind its
   own. With the majority requirement, this guarantees the winner already holds every committed
   entry — so a new leader never has to recover entries from followers.
3. **A leader never overwrites its own log.** It only appends, and brings followers into line by
   finding the last index where they agree and replacing their divergent suffix. Divergence is
   always resolved in the leader's favour.

### Why the no-op entry exists

A new leader may hold entries from previous terms that are replicated on a majority but not
committed, and Raft forbids committing them by counting replicas — there is an interleaving in
which such an entry is subsequently overwritten, so counting it as committed can lose data a client
was told was durable (figure 8 of the Raft paper). The fix is for the leader to append one entry of
its own term and commit that; committing an entry implicitly commits everything before it, so the
stale prefix becomes committed safely as a side effect.

### Testing consensus

The transport is behind an interface, and that is what makes any of this testable. Raft's
interesting behaviour is entirely about coping with a network that loses, delays, reorders and
partitions messages, and none of that is reproducible over a loopback socket — a test cannot sever
a connection at a precise instant, or drop exactly the reply that would have completed an election.
With an in-process network the test controls, a split-brain scenario is three lines and runs in
milliseconds, deterministically. A separate suite runs a cluster over real sockets so the wire
format is exercised too, including a 512 KiB value that must be reassembled from several reads.

### Three safety bugs one test found

All three produced a cluster that looked perfectly healthy from outside — one leader, matching
commit indices, writes returning success — while the logs underneath had quietly diverged. All three
reported success for a write that did not happen, which is the worst failure a database can have.
None was visible to any test that checked the cluster from outside; all three were found by one
test that compares every node's log entry by entry.

1. **Two proposals assigned the same index.** The index was computed as `LastIndex + 1` under the
   state lock, which was released before the append. Two concurrent proposals both built an entry
   for the same index; the first was written, the second matched an existing index with an identical
   term, was taken for a duplicate, and was silently discarded — while its client was told the write
   had committed, because the waiter was keyed on the index and the *other* entry committed there.
   The log now assigns indices while holding its own append lock.
2. **A follower could accept entries beyond the end of its log.** `Matches` returned true for a
   missing index, because `TermAt` returns 0 for an absent entry and the leader sent 0 for an entry
   it did not have either. The follower passed the consistency check for a position it did not hold,
   accepted entries starting there, and opened a gap — after which every index was off by one while
   still carrying a plausible term. That is the one state Raft's induction argument cannot recover
   from: two logs agreeing on (index, term) while holding different entries.
3. **A waiter was keyed on index alone.** A deposed leader's uncommitted entry at index N can be
   replaced by a different entry at index N from a later term, and applying that entry satisfied the
   original client. Waiters now carry their term and are failed on a mismatch.

### What replication does not guarantee

Reads go to the local engine, which makes them fast and, on a follower, possibly stale. Making every
read linearizable needs either a round trip through the log per read, or a quorum confirmation of
leadership first (ReadIndex) — both trade read latency for a guarantee many callers do not need.

What *is* guaranteed: a read on the leader after a successful write on the leader sees that write,
because `ProposeAsync` returns only once the entry has been applied locally. Read-your-writes holds
against the leader; cluster-wide linearizability does not.

---

## 7. Testing strategy

250 tests. The ones that earned their place:

**Model-based differential testing.** A key-value store should behave exactly like a `Dictionary`
that survives restarts. Drive a long random sequence of operations into both and assert they agree
after every one. Seeds are fixed, because a test that fails only on CI and cannot be reproduced
locally is close to useless. This found two bugs nobody suspected: the original tombstone replay
off-by-one, and a compaction that stripped the wrong framing from the records it kept.

**Single-bit-flip sweeps.** Flip every bit in a database file in turn and assert the engine never
returns a value that was not written. It may refuse to open, it may lose the tail, it may be
unaffected — it must not lie.

**SIGKILL crash tests.** A real child process, killed with a signal no handler can intercept, with
every acknowledged write verified afterwards. Including batch atomicity: with batches of ten, the
recovered key count must be a multiple of ten.

**Log Matching verification.** Compare every Raft node's log entry by entry. Found all three
consensus safety bugs above.

**Measurement as a test.** The Bloom filter tests assert the measured false-positive rate tracks
theory *and* that more bits per key lowers it — the second being the assertion a colliding hash
cannot satisfy, and the one that would have caught the hash bug directly.

---

## 8. What I would do differently

Honest gaps, roughly in order of how much they would matter:

- **Linearizable reads.** ReadIndex would close the gap with a quorum round trip per read, without
  putting reads through the log. The current behaviour is correct and documented, but "reads may be
  stale on a follower" is a real limitation.
- **Cluster membership changes.** The peer set is fixed at startup. Adding or removing a node needs
  Raft's joint-consensus protocol, which is a substantial piece of work on its own.
- **Log compaction and snapshots.** The Raft log grows without bound. Production implementations
  snapshot the state machine and truncate the log behind it; here a long-running cluster's log grows
  forever.
- **The write path's per-operation cost.** Group commit fixed the serialisation; the remaining 6.6x
  gap to Redis on SET is per-operation overhead. The next step would be profiling rather than
  guessing — my suspicion is memtable allocation and the per-command `WriteBatch`.
- **A better block cache policy.** LRU is vulnerable to a large scan evicting the working set.
  Compactions here bypass the cache, which sidesteps it, but S3-FIFO or ARC would be more robust.
- **LZ4 or Zstandard instead of Deflate.** Deflate was chosen because it is in the base class
  library. Both alternatives are several times faster at comparable ratios for this access pattern;
  the format carries a per-block type byte precisely so another codec can be added without breaking
  existing files.
- **A smaller container image.** 205 MB, dominated by the .NET runtime base image. Trimming or
  native AOT would cut it substantially, but AOT conflicts with the reflection-based JSON used for
  the manifest.

---

## Measurement conditions

All figures in this document were measured on:

- **CPU** Intel Core i3-10110U, 2 cores / 4 threads, 2.10 GHz
- **Memory** 7.6 GiB
- **Storage** NVMe SSD, btrfs
- **OS** Fedora 44, Linux 6.19
- **Runtime** .NET 9.0.318, Release build, server GC

This is a laptop, and a modest one. The absolute numbers would be substantially higher on server
hardware; the *ratios* between configurations — which is what the comparisons are about — should
hold.

Benchmarks live in `benchmarks/`. `kestrel-bench latency` produces the percentile tables,
`kestrel-bench compare` the cross-engine ones. Published figures come from a dedicated run, never
from CI, because the numbers a shared runner produces are not comparable between runs.
