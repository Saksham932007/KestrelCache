# Changelog

Notable changes, newest first. Dates are when the work landed on `main`.

The format is loose on purpose: this is a project whose interesting history is *which bugs were
found and how*, not a package whose consumers need to diff API surfaces. Where a change fixed
something, the entry says what was broken and what revealed it.

---

## Raft log snapshotting and membership changes — 2026-10-02

Closes the two largest gaps the previous release documented.

**Log snapshotting.** The Raft log no longer grows without bound. A snapshot captures the state
machine, is installed atomically, and the log prefix it replaces is discarded — in that order,
which is the whole crash-safety argument. Measured on a live cluster: 301 entries to 0, a
7,058-byte snapshot, all data intact. A follower too far behind to be caught up from the log is
sent the state instead, chunked, via a new `InstallSnapshot` RPC.

**Membership changes.** Servers can be added and removed while the cluster runs, through joint
consensus. A node added at runtime — whose entire state arrived as a snapshot because the leader
had already discarded its log — went on to win an election and serve writes under the new
membership.

New: `CLUSTER ADDNODE`, `CLUSTER REMOVENODE`, `CLUSTER PEERS`, `CLUSTER SNAPSHOT`, a `--raft-join`
flag for servers joining rather than bootstrapping, and Raft metrics on the Prometheus endpoint.

Fixed, each found by a test rather than by reading:

- A removed server was dropped the instant it was removed, so it never learned it was out and
  campaigned forever — driving a settled three-node cluster from term 1 to **term 22**. The leader
  now replicates to a departed server until it acknowledges the entry that removed it.
- A leader whose membership change was abandoned (cancelled caller, dead caller, replaced leader)
  left the cluster permanently joint — needing two majorities forever, so tolerating fewer
  failures than either configuration alone. Completing a committed joint configuration is now a
  standing responsibility of whoever is leading.
- Prefix truncation swapped the log's `FileStream` outside the readers' lock, so a concurrent read
  could observe a disposed handle. Surfaced as an intermittent `ObjectDisposedException` in about
  one full test run in three.

Tests: 250 to 278.

## Raft replication — 2026-10-02

Leader election, log replication, persistent state, and commitment, behind an adapter that presents
a replicated cluster as an ordinary `IStorageEngine` — so the whole server above it is unchanged.

Verified as a real three-node cluster over TCP driven by `redis-cli`: leader elected, `kill -9` on
it, replacement elected, all prior data intact, killed node restarted and caught up.

Fixed three safety bugs, each reporting success for a write that did not happen, all three found by
one test comparing every node's log entry by entry: two proposals assigned the same log index; a
follower accepting entries beyond the end of its own log; a proposal waiter keyed on index alone.

## A Redis-protocol server, group commit, metrics and containers — 2026-10-02

RESP over `System.IO.Pipelines`, verified against the genuine `redis-cli` and `redis-benchmark`.
GET within 1.4x of Redis 7; SET 6.6x.

Group commit, which the benchmark identified rather than guessed at: writes were flat at ~49,000/s
whether fsync was on or off while reads scaled 4.7x with pipelining — the signature of a serialised
path, not a slow device. SET went from 49,456 to 82,440/s *while* moving from no fsync to interval
fsync.

Prometheus metrics with a latency histogram, a 22-panel provisioned Grafana dashboard, six alert
rules, a non-root container image, and compose stacks for single-node and clustered deployments.

Fixed: both listeners bound IPv4 only, so `localhost` (which resolves to `::1` first) failed; the
shutdown path cancelled an already-disposed token source, turning a clean stop into a crash; stale
ratio counted the write-ahead log as garbage, so a healthy database reported 100% stale.

## A log-structured merge-tree engine — 2026-10-02

A second storage engine behind the same interface, so the two designs can be benchmarked head to
head: write-ahead log, skip-list memtable with lock-free readers, prefix-compressed SSTables with
Bloom filters and a block cache, levelled compaction, a JSON manifest installed atomically, MVCC
snapshots, and ordered range scans.

Measured: Bloom filter false-positive rates within 0.1 points of theory; prefix compression alone
cuts structured keys to 40% of nominal size.

Fixed three bugs found while building it: compaction discarded the version an older snapshot still
needed; the level-0 compaction trigger was off by one; compaction could roll to a new output file
mid-key, splitting one key's versions across two tables at the same level.

## Correctness, tests and benchmarks — 2026-10-02

Restructured a single 171-line file into a solution, and fixed six correctness bugs in the original
engine — the headline one being that tombstone replay advanced its offset cursor by the value
length, and a tombstone encoded that length as `-1`, so every key written after a delete became
unreadable after a restart.

Added a model-based fuzzer that differentially tests the engine against a `Dictionary`,
single-bit-flip corruption sweeps, fsync accounting, and crash-consistency tests that `SIGKILL` a
real child process and verify every acknowledged write came back.

Two findings worth recording. The fuzzer caught a bug introduced in that very change: compaction
copied records verbatim, preserving batch-continuation flags while dropping their terminators, so a
clean reopen silently discarded live data. And the test scratch directory had to be moved off
`/tmp`, which is a tmpfs where `fsync` is a no-op — every durability test had been passing without
testing anything.
