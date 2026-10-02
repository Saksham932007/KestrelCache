# Changelog

Notable changes, newest first. Dates are when the work landed on `main`.

The format is loose on purpose: this is a project whose interesting history is *which bugs were
found and how*, not a package whose consumers need to diff API surfaces. Where a change fixed
something, the entry says what was broken and what revealed it.

---

## Fix a red CI pipeline — 2026-10-03

Three bugs that CI had been reporting since it was added, and which nobody had read closely.

**The container image never built.** The Dockerfile copies each `.csproj` individually so that
`restore` caches independently of source changes, and `KestrelCache.Raft.csproj` was never added
to that list when the Raft project was created. The solution names it, so `dotnet restore` failed
with MSB3202 and took three jobs down with it — the image build, the compose stack, and the whole
replicated-cluster job, which is why none of the cluster assertions had ever actually run.
`Directory.Packages.props` and `global.json` were missing from the same layer and would have
failed restore immediately afterwards: central package management is what supplies the versions
the project files deliberately omit, and an SDK pin that is copied only after restore has already
chosen a toolchain is not a pin.

**Every Bitcask compaction test failed on Windows** — 15 tests, one cause. Compaction installs the
rewritten log by renaming over the live path while both files are still open. POSIX allows that
unconditionally, which is the whole basis of the design: readers holding a descriptor finish
against the old inode and never block. Windows refuses to replace a file unless every open handle
to it granted delete sharing, so the install failed with "being used by another process" on every
run while passing on Linux. The log is now opened `FileShare.Read | FileShare.Delete`, which makes
Windows behave the way the compaction path already assumed. The alternative — closing the handle
before the rename and reopening after, which is what the Raft log does — is simpler but stalls
readers, and not stalling readers is what the positional-I/O design is for.

**The container job checked for a metric that cannot exist yet.** `kestrelcache_engine_sstables`
is emitted one series per level, so a tree that has never flushed exports a bare header and no
series at all. The job wrote five keys and then required the metric to be present. It now issues
`COMPACT` first, which flushes the memtable, makes the gauge real, and incidentally gives a
supported command its first coverage.

**The clustered compose stack had never enabled clustering.** Once the image built, the cluster job
ran for the first time and reported three nodes all answering `cluster_enabled:0`. Both compose
files pass every flag as `--name=value`; the argument parser only understood `--name value`, so
`--raft-id=n1` matched no case and the switch's `_ => options` default discarded it. Three
unrelated single-node servers came up looking perfectly healthy. The single-node stack had the
same bug and passed anyway, because every value it passes happens to equal the default.

The parser now accepts both spellings, and — more importantly — **refuses an unrecognised flag
instead of ignoring it**, distinguishing a misspelling from a missing value. A silently dropped
option is indistinguishable from one that was honoured until something downstream behaves
inexplicably, which is exactly how this survived three commits. The server already refuses
unsupported commands rather than ignoring them; its own arguments now get the same treatment, and
a usage mistake prints one line and exits 2 rather than a stack trace, since in a container that
line is the only output anyone will see. CI asserts both refusals.

Found by actually reading the run rather than the badge: 305 tests pass on Linux and macOS, and the
container image, the metrics endpoint, the `redis-cli` exchange, the bad-flag refusals and a real
three-node cluster formed from the compose arguments were all reproduced locally under podman
before pushing.

## Learner members and pre-vote — 2026-10-03

Closes the two Raft gaps the previous release documented, and the two concurrency bugs the work
uncovered on the way.

**Learners.** A learner is replicated to and counted in nothing: no quorum, no votes, no
campaigning. That last property matters — `IsVoter`, not `IsMember`, gates candidacy, because a
learner that campaigned would increment terms it can never win with. And because a learner affects
no majority, adding or removing one needs **no joint consensus phase at all**; a single
configuration entry is safe.

Which makes the two-step admission the cheap one. `CLUSTER ADDNODE` was always the wrong default:
the newcomer counts toward quorums from the moment the entry is appended, so a three-node cluster
admitting a fourth voter needs three of four before the fourth can answer anything — it tolerates
*fewer* failures during the change than before it, for however long catch-up takes. With an empty
log and a leader that has already snapshotted, that is a full state transfer. `ADDLEARNER` then
`PROMOTE` narrows the window to a round trip.

Verified on a live three-node cluster that had already discarded its log: n4 admitted as a learner
(`voters:n1,n2,n3  learners:n4  quorum_size:2  learner_lag_n4:0`, its state arriving as a
snapshot), promoted (`quorum_size:3`), then **n4 won term 2 after the leader was killed** and held
it when the old leader came back.

**Pre-vote.** A straw poll before any term is incremented: *would you vote for me?* A voter
refuses if it has heard from a leader within its own election timeout, and a leader refuses because
it knows of one — itself. Nothing is persisted and the voter's election timer is **not** reset,
since a poll that quieted its own voters would become the disruption it exists to prevent.

The A/B measurement, one node partitioned off while the cluster commits 25 entries, then rejoining:

| | with pre-vote | without |
| --- | --- | --- |
| elections the rejoining node started | **0** | 3 |
| cluster term | **1 → 1** | 1 → 5 |
| leader | **unchanged** | changed |

Three elections and a leadership change, caused by a node that could never have won any of them.

New: `CLUSTER ADDLEARNER`, `CLUSTER PROMOTE`, learner fields and `quorum_size` on `CLUSTER INFO`,
a `PreVote` option (on by default, and switchable mainly so the table above can be produced),
pre-vote and learner metrics, four more alert rules, and four more Grafana panels. The
configuration codec is at format version 2 and still reads version 1, so an existing cluster's
persisted state loads unchanged.

Two metrics bugs found while building the panels, both of which made an existing dashboard quietly
useless rather than visibly broken:

- No consensus metric carried a `node` label, although the dashboard legends and two alert
  annotations were already written against `{{ node }}`. Every Raft legend and both annotations
  rendered blank. All consensus series are now labelled with the node id — which is the right
  identifier rather than the scrape address, because the address a node is scraped on has nothing
  to do with the id it votes under.
- `kestrelcache_raft_peer_match_index` did not distinguish a learner from a voter, so the
  follower-lag alert averaged two things that mean different things: a voter lagging slows every
  commit, while a learner lagging only delays its promotion. It now carries `role`, and the alert
  is split into `VoterFallingBehind` and a `LearnerNotCatchingUp` that fires only when the lag is
  large *and* not shrinking.

Fixed, both the same shape — a file handle closed while another path still held it:

- `HandleInstallSnapshotAsync` accumulated incoming chunks into a field touched entirely outside
  any lock. A `FileStream` is not thread-safe, and a leader can have two `InstallSnapshot` RPCs in
  flight at once — a retry from offset zero overlapping the chunk it assumed was lost — so two
  writers raced one handle and `Complete()` flushed a stream the other call had already disposed.
  Surfaced as an `ObjectDisposedException` in two full test runs out of eight.
- `LsmEngine.InstallVersionAsync` retired the memtable *before* publishing the version that
  replaced it, with a manifest fsync in between. Readers hold no lock, so a read landing in that
  window found the data in neither place — a genuine read-availability hole, caught by a
  background-maintenance test rather than by reading the code.

And one in CI itself, which is the kind of bug that makes a green check mean nothing: the cluster
job captured the leader's port once at startup and never refreshed it after deliberately killing
that leader, so every later step — snapshotting, the runtime join, the removal — addressed a dead
container. The failover step now publishes its replacement, and the removal step picks a victim
that is neither the leader nor the node still down. A new step also asserts pre-vote end to end by
restarting the killed node and checking that the cluster's term does not move.

Also: every test whose correctness depends on elapsed time now runs serialised in one collection.
They were being starved of CPU by the fuzzers and compaction sweeps and reporting absurdities —
300 unsynced writes taking 7.1 s when they take 5 ms idle. Loosening the thresholds was the first
instinct and the wrong one; the thresholds were right and the scheduling was wrong.

Tests: 278 to 305.

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
