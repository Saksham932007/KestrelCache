# KestrelCache: replication

How the cluster works: Raft consensus, log snapshotting, and changing membership while the
cluster is running.

The storage engines underneath are covered in **[DESIGN.md](DESIGN.md)**. As there, every figure
quoted was measured rather than estimated, on the machine described under
[Measurement conditions](DESIGN.md#measurement-conditions).

**Contents**

1. [What replication adds, and where it sits](#1-what-replication-adds-and-where-it-sits)
2. [Consensus](#2-consensus)
3. [Log snapshotting](#3-log-snapshotting)
4. [Membership changes, and learners](#4-membership-changes-and-learners)
5. [Pre-vote](#5-pre-vote)
6. [Testing consensus](#6-testing-consensus)
7. [Bugs worth reading about](#7-bugs-worth-reading-about)
8. [What replication does not guarantee](#what-replication-does-not-guarantee)

---

## 1. What replication adds, and where it sits

Raft gets a group of machines to agree on an ordered sequence of commands, such that the agreed
prefix is never lost and never contradicted, even when machines crash and the network misbehaves.
Everything else here follows from that one job.

The whole of it sits behind a single seam: `ReplicatedEngine` implements `IStorageEngine`, the same
interface the Bitcask and LSM engines implement. Writes are routed through consensus, reads are
served locally, and **everything above that line is unchanged** — the same RESP server, the same
command table, the same metrics endpoint serve a three-node cluster as serve one node. Replication
is a deployment choice, not a second codebase.

```
        redis-cli / any Redis client
                    │
              RESP / TCP
                    │
        ┌───────────▼────────────┐
        │  KestrelCache.Server   │   unchanged in both deployments
        └───────────┬────────────┘
                    │  IStorageEngine
        ┌───────────▼────────────┐
        │   ReplicatedEngine     │   reads → local, writes → consensus
        └───────────┬────────────┘
                    │
        ┌───────────▼────────────┐        ┌─────────────┐
        │       RaftNode         │◄──────►│  peers      │
        │  election, replication │  TCP   │  n2, n3     │
        │  commitment, snapshots │        └─────────────┘
        └───────────┬────────────┘
                    │  committed commands, in order
        ┌───────────▼────────────┐
        │   LsmEngine (local)    │
        └────────────────────────┘
```

Keeping the seam there is worth the indirection: the same protocol tests cover both deployments,
and the storage engine has no idea it is being replicated.

The one difference a client can see is that a write to a follower fails. That cannot be hidden —
a follower genuinely cannot accept a write — so the protocol layer turns it into a redirect the
client can act on:

```
$ redis-cli -p 6381 set k v
NOTLEADER the leader is n3
```

Silently forwarding instead would turn one network hop into two and make the failure modes much
harder to reason about.

---

## 2. Consensus

### The three rules that carry the safety argument

1. **One leader per term.** A node votes at most once per term and persists that vote *before*
   replying, so two candidates cannot both collect a majority in the same term. This is why the
   persistent state fsyncs: a node that forgot its vote could grant a second after restarting, and
   two leaders accepting conflicting writes is the failure Raft exists to prevent.
2. **Only an up-to-date candidate can win.** A voter refuses any candidate whose log is behind its
   own. With the majority requirement, this guarantees the winner already holds every committed
   entry — so a new leader never has to recover entries from followers, which is what keeps the
   protocol simple enough to reason about.
3. **A leader never overwrites its own log.** It only appends, and brings followers into line by
   finding the last index where they agree and replacing their divergent suffix. Divergence is
   always resolved in the leader's favour.

### Why the no-op entry exists

A new leader may hold entries from previous terms that are replicated on a majority but not
committed, and Raft forbids committing them by counting replicas — there is an interleaving in
which such an entry is subsequently overwritten, so counting it as committed can lose data a
client was told was durable (figure 8 of the Raft paper). The fix is for the leader to append one
entry of its own term and commit that; committing an entry implicitly commits everything before
it, so the stale prefix becomes committed safely as a side effect.

### Entry kinds

Three, and the distinction matters:

| kind | goes to | takes effect |
| --- | --- | --- |
| `Command` | the state machine | when committed and applied |
| `NoOp` | nowhere | n/a; it exists only to be committed |
| `Configuration` | the consensus module | **when appended** |

The last row is the subtle one, and [§4](#4-membership-changes) explains why.

### Verified on a real cluster

Not asserted — run, over TCP, driven by the genuine `redis-cli`:

```
n3 elected leader in term 1; followers answer writes with "-NOTLEADER the leader is n3".
Writes to the leader appear on all three nodes.
kill -9 on the leader: n1 elected in term 2, all prior data intact, writes continue.
The killed node restarted, rejoined as a follower, and caught up on everything
committed while it was dead.
```

---

## 3. Log snapshotting

### The problem

Without it the Raft log grows forever. A cluster that has served a billion writes replays a
billion entries on every restart and never reclaims the space, however small the live dataset
is. The log is a *history*; what a restart actually needs is the *state* that history produced.

A snapshot is that state, plus a note saying which entries it stands in for. Once it is durable,
everything up to that index can be discarded.

Measured on a live three-node cluster, 300 keys written then `CLUSTER SNAPSHOT`:

```
before:  log [1..301], 301 entries, no snapshot
after:   log [302..301], 0 entries, snapshot at index 301, 7,058 bytes
         all 300 keys still readable
```

### What the file holds, and why

```
magic / version
lastIncludedIndex, lastIncludedTerm    which entries this replaces
configuration                          the voter set as of that index
headerCrc32                            over the header only
payloadCrc32                           over the state-machine image
payload                                opaque bytes, the state machine's business
```

Two checksums rather than one, because they answer different questions. The header checksum makes
the length fields trustworthy *before* anything is allocated from them. The payload checksum
detects a corrupt image, and is verified before restoring — a damaged snapshot is refused rather
than written over good state.

The configuration has to travel with the snapshot. Membership is established by configuration
entries in the log, and a snapshot exists precisely to let those entries be discarded; a node
restoring without it would come back up not knowing who its peers are.

### The ordering that makes it safe

Three steps, and the order is the entire crash-safety argument:

1. **Capture** the state machine, quiesced, at `lastApplied`.
2. **Install** the snapshot durably — write to a temporary file, fsync, rename.
3. **Discard** the log prefix it replaces.

A crash between any two steps is survivable. Between 1 and 2 nothing has changed. Between 2 and 3
the snapshot exists and the log is merely redundant, so the next start restores the snapshot and
replays a prefix it already covers, which is idempotent. The one ordering that would lose data —
discarding the history before the state replacing it is durable — is the one the sequence forbids.

Snapshots are taken at `lastApplied`, never at the commit index. An entry that is committed but
not yet applied is not in the state machine, so including it would produce a snapshot claiming to
cover state it does not contain.

Prefix truncation itself rewrites the log to a temporary file and renames it, rather than shifting
the surviving entries down in place. In-place would be simpler and is not crash-safe: a crash
partway through leaves a file that is neither the old log nor the new one, and replay would find
garbage where committed entries used to be.

### The gap it opens: InstallSnapshot

Truncation creates a situation ordinary replication cannot handle. A follower that was down while
the leader snapshotted needs entries the leader has already discarded, so `AppendEntries` can
never succeed for it however far back `nextIndex` is wound. The leader has to send the *state*
instead of the *history*.

Sent in chunks, because a snapshot is the size of the whole dataset: one message would mean
buffering all of it on both sides and would make a single lost packet cost the entire transfer.
The follower accumulates chunks into a side file and installs only when the final one arrives, so
an interrupted transfer leaves its existing state untouched rather than half-replaced.

On receipt the follower discards its log *entirely*, not just the prefix. A follower far enough
behind to need a snapshot may hold entries that were never committed and have since been
overwritten by a different leader; keeping them would leave it permanently divergent.

### When it happens

Automatically, past a configurable entry count or byte size, and on demand via
`CLUSTER SNAPSHOT`. The default threshold is deliberately generous — 10,000 entries — because the
expensive failure is the eager one: snapshotting too often turns a cheap incremental catch-up
into a full state transfer for any follower that misses the window.

---

## 4. Membership changes, and learners

### Why this is not a configuration file edit

The naive approach — stop the cluster, edit everyone's config, start it again — is a planned
outage. Changing configs one node at a time while the cluster runs is worse, and the reason is
worth spelling out.

Three nodes `{A,B,C}` becoming five `{A,B,C,D,E}` is enough to show it. During the window where
some nodes hold the old view and some the new, `{A,B}` is a majority of the old configuration and
`{C,D,E}` is a majority of the new one. Those sets are **disjoint**, so both can elect a leader in
the same term — and the single-leader-per-term guarantee the entire protocol rests on is gone.

### Joint consensus

Raft's answer is an intermediate configuration belonging to both. A transition from `C_old` to
`C_new` goes:

1. The leader appends a **joint** entry naming both sets. From that moment every decision needs a
   majority of `C_old` **and** a majority of `C_new`.
2. Once that entry commits — under the joint rule, so under both majorities — the leader appends
   an entry naming `C_new` alone.
3. Once *that* commits, the change is complete and any server no longer in `C_new` can be shut
   down.

The requirement for both majorities is what makes two disjoint majorities impossible: any two sets
that each satisfy it must overlap, in both configurations. That is stated directly as a test —
every subset of a five-node joint configuration is enumerated, and every pair that satisfies the
rule is asserted to intersect.

All of it funnels through one method, `RaftConfiguration.HasQuorum`. Counting votes in an election
and deciding an entry is committed both call it, so the joint rule is applied uniformly and cannot
be forgotten at one of the call sites.

### A configuration takes effect when appended

Not when committed. This is the detail most likely to be got wrong, and it is not an
optimisation: waiting for commitment would mean counting votes and replicas under a configuration
the node had already superseded, which reopens the exact window joint consensus exists to close.

### Learners: the cheap way to grow

A learner is replicated to but counted in nothing. It never votes, never appears in a quorum, and
never campaigns. That sounds like a half-member, and the point is exactly that: **because a learner
affects no majority, adding or removing one needs no joint phase at all.** A single configuration
entry is safe, because no quorum anywhere changes size.

Which solves a real problem with adding a voter directly. A three-node cluster admitting a fourth
voter immediately needs three of four rather than two of three, so until the newcomer has caught up
the cluster tolerates *fewer* failures than it did before. And a newcomer with an empty log can
take a long time to catch up — especially if a whole snapshot has to be transferred, which is
exactly the case when the leader has already truncated its log.

So the recommended sequence is two steps:

```
CLUSTER ADDLEARNER n4 host:7384     # single entry, no joint phase, quorum unchanged
CLUSTER INFO | grep learner_lag     # watch it catch up
CLUSTER PROMOTE n4                  # joint change, made when it is already current
```

Measured on a live cluster that had already snapshotted its whole log away:

```
after ADDLEARNER:   voters:n1,n2,n3   learners:n4   quorum_size:2   learner_lag_n4:0
                    n4: role:follower  snapshots_installed:1  (its state arrived as a snapshot)
after PROMOTE:      voters:n1,n2,n3,n4  learners:   quorum_size:3
```

The cost of the window is the difference between those two `quorum_size` values, and with the
two-step sequence it lasts a round trip rather than a state transfer.

A learner is also the right answer for a read replica that should never be able to lead — a
follower in a different rack or region, say, where promoting it would be worse than failing over
locally. It receives everything and participates in nothing.

Two properties are asserted directly rather than assumed:

- **Adding or removing a learner appends exactly one entry**, and the configuration is never
  joint. A joint change appends two.
- **A learner cut off from the cluster starts zero elections and its term does not move.** A
  learner that campaigned would increment terms it can never win with, disrupting the cluster for
  no possible benefit, so `IsVoter` — not `IsMember` — is the predicate that gates campaigning.

### A joining server starts with no voters

A server joining an existing cluster comes up with an **empty** configuration. It cannot campaign,
because it has no voters to count a majority against, and it waits to learn the membership from
whichever leader starts replicating to it. Starting it with the current membership instead would
let it elect itself the moment its election timer fired, before it had ever heard from the cluster.

That is why `--raft-join` exists: `--raft-peers` serves two purposes that have to be separated for
a joining server. It says who the voters are, and it says how to reach them. A joining server needs
the addresses — it will have to replicate and vote once admitted — but must start with no voters.

### A removed server has to be told

Dropping a removed server the instant the final configuration is appended leaves it believing it
is still a voter, in the joint configuration no less. It keeps timing out and campaigning, bumping
the term on every attempt and forcing the healthy cluster to react. It cannot win, but it can be
persistently disruptive: in testing, two such servers drove a settled three-node cluster from
term 1 to **term 22**.

So replication continues to a departed server until it has acknowledged the entry that removed it,
at which point it stops campaigning of its own accord. Its acknowledgements are never counted
toward a quorum, because quorum is asked of the configuration and it is no longer in it.

### Completion is the leader's standing job

The second phase can be abandoned in ways the originating call has no say over: the caller
cancels, the caller's process dies, or the leader that started the change is replaced. Any of
those used to leave the cluster joint indefinitely — not broken, but permanently requiring two
majorities, so tolerating *fewer* failures than either configuration alone, with no mechanism to
recover.

So completing a committed joint configuration is a standing responsibility of whoever is leading,
checked on every tick, rather than a step inside one method. A half-finished change always
finishes, which is what an operator would assume anyway.

A leader that removes itself steps aside once the change commits: it can no longer count itself
toward a quorum, so continuing to lead would stall every subsequent decision.

### Verified on a real cluster

Run over TCP against a live three-node cluster that had already snapshotted away its entire log:

```
$ redis-cli -p 6384 cluster info          # n4, before joining
role:follower   term:0   leader:none   voters:            ← no voters, cannot campaign

$ redis-cli -p 6381 cluster addnode n4 127.0.0.1:7384
added n4; voters are now n1,n2,n3,n4

$ redis-cli -p 6384 cluster info          # n4, after joining
role:follower   term:1   leader:n1   voters:n1,n2,n3,n4
log_first_index:302   snapshot_index:301   snapshots_installed:2

$ redis-cli -p 6384 get key:150           # a key written before it existed
value-150                                  ← arrived via the snapshot, not the log

$ redis-cli -p 6381 cluster removenode n2
removed n2; voters are now n1,n3,n4

$ kill -9 <n1>                             # kill the leader
$ redis-cli -p 6384 cluster info
role:leader   term:2   voters:n1,n3,n4     ← n4, which joined at runtime, now leads
```

The last line is the one worth noticing: the node that was added while the cluster was running,
and that only ever learned its state from a snapshot, went on to win an election under the new
membership and serve writes.

---

## 5. Pre-vote

### The disruption

Ordinary Raft has a problem that is nobody's bug and still costs availability. A node partitioned
off rejoins, times out because it has heard from no leader, increments its term and asks for votes.
Every healthy node — including a perfectly good leader — sees the higher term, steps down, and the
cluster holds an election it did not need. The rejoining node cannot win, because its log is
behind. It has still cost an election, and a flapping link makes that happen over and over.

### The straw poll

A pre-vote asks the same question without consequences: *would you vote for me at term N+1?* Three
properties make it work, and all three are easy to get wrong:

1. **Nothing is persisted and no term advances.** A pre-vote carrying a higher term must not make
   the voter step down, or the straw poll would cause exactly the disruption it exists to avoid.
2. **A recent leader is a refusal.** If the voter has heard from a leader within its own election
   timeout, it believes a leader exists and says no. This is the clause that does the work: a
   majority will all have heard from a healthy leader, so the candidate never reaches the real
   round. A leader refuses too, because it knows a leader exists — itself.
3. **The election timer is not reset.** Unlike granting a real vote, answering a straw poll must
   not delay the voter's own candidacy — otherwise a node could keep every peer quiet just by
   polling them.

Only if a majority says yes does the candidate increment its term and run a real election. Quorum
for the straw poll is asked of the configuration, so the joint rule applies to it as well — a
pre-vote must not greenlight a campaign that cannot actually be won.

### Measured both ways

The option can be turned off, which is useful for exactly one thing: demonstrating what it
prevents. The same scenario, a node partitioned off while the cluster commits 25 entries, then
rejoining:

| | with pre-vote | without |
| --- | --- | --- |
| elections the rejoining node started | **0** | 3 |
| pre-vote rounds it lost | 2 | — |
| cluster term | **1 → 1** | 1 → 5 |
| leader | **unchanged** | changed |

Three elections, four term increments and a leadership change, caused by a node that could never
have won any of them. That is the whole case for the feature.

One counter: a lost pre-vote is not a failure to be alarmed by. It is an election that did not
happen, which is why `kestrelcache_raft_pre_votes_lost_total` is worth graphing — a climbing count
means something keeps trying to campaign and cannot, which is a flapping link or a node that does
not know it was removed.

---

## 6. Testing consensus

The transport is behind an interface, and that is what makes any of this testable. Raft's
interesting behaviour is entirely about coping with a network that loses, delays, reorders and
partitions messages, and almost none of that is reproducible over a loopback socket — a test
cannot sever a connection at a precise instant, or drop exactly the reply that would have
completed an election.

With an in-process network the test controls, a split-brain scenario is three lines and runs in
milliseconds, deterministically. A separate suite runs a cluster over real sockets so the wire
format is exercised too, including a 512 KiB value that must be reassembled from several reads.

95 consensus tests, covering:

- single-node and five-node elections, the election restriction, one-vote-per-term, and that terms
  never go backwards
- replication, batch atomicity across replication, leader failure and failover
- minority partitions that must not commit, partition healing, and a lossy network
- restart persistence, divergent-log repair, and a churn test asserting applied histories never
  diverge
- **Log Matching verification** at 0, 10, 25 and 40 percent message loss on three and five nodes
- snapshot capture, truncation, recovery, chunked transfer, and refusal of a corrupt image
- joint-consensus quorum rules, adding and removing servers, a leader removing itself, membership
  surviving both a restart and a snapshot, and a change that cannot commit without both majorities
- learners: that they are replicated to without voting, that adding or removing one appends a
  single entry, that one cut off from the cluster never campaigns, that one is not counted toward
  a quorum, promotion, and learner membership surviving a restart and a snapshot
- pre-vote: that a straw poll changes nothing on the voter, that a live leader refuses one, that a
  leader refuses one for itself, that a stale log is refused, that replies are tagged so the two
  rounds cannot be mixed, and the A/B measurement above

Every test whose correctness depends on elapsed time — the consensus suites and the durability
suite — lives in one collection with parallelisation disabled, which in xUnit means it runs neither
concurrently with itself nor alongside any other collection. The rest of the suite is CPU-bound
(fuzzers, compaction, bit-flip sweeps, crash harnesses) and saturates every core; run together on a
four-thread machine, the timing tests get starved and report absurdities — 300 unsynced writes
taking 7.1 seconds when they take 5 milliseconds idle, or a background fsync loop that never ran.

Loosening each threshold until it stopped failing was the wrong instinct, and it was the first
thing I tried. The thresholds were right; the scheduling was wrong. Isolating them keeps the
assertions tight enough to mean something — the fsync-cost test in particular only has value if it
is allowed to actually measure an fsync.

A related lesson from the pre-vote A/B test, which was racy for a subtler reason. The first version
healed the partition and then waited to see whether the rejoining node disturbed anything. But a
rejoining node usually receives a heartbeat before its own timer fires, so it never campaigns at
all and the test measured nothing — passing or failing on which arrived first. The deterministic
version makes the node attempt its campaign *while still partitioned*, where the outcome is
decided by the mechanism rather than by a race.

---

## 7. Bugs worth reading about

### Three safety bugs one test found

All three produced a cluster that looked perfectly healthy from outside — one leader, matching
commit indices, writes returning success — while the logs underneath had quietly diverged. All
three reported success for a write that did not happen, which is the worst failure a database can
have. None was visible to any test that checked the cluster from outside; all three were found by
one test that compares every node's log entry by entry.

1. **Two proposals assigned the same index.** The index was computed as `LastIndex + 1` under the
   state lock, which was released before the append. Two concurrent proposals both built an entry
   for the same index; the first was written, the second matched an existing index with an
   identical term, was taken for a duplicate, and was silently discarded — while its client was
   told the write had committed, because the waiter was keyed on the index and the *other* entry
   committed there. The log now assigns indices while holding its own append lock, so the choice
   and the write are atomic.
2. **A follower could accept entries beyond the end of its log.** `Matches` returned true for a
   missing index, because `TermAt` returns 0 for an absent entry and the leader sent 0 for an entry
   it did not have either. The follower passed the consistency check for a position it did not
   hold, accepted entries starting there, and opened a gap — after which every index was off by one
   while still carrying a plausible term. That is the one state Raft's induction argument cannot
   recover from: two logs agreeing on (index, term) while holding different entries.
3. **A waiter was keyed on index alone.** A deposed leader's uncommitted entry at index N can be
   replaced by a different entry at index N from a later term, and applying that entry satisfied
   the original client. Waiters now carry their term and are failed, not completed, on a mismatch.

### A diagnostic that destroyed what it measured

The test written to find those bugs originally read each node's log by reopening the file. Replay
truncates a torn tail — so inspecting a live node's log from outside *shortened* it. The
measurement reported followers holding two entries out of twenty-six, having caused the very
divergence it was looking for.

The log is now opened with `FileShare.None`, so a second owner fails at open rather than corrupting
data later, and `RaftNode.InspectLog` is the way in. Two latent bugs surfaced alongside it: the log
used bare `RandomAccess.Read`, where a short read would have been taken for end-of-log and
truncated a good log.

### A file swap with no lock

Prefix truncation closes and reopens the log's `FileStream`, and for an instant there is no usable
handle at all. The swap happened outside the lock readers take, so a concurrent read could observe
a disposed `SafeFileHandle` — which surfaced as an intermittent `ObjectDisposedException` in
roughly one full test run in three. The swap and the bookkeeping that follows it now happen under
the readers' lock; everything in that section is synchronous, so holding it is safe.

### Non-idempotent teardown, in four places

`DisposeAsync` on `RaftRpcServer`, `ClusterHost`, `RespServer` and `MetricsEndpoint` all cancelled
a `CancellationTokenSource` and then disposed it, so a second call threw. Double disposal is
routine — nested `await using` blocks, a teardown that also disposes its children — and throwing
on teardown turns an orderly shutdown into a crash. The same bug as the `CloseAsync`-then-dispose
one fixed in the storage engine, in four more places.

---

## What replication does not guarantee

Reads go to the local engine, which makes them fast and, on a follower, possibly stale. Making
every read linearizable needs either a round trip through the log per read, or a quorum
confirmation of leadership first (ReadIndex) — both trade read latency for a guarantee many
callers do not need.

What *is* guaranteed: a read on the leader after a successful write on the leader sees that write,
because `ProposeAsync` returns only once the entry has been applied locally. Read-your-writes
holds against the leader; cluster-wide linearizability does not.

Also still missing:

- **A pre-vote phase.** A partitioned node that rejoins can force a term increment and a needless
  election. The Raft dissertation's pre-vote extension avoids it by having a candidate check it
  could win before incrementing anything.
- **Learner (non-voting) members.** A joining server becomes a voter immediately, so it counts
  toward quorums while it is still catching up — which briefly makes the cluster less available
  than it was. Real implementations add a server as a non-voting learner first and promote it once
  it has caught up.
- **Snapshotting on the Bitcask engine.** Capturing state requires enumerating the keyspace, which
  a hash index cannot do without sorting it first. A replicated node therefore has to use the LSM
  engine; a Bitcask-backed one could replicate but never truncate its log.
