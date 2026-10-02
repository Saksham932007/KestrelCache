namespace KestrelCache.Raft;

/// <summary>Configuration for one node in a Raft cluster.</summary>
public sealed record RaftOptions
{
    /// <summary>This node's stable identifier. Must be unique and must not change across restarts.</summary>
    public required string NodeId { get; init; }

    /// <summary>
    /// The cluster's <i>bootstrap</i> membership, this node included.
    /// </summary>
    /// <remarks>
    /// Only used when there is nothing on disk to recover from. Once the cluster has run,
    /// membership lives in the replicated log and in snapshots, because it has to: a node that
    /// trusted its configuration file over its log would come back up with a stale view of who
    /// the voters are, and a stale view of the voters is how two majorities come to exist at
    /// once. See <see cref="RaftConfiguration"/>.
    /// </remarks>
    public required IReadOnlyList<string> Peers { get; init; }

    /// <summary>Directory holding this node's Raft log and persistent state.</summary>
    public required string DataDirectory { get; init; }

    /// <summary>
    /// How long a follower waits without hearing from a leader before standing for election.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This single number governs both availability and stability, and the two pull in opposite
    /// directions. Too short and a momentary network hiccup unseats a healthy leader, costing an
    /// election; too long and a genuine leader failure leaves the cluster unable to accept writes
    /// for that whole period.
    /// </para>
    /// <para>
    /// The Raft paper's guidance is
    /// <c>broadcastTime &lt;&lt; electionTimeout &lt;&lt; meanTimeBetweenFailures</c>. With a
    /// broadcast time of a millisecond or two on a LAN, a few hundred milliseconds satisfies the
    /// left inequality comfortably while keeping failover fast enough that clients see a brief
    /// stall rather than an outage.
    /// </para>
    /// </remarks>
    public TimeSpan ElectionTimeout { get; init; } = TimeSpan.FromMilliseconds(300);

    /// <summary>
    /// How often a leader sends heartbeats. Must be comfortably below
    /// <see cref="ElectionTimeout"/>, or followers will time out while the leader is healthy.
    /// </summary>
    public TimeSpan HeartbeatInterval { get; init; } = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// Random jitter added to each node's election timeout, as a fraction of it.
    /// </summary>
    /// <remarks>
    /// Without randomisation, every follower times out at the same instant, all become
    /// candidates, all split the vote, and none wins a majority — then they all time out together
    /// again. Randomisation is what breaks the symmetry and makes a split vote resolve rather
    /// than repeat; it is not a tuning nicety but a requirement for the election to terminate.
    /// </remarks>
    public double ElectionTimeoutJitter { get; init; } = 1.0;

    /// <summary>Most entries one AppendEntries message will carry.</summary>
    public int MaxEntriesPerAppend { get; init; } = 256;

    /// <summary>
    /// Entries the log may hold before a snapshot is taken and the prefix discarded. Zero
    /// disables automatic snapshotting.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the knob that decides how much history is kept, and it trades two costs against
    /// each other. A high threshold means a long log: slower restarts, more disk, and a
    /// follower that falls behind can usually still be caught up with ordinary replication. A low
    /// threshold means frequent snapshots: each one costs a full pass over the state machine, and
    /// a follower that misses the window needs the whole dataset transferred rather than a few
    /// entries.
    /// </para>
    /// <para>
    /// The default is deliberately generous, because the expensive failure is the second one:
    /// snapshotting too eagerly turns a cheap, incremental catch-up into a full state transfer.
    /// </para>
    /// </remarks>
    public long SnapshotThresholdEntries { get; init; } = 10_000;

    /// <summary>
    /// Log size in bytes that also triggers a snapshot, for workloads whose entries are large
    /// enough that the entry count never reaches its threshold. Zero disables it.
    /// </summary>
    public long SnapshotThresholdBytes { get; init; } = 64 * 1024 * 1024;

    /// <summary>Bytes of snapshot sent per <c>InstallSnapshot</c> message.</summary>
    /// <remarks>
    /// Chunked because a snapshot is the size of the whole dataset. One message would mean
    /// buffering all of it on both sides, and would make a single lost packet cost the entire
    /// transfer rather than one chunk of it.
    /// </remarks>
    public int SnapshotChunkBytes { get; init; } = 512 * 1024;

    /// <summary>How long to wait for a peer's reply before giving up on that round.</summary>
    public TimeSpan RpcTimeout { get; init; } = TimeSpan.FromMilliseconds(500);

    /// <summary>Seed for the election-timeout randomiser, so tests can be deterministic.</summary>
    public int? RandomSeed { get; init; }

    /// <summary>
    /// Called on every role transition and election decision.
    /// </summary>
    /// <remarks>
    /// A consensus bug is almost never visible in the final state; it is visible in the sequence
    /// of transitions that produced it. A trace hook is the difference between "two nodes think
    /// they are leader" and "n2 won term 1 at tick 14 because n3 granted it a vote it had already
    /// given to n1".
    /// </remarks>
    public Action<string>? Trace { get; init; }

    /// <summary>
    /// The bootstrap configuration, or <see cref="RaftConfiguration.Empty"/> for a server
    /// joining an existing cluster.
    /// </summary>
    public RaftConfiguration BootstrapConfiguration =>
        Peers.Count == 0 ? RaftConfiguration.Empty : RaftConfiguration.Of(Peers);

    internal void Validate()
    {
        if (string.IsNullOrWhiteSpace(NodeId))
            throw new ArgumentException("NodeId must be set.", nameof(NodeId));
        // An empty set is legal and meaningful: it marks a server joining an existing cluster,
        // which starts with no configuration and learns the membership from the leader. A
        // non-empty set must contain this node, since a server cannot bootstrap a cluster it is
        // not a member of.
        if (Peers.Count > 0 && !Peers.Contains(NodeId))
            throw new ArgumentException($"Peers must contain NodeId '{NodeId}'.", nameof(Peers));
        if (Peers.Distinct().Count() != Peers.Count)
            throw new ArgumentException("Peers must not contain duplicates.", nameof(Peers));
        if (HeartbeatInterval >= ElectionTimeout)
        {
            throw new ArgumentException(
                $"HeartbeatInterval ({HeartbeatInterval.TotalMilliseconds} ms) must be well below "
                    + $"ElectionTimeout ({ElectionTimeout.TotalMilliseconds} ms), or followers "
                    + "will unseat a healthy leader.",
                nameof(HeartbeatInterval));
        }
        if (ElectionTimeoutJitter < 0)
            throw new ArgumentOutOfRangeException(nameof(ElectionTimeoutJitter));
        if (SnapshotThresholdEntries < 0)
            throw new ArgumentOutOfRangeException(nameof(SnapshotThresholdEntries));
        if (SnapshotThresholdBytes < 0)
            throw new ArgumentOutOfRangeException(nameof(SnapshotThresholdBytes));
        if (SnapshotChunkBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(SnapshotChunkBytes));
    }
}
