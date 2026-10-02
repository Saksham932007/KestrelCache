namespace KestrelCache.Raft;

/// <summary>Configuration for one node in a Raft cluster.</summary>
public sealed record RaftOptions
{
    /// <summary>This node's stable identifier. Must be unique and must not change across restarts.</summary>
    public required string NodeId { get; init; }

    /// <summary>Every node in the cluster, this one included.</summary>
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

    /// <summary>Peers other than this node.</summary>
    public IEnumerable<string> OtherPeers => Peers.Where(peer => peer != NodeId);

    /// <summary>Votes needed to win an election or commit an entry.</summary>
    public int QuorumSize => (Peers.Count / 2) + 1;

    internal void Validate()
    {
        if (string.IsNullOrWhiteSpace(NodeId))
            throw new ArgumentException("NodeId must be set.", nameof(NodeId));
        if (Peers.Count == 0)
            throw new ArgumentException("Peers must include at least this node.", nameof(Peers));
        if (!Peers.Contains(NodeId))
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
    }
}
