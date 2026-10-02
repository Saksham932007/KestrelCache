namespace KestrelCache.Raft;

/// <summary>
/// The set of servers that may vote, and so the set whose majorities decide elections and
/// commitment.
/// </summary>
/// <remarks>
/// <para>
/// Changing cluster membership looks trivial and is not. The naive approach — stop the cluster,
/// edit everyone's config, start it again — is a planned outage. Changing configs one node at a
/// time while the cluster runs is worse: there is an interval during which two different nodes
/// hold two different ideas of who the voters are, and two disjoint majorities can exist
/// simultaneously. Three nodes {A,B,C} becoming five {A,B,C,D,E} is enough to show it: {A,B} is
/// a majority of the old config and {C,D,E} is a majority of the new one, so both can elect a
/// leader in the same term, and the single-leader-per-term guarantee the whole protocol rests on
/// is gone.
/// </para>
/// <para><b>Joint consensus</b></para>
/// <para>
/// Raft's answer is to pass through an intermediate configuration that belongs to both. A
/// transition from <c>C_old</c> to <c>C_new</c> goes:
/// </para>
/// <list type="number">
/// <item>
/// The leader appends a <i>joint</i> entry naming both sets. From the moment it is appended, every
/// decision needs a majority of <c>C_old</c> <b>and</b> a majority of <c>C_new</c>.
/// </item>
/// <item>
/// Once that entry commits — under the joint rule, so under both majorities — the leader appends
/// an entry naming <c>C_new</c> alone.
/// </item>
/// <item>
/// Once <i>that</i> commits, the change is complete and any server no longer in <c>C_new</c> can
/// be shut down.
/// </item>
/// </list>
/// <para>
/// The requirement for both majorities during the joint phase is what makes two disjoint
/// majorities impossible: any two sets that each satisfy it must overlap, in both configurations.
/// </para>
/// <para>
/// One subtlety that is easy to get wrong: a configuration entry takes effect when it is
/// <b>appended</b>, not when it commits. Waiting for commitment would mean the leader counts
/// votes under a configuration it has already superseded, which reopens the window the whole
/// mechanism exists to close.
/// </para>
/// </remarks>
public sealed record RaftConfiguration
{
    /// <summary>The voters a decision is counted against.</summary>
    public required IReadOnlyList<string> Voters { get; init; }

    /// <summary>
    /// During a joint configuration, the voters being transitioned away from. Null outside a
    /// transition.
    /// </summary>
    public IReadOnlyList<string>? OutgoingVoters { get; init; }

    /// <summary>True while a membership change is in flight.</summary>
    public bool IsJoint => OutgoingVoters is { Count: > 0 };

    /// <summary>Every server that must be replicated to, across both configurations.</summary>
    public IReadOnlyList<string> AllServers =>
        OutgoingVoters is null
            ? Voters
            : [.. Voters.Concat(OutgoingVoters).Distinct()];

    /// <summary>
    /// The configuration a server joining an existing cluster starts from: no voters, so it
    /// cannot campaign, and it waits to learn the membership from the leader.
    /// </summary>
    public static RaftConfiguration Empty { get; } = new() { Voters = [] };

    /// <summary>Creates a simple, non-joint configuration.</summary>
    public static RaftConfiguration Of(params string[] voters) =>
        new() { Voters = [.. voters.Distinct()] };

    /// <summary>Creates a simple, non-joint configuration.</summary>
    public static RaftConfiguration Of(IEnumerable<string> voters) =>
        new() { Voters = [.. voters.Distinct()] };

    /// <summary>Begins a transition to <paramref name="newVoters"/>.</summary>
    public RaftConfiguration BeginTransitionTo(IEnumerable<string> newVoters)
    {
        if (IsJoint)
        {
            throw new InvalidOperationException(
                "A membership change is already in progress; the joint configuration must commit "
                    + "before another change can begin.");
        }

        var target = newVoters.Distinct().ToArray();
        if (target.Length == 0)
        {
            throw new ArgumentException(
                "A configuration must contain at least one voter.", nameof(newVoters));
        }

        return new RaftConfiguration { Voters = target, OutgoingVoters = Voters };
    }

    /// <summary>Completes a transition, leaving only the incoming voters.</summary>
    public RaftConfiguration CompleteTransition()
    {
        if (!IsJoint)
        {
            throw new InvalidOperationException("No membership change is in progress.");
        }

        return new RaftConfiguration { Voters = Voters };
    }

    /// <summary>Whether <paramref name="nodeId"/> may vote under this configuration.</summary>
    public bool Contains(string nodeId) =>
        Voters.Contains(nodeId) || (OutgoingVoters?.Contains(nodeId) ?? false);

    /// <summary>Whether <paramref name="nodeId"/> is a voter in the configuration being moved to.</summary>
    public bool ContainsIncoming(string nodeId) => Voters.Contains(nodeId);

    /// <summary>
    /// Whether <paramref name="granters"/> constitutes a decision under this configuration.
    /// </summary>
    /// <remarks>
    /// This single method is where joint consensus actually bites. Everything that needs a
    /// quorum — counting votes in an election, deciding an entry is committed — goes through
    /// here, so the joint rule is applied uniformly and cannot be forgotten at one of the call
    /// sites.
    /// </remarks>
    public bool HasQuorum(IReadOnlySet<string> granters)
    {
        if (!IsMajority(Voters, granters)) return false;
        if (OutgoingVoters is null) return true;

        // Both majorities, during a transition. Any two sets that each satisfy this must
        // intersect in both configurations, which is what rules out two simultaneous leaders.
        return IsMajority(OutgoingVoters, granters);
    }

    private static bool IsMajority(IReadOnlyList<string> voters, IReadOnlySet<string> granters)
    {
        // An empty voter set has no majority, which matters for a specific case: a server joining
        // an existing cluster starts with no configuration at all and learns it from the leader.
        // Treating "no voters" as trivially satisfied would let such a node decide it had won an
        // election before it knew the cluster existed.
        if (voters.Count == 0) return false;

        int count = 0;
        foreach (string voter in voters)
        {
            if (granters.Contains(voter)) count++;
        }

        return count >= (voters.Count / 2) + 1;
    }

    /// <summary>Votes needed in the incoming configuration, for diagnostics.</summary>
    public int QuorumSize => (Voters.Count / 2) + 1;

    /// <inheritdoc />
    public override string ToString() =>
        IsJoint
            ? $"joint([{string.Join(",", OutgoingVoters!)}] -> [{string.Join(",", Voters)}])"
            : $"[{string.Join(",", Voters)}]";
}
