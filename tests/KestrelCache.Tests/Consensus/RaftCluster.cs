using KestrelCache.Raft;
using Xunit;

namespace KestrelCache.Tests;

/// <summary>
/// A Raft cluster running entirely in process, on a network the test controls.
/// </summary>
/// <remarks>
/// <para>
/// The timers are not started. Tests drive the cluster by calling <see cref="TickAsync"/>, which
/// is what makes consensus testable: an election becomes a bounded number of deterministic steps
/// rather than a sleep and a hope. A test can tick until a leader emerges, partition the
/// network, tick again, and assert exactly what happened — repeatably, and in milliseconds.
/// </para>
/// <para>
/// Each node gets its own real storage engine and its own real Raft log on disk, so persistence
/// and recovery are genuinely exercised. Only the network is simulated.
/// </para>
/// </remarks>
public sealed class RaftCluster : IAsyncDisposable
{
    private readonly TempDirectory _directory;
    private readonly Dictionary<string, Member> _members = [];

    private readonly List<string> _trace = [];
    private readonly object _traceGate = new();

    private RaftCluster(TempDirectory directory, InMemoryRaftNetwork network)
    {
        _directory = directory;
        Network = network;
    }

    /// <summary>Every traced transition, in order, for assertion messages.</summary>
    public IReadOnlyList<string> Trace
    {
        get { lock (_traceGate) return [.. _trace]; }
    }

    private void Record(string message)
    {
        lock (_traceGate) _trace.Add(message);
    }

    /// <summary>The simulated network, for injecting partitions and loss.</summary>
    public InMemoryRaftNetwork Network { get; }

    /// <summary>Every node's id.</summary>
    public IReadOnlyCollection<string> NodeIds => _members.Keys;

    /// <summary>One node and everything belonging to it.</summary>
    public sealed record Member(
        string NodeId,
        RaftNode Node,
        ReplicatedStore Store,
        IStorageEngine Engine,
        string DataDirectory);

    private EngineKind _engineKind = EngineKind.Lsm;
    private Func<RaftOptions, RaftOptions>? _configure;

    /// <summary>Starts a cluster of <paramref name="size"/> nodes named n1..nN.</summary>
    public static async Task<RaftCluster> StartAsync(
        int size,
        EngineKind engine = EngineKind.Lsm,
        Func<RaftOptions, RaftOptions>? configure = null)
    {
        var directory = new TempDirectory($"raft-{size}");
        var network = new InMemoryRaftNetwork();
        var cluster = new RaftCluster(directory, network)
        {
            _engineKind = engine,
            _configure = configure,
        };

        var ids = Enumerable.Range(1, size).Select(i => $"n{i}").ToArray();

        foreach (string id in ids)
        {
            await cluster.AddMemberAsync(id, ids, engine, configure);
        }

        return cluster;
    }

    /// <summary>
    /// Brings up a server that is not yet a cluster member, with no bootstrap configuration.
    /// </summary>
    /// <remarks>
    /// An empty peer list is how Raft models a joining server: it cannot campaign, because it has
    /// no voters to count a majority against, and it waits to learn the membership from whichever
    /// leader starts replicating to it. Starting it with the current membership instead would let
    /// it elect itself the moment it timed out.
    /// </remarks>
    public Task AddJoiningNodeAsync(string id) =>
        AddMemberAsync(id, [], _engineKind, _configure);

    private async Task AddMemberAsync(
        string id,
        IReadOnlyList<string> allIds,
        EngineKind engineKind,
        Func<RaftOptions, RaftOptions>? configure)
    {
        string nodeDirectory = _directory.File(id);
        Directory.CreateDirectory(nodeDirectory);

        var engine = await OpenEngineAsync(engineKind, nodeDirectory);

        var options = new RaftOptions
        {
            NodeId = id,
            Peers = allIds,
            DataDirectory = Path.Combine(nodeDirectory, "raft"),
            // Short timeouts, because tests drive the clock by ticking rather than sleeping.
            ElectionTimeout = TimeSpan.FromMilliseconds(60),
            HeartbeatInterval = TimeSpan.FromMilliseconds(10),
            RpcTimeout = TimeSpan.FromMilliseconds(200),
            // A distinct seed per node, so their randomised timeouts differ and a split vote can
            // actually resolve.
            RandomSeed = id.GetHashCode(StringComparison.Ordinal),
            Trace = Record,
        };

        // RaftOptions is a record with init-only properties, so a test tweaks it by returning a
        // modified copy rather than mutating the original.
        if (configure is not null) options = configure(options);

        var store = new ReplicatedStore(engine);
        var node = new RaftNode(options, transport: Network.TransportFor(id), stateMachine: store);
        store.Attach(node);
        Network.Register(node);

        _members[id] = new Member(id, node, store, engine, nodeDirectory);
    }

    private static async ValueTask<IStorageEngine> OpenEngineAsync(EngineKind kind, string directory) =>
        kind == EngineKind.Bitcask
            ? await Bitcask.BitcaskEngine.OpenAsync(new DatabaseOptions
            {
                Path = Path.Combine(directory, "data.kc"),
                SyncPolicy = SyncPolicy.None,
                AutoCompactStaleRatio = 0,
            })
            : await Lsm.LsmEngine.OpenAsync(new DatabaseOptions
            {
                Path = Path.Combine(directory, "data"),
                SyncPolicy = SyncPolicy.None,
                EnableBackgroundCompaction = false,
            });

    /// <summary>A node by id.</summary>
    public Member this[string nodeId] => _members[nodeId];

    /// <summary>Every node.</summary>
    public IEnumerable<Member> Members => _members.Values;

    /// <summary>Nodes that currently believe they are the leader.</summary>
    public IReadOnlyList<Member> Leaders =>
        [.. _members.Values.Where(member => member.Node.Role == RaftRole.Leader)];

    /// <summary>Advances every node by one step.</summary>
    public async Task TickAsync(CancellationToken cancellationToken = default)
    {
        foreach (var member in _members.Values.ToList())
        {
            await member.Node.TickAsync(cancellationToken);
        }
    }

    /// <summary>Advances every node, repeatedly, until <paramref name="condition"/> holds.</summary>
    public async Task<bool> TickUntilAsync(
        Func<bool> condition,
        int maxTicks = 400,
        int millisecondsBetweenTicks = 2)
    {
        for (int i = 0; i < maxTicks; i++)
        {
            if (condition()) return true;
            await TickAsync();
            if (millisecondsBetweenTicks > 0) await Task.Delay(millisecondsBetweenTicks);
        }

        return condition();
    }

    /// <summary>
    /// Ticks until exactly one node is leading, and returns it.
    /// </summary>
    public async Task<Member> WaitForLeaderAsync(int maxTicks = 400)
    {
        bool elected = await TickUntilAsync(() => Leaders.Count == 1, maxTicks);

        if (!elected)
        {
            var roles = string.Join(
                ", ",
                _members.Values.Select(m =>
                    $"{m.NodeId}={m.Node.Role}(term {m.Node.CurrentTerm})"));
            throw new InvalidOperationException(
                $"No single leader emerged after {maxTicks} ticks: {roles}");
        }

        return Leaders[0];
    }

    /// <summary>Ticks until a leader emerges among the given nodes.</summary>
    public async Task<Member?> WaitForLeaderAmongAsync(
        IEnumerable<string> candidates,
        int maxTicks = 400)
    {
        var allowed = candidates.ToHashSet();

        await TickUntilAsync(
            () => _members.Values.Count(m =>
                allowed.Contains(m.NodeId) && m.Node.Role == RaftRole.Leader) == 1,
            maxTicks);

        return _members.Values.FirstOrDefault(m =>
            allowed.Contains(m.NodeId) && m.Node.Role == RaftRole.Leader);
    }

    /// <summary>Ticks until every listed node has applied up to <paramref name="index"/>.</summary>
    public Task<bool> WaitForAppliedAsync(long index, IEnumerable<string>? nodes = null, int maxTicks = 400)
    {
        var ids = (nodes ?? _members.Keys).ToHashSet();
        return TickUntilAsync(
            () => _members.Values.Where(m => ids.Contains(m.NodeId))
                .All(m => m.Node.LastApplied >= index),
            maxTicks);
    }

    /// <summary>
    /// Stops a node's consensus module but leaves its disk state, so it can be restarted.
    /// </summary>
    public async Task StopAsync(string nodeId)
    {
        var member = _members[nodeId];
        await member.Node.DisposeAsync();
        await member.Engine.DisposeAsync();
        _members.Remove(nodeId);
        Network.Isolate(nodeId);
    }

    /// <summary>Ticks until every live node agrees on the configuration, then returns it.</summary>
    public async Task<RaftConfiguration?> WaitForConfigurationAsync(
        IReadOnlyList<string> expectedVoters,
        int maxTicks = 800)
    {
        var expected = expectedVoters.ToHashSet();

        await TickUntilAsync(
            () => _members.Values.All(m =>
            {
                var configuration = m.Node.Configuration;
                return !configuration.IsJoint
                    && configuration.Voters.Count == expected.Count
                    && configuration.Voters.All(expected.Contains);
            }),
            maxTicks);

        return _members.Values.FirstOrDefault()?.Node.Configuration;
    }

    /// <summary>
    /// Brings a stopped node back, reopening its log and state from disk.
    /// </summary>
    /// <remarks>
    /// This is the test that matters most for persistence: a restarted node must remember its
    /// term, its vote and its log, or the protocol's safety guarantees do not hold.
    /// </remarks>
    public async Task RestartAsync(string nodeId, IReadOnlyList<string> allIds)
    {
        Network.Heal(nodeId);
        await AddMemberAsync(nodeId, allIds, _engineKind, _configure);
    }

    /// <summary>A readable dump of every node's state, for assertion messages.</summary>
    public string Describe() => string.Join(
        "\n",
        _members.Values.Select(m =>
        {
            var stats = m.Node.GetStats();
            string snapshot = stats.SnapshotIndex > 0
                ? $" snap={stats.SnapshotIndex}@{stats.SnapshotTerm}"
                : string.Empty;
            return $"  {m.NodeId}: {stats.Role,-9} term={stats.Term} "
                + $"log=[{stats.FirstLogIndex}..{stats.LastLogIndex}]{snapshot} "
                + $"commit={stats.CommitIndex} applied={stats.LastApplied} "
                + $"leader={stats.LeaderId ?? "?"} cfg={stats.Configuration}";
        }));

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        foreach (var member in _members.Values.ToList())
        {
            await member.Node.DisposeAsync();
            await member.Engine.DisposeAsync();
        }
        _members.Clear();
        _directory.Dispose();
    }
}

/// <summary>
/// Cluster tests run one at a time.
/// </summary>
/// <remarks>
/// They drive consensus against wall-clock election timeouts, so starving them of CPU makes a
/// healthy leader look like a failed one and a tick budget run out for reasons that have nothing
/// to do with the code. On a four-thread machine, running them alongside the rest of the suite
/// produced failures that disappeared in isolation -- which is the worst kind of test, because it
/// trains you to ignore it.
/// </remarks>
[CollectionDefinition("raft-cluster", DisableParallelization = true)]
public sealed class RaftClusterCollection;
