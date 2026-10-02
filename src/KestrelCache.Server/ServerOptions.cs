namespace KestrelCache.Server;

/// <summary>How the server is configured at startup.</summary>
public sealed record ServerOptions
{
    /// <summary>Address to bind the RESP listener to.</summary>
    public string BindAddress { get; init; } = "0.0.0.0";

    /// <summary>
    /// RESP port. Defaults to 6380 rather than Redis's 6379 so that running this alongside a
    /// real Redis — which is exactly what a comparative benchmark wants — needs no configuration.
    /// </summary>
    public int Port { get; init; } = 6380;

    /// <summary>Port serving Prometheus metrics and health checks over HTTP. Zero disables it.</summary>
    public int MetricsPort { get; init; } = 9180;

    /// <summary>Database directory (LSM) or file (Bitcask).</summary>
    public string DataPath { get; init; } = "./kc-data";

    /// <summary>Which storage engine to serve from.</summary>
    public EngineKind Engine { get; init; } = EngineKind.Lsm;

    /// <summary>Durability policy for acknowledged writes.</summary>
    public SyncPolicy SyncPolicy { get; init; } = SyncPolicy.Interval;

    /// <summary>Maximum simultaneous client connections. Zero means unlimited.</summary>
    public int MaxConnections { get; init; } = 10_000;

    /// <summary>
    /// Require <c>AUTH</c> with this password before serving commands. Empty disables
    /// authentication.
    /// </summary>
    public string? RequirePassword { get; init; }

    /// <summary>
    /// This node's id when running as part of a cluster. Null runs a single, unreplicated node.
    /// </summary>
    public string? RaftNodeId { get; init; }

    /// <summary>Every node in the cluster, as <c>id=host:port</c>, this one included.</summary>
    public IReadOnlyList<string> RaftPeers { get; init; } = [];

    /// <summary>Port this node listens on for peer traffic.</summary>
    public int RaftPort { get; init; } = 7380;

    /// <summary>
    /// Start as a server joining an existing cluster rather than bootstrapping a new one.
    /// </summary>
    /// <remarks>
    /// <c>--raft-peers</c> serves two purposes that have to be separated for a joining server:
    /// it says who the voters are, and it says how to reach them. A joining server needs the
    /// addresses — it will have to replicate and vote once it is admitted — but must start with
    /// <i>no</i> voters, because a server that counts itself a voter can elect itself the moment
    /// its election timer fires, before it has ever heard from the cluster it is joining.
    /// </remarks>
    public bool RaftJoin { get; init; }

    /// <summary>How long a follower waits without hearing from a leader before standing for election.</summary>
    public TimeSpan RaftElectionTimeout { get; init; } = TimeSpan.FromMilliseconds(600);

    /// <summary>How often a leader sends heartbeats.</summary>
    public TimeSpan RaftHeartbeatInterval { get; init; } = TimeSpan.FromMilliseconds(100);

    /// <summary>True when this server is configured to replicate.</summary>
    public bool IsClustered => !string.IsNullOrWhiteSpace(RaftNodeId) && RaftPeers.Count > 0;

    /// <summary>
    /// The voters this node bootstraps with: everyone for a founding member, nobody for a
    /// joining one.
    /// </summary>
    public IReadOnlyList<string> BootstrapVoters =>
        RaftJoin ? [] : [.. RaftPeers.Select(peer => peer.Split('=')[0])];

    /// <summary>Builds the engine options this server will open its database with.</summary>
    public DatabaseOptions ToDatabaseOptions() => new()
    {
        Path = DataPath,
        SyncPolicy = SyncPolicy,
    };
}
