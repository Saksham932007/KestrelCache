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

    /// <summary>How long a follower waits without hearing from a leader before standing for election.</summary>
    public TimeSpan RaftElectionTimeout { get; init; } = TimeSpan.FromMilliseconds(600);

    /// <summary>How often a leader sends heartbeats.</summary>
    public TimeSpan RaftHeartbeatInterval { get; init; } = TimeSpan.FromMilliseconds(100);

    /// <summary>True when this server is configured to replicate.</summary>
    public bool IsClustered => !string.IsNullOrWhiteSpace(RaftNodeId) && RaftPeers.Count > 0;

    /// <summary>Builds the engine options this server will open its database with.</summary>
    public DatabaseOptions ToDatabaseOptions() => new()
    {
        Path = DataPath,
        SyncPolicy = SyncPolicy,
    };
}
