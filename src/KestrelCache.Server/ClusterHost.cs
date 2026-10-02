using KestrelCache.Raft;

namespace KestrelCache.Server;

/// <summary>
/// Assembles the pieces a clustered node needs: a local engine, consensus over it, and a peer
/// listener.
/// </summary>
/// <remarks>
/// The ordering here is the only subtle part. The peer listener has to be accepting before the
/// node starts campaigning, because a node that begins an election while its own inbound port is
/// closed cannot be voted for — every peer's reply would fail to reach it, and the cluster would
/// spend several election timeouts getting nowhere on startup.
/// </remarks>
public sealed class ClusterHost : IAsyncDisposable
{
    private readonly RaftRpcServer _rpcServer;
    private readonly TcpRaftTransport _transport;
    private readonly CancellationTokenSource _shutdown = new();
    private Task? _serving;
    private int _disposed;

    private ClusterHost(
        ReplicatedEngine engine,
        RaftNode node,
        RaftRpcServer rpcServer,
        TcpRaftTransport transport)
    {
        Engine = engine;
        Node = node;
        _rpcServer = rpcServer;
        _transport = transport;
    }

    /// <summary>The engine the RESP server serves from.</summary>
    public ReplicatedEngine Engine { get; }

    /// <summary>The consensus node.</summary>
    public RaftNode Node { get; }

    /// <summary>The peer port actually bound.</summary>
    public int RaftPort => _rpcServer.BoundPort;

    /// <summary>Builds a clustered node from the server's options.</summary>
    public static async Task<ClusterHost> StartAsync(
        ServerOptions options,
        IStorageEngine localEngine,
        Action<string>? trace = null)
    {
        var addresses = options.RaftPeers.Select(RaftPeerAddress.Parse).ToList();

        if (addresses.All(a => a.NodeId != options.RaftNodeId))
        {
            throw new ArgumentException(
                $"--raft-peers must include this node ('{options.RaftNodeId}'); got "
                    + string.Join(", ", addresses.Select(a => a.NodeId)),
                nameof(options));
        }

        var raftOptions = new RaftOptions
        {
            NodeId = options.RaftNodeId!,
            Peers = [.. addresses.Select(a => a.NodeId)],
            DataDirectory = Path.Combine(
                Path.GetFullPath(options.DataPath) + "-raft", options.RaftNodeId!),
            ElectionTimeout = options.RaftElectionTimeout,
            HeartbeatInterval = options.RaftHeartbeatInterval,
            Trace = trace,
        };

        var transport = new TcpRaftTransport(addresses);
        var store = new ReplicatedStore(localEngine, ownsEngine: true);
        var node = new RaftNode(raftOptions, transport, store);
        store.Attach(node);

        var rpcServer = new RaftRpcServer(node, options.BindAddress, options.RaftPort);
        rpcServer.Bind();

        var host = new ClusterHost(new ReplicatedEngine(store, node), node, rpcServer, transport);

        // Accept peer traffic before campaigning. See the remarks on this type.
        host._serving = rpcServer.RunAsync(host._shutdown.Token);
        await node.StartAsync().ConfigureAwait(false);

        return host;
    }

    /// <inheritdoc />
    /// <remarks>
    /// Guarded so that disposing twice is safe. Cancelling and then disposing a
    /// <see cref="CancellationTokenSource"/> leaves a second call to throw
    /// <see cref="ObjectDisposedException"/>, and double disposal is not an exotic case: nested
    /// <c>await using</c> blocks, a teardown path that also disposes its children, and an
    /// explicit close followed by a dispose all produce it. Throwing on teardown turns an
    /// orderly shutdown into a crash.
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        await _shutdown.CancelAsync().ConfigureAwait(false);
        await _rpcServer.DisposeAsync().ConfigureAwait(false);

        if (_serving is not null)
        {
            try
            {
                await _serving.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected.
            }
        }

        await Engine.DisposeAsync().ConfigureAwait(false);
        await _transport.DisposeAsync().ConfigureAwait(false);
        _shutdown.Dispose();
    }
}
