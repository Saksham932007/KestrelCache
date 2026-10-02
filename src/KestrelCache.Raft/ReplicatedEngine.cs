namespace KestrelCache.Raft;

/// <summary>
/// Presents a replicated cluster as an ordinary <see cref="IStorageEngine"/>.
/// </summary>
/// <remarks>
/// <para>
/// This adapter is what lets the entire server — the RESP protocol layer, the command table, the
/// metrics endpoint — work against a three-node cluster without a line of change. Writes are
/// routed through consensus, reads are served locally, and everything above sees the same
/// interface the single-node engines implement.
/// </para>
/// <para>
/// Keeping the seam at <see cref="IStorageEngine"/> rather than threading a cluster concept
/// through the server is worth the small amount of indirection: replication becomes a
/// deployment choice rather than a different codebase, and the same protocol tests cover both.
/// </para>
/// <para>
/// The one visible difference is that a write on a follower throws
/// <see cref="NotLeaderException"/>. That is deliberate and cannot be hidden — a follower
/// genuinely cannot accept a write, and silently forwarding it would turn one network hop into
/// two while making the failure modes much harder to reason about. The protocol layer turns the
/// exception into a redirect the client can act on.
/// </para>
/// </remarks>
public sealed class ReplicatedEngine : IScannableStorageEngine
{
    private readonly ReplicatedStore _store;
    private readonly RaftNode _node;

    private readonly IRaftPeerDirectory? _directory;

    /// <summary>Wraps a replicated store as an engine.</summary>
    /// <param name="store">The replicated state machine.</param>
    /// <param name="node">The consensus node.</param>
    /// <param name="directory">
    /// Where peer addresses are registered, so a server added at runtime can be reached. Null
    /// when the transport needs no addresses, as with an in-process network.
    /// </param>
    public ReplicatedEngine(
        ReplicatedStore store,
        RaftNode node,
        IRaftPeerDirectory? directory = null)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(node);
        _store = store;
        _node = node;
        _directory = directory;
    }

    /// <summary>Peers this node knows how to reach, when the transport tracks addresses.</summary>
    public IReadOnlyCollection<RaftPeerAddress> KnownPeers => _directory?.Peers ?? [];

    /// <summary>
    /// Adds a server to the cluster, registering its address first so it can be reached.
    /// </summary>
    /// <remarks>
    /// The order matters. Appending the configuration entry before the address is known would
    /// leave the leader unable to replicate to the very server it has just made a voter, so the
    /// joint entry could never commit and the change would wedge.
    /// </remarks>
    public async Task AddServerAsync(
        RaftPeerAddress address,
        CancellationToken cancellationToken = default)
    {
        _directory?.AddPeer(address);
        await _node.AddServerAsync(address.NodeId, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Removes a server from the cluster.</summary>
    /// <remarks>
    /// The address is forgotten only after the change has committed. Dropping it first would
    /// stop the leader replicating the very entry that tells the departing server it has been
    /// removed — leaving it to campaign indefinitely against a cluster it is no longer part of.
    /// </remarks>
    public async Task RemoveServerAsync(
        string nodeId,
        CancellationToken cancellationToken = default)
    {
        await _node.RemoveServerAsync(nodeId, cancellationToken).ConfigureAwait(false);
        _directory?.RemovePeer(nodeId);
    }

    /// <summary>Captures a snapshot and discards the log prefix it replaces.</summary>
    public Task<RaftSnapshotMetadata?> CreateSnapshotAsync(
        CancellationToken cancellationToken = default) =>
        _node.CreateSnapshotAsync(cancellationToken);

    /// <inheritdoc />
    public string Name => $"raft+{_store.Engine.Name}";

    /// <summary>The consensus node behind this engine.</summary>
    public RaftNode Node => _node;

    /// <summary>True when this node currently accepts writes.</summary>
    public bool IsLeader => _node.IsLeader;

    /// <summary>The node that currently accepts writes, if known.</summary>
    public string? LeaderId => _node.LeaderId;

    /// <inheritdoc />
    public ValueTask<byte[]?> GetAsync(byte[] key, CancellationToken cancellationToken = default) =>
        _store.GetAsync(key, cancellationToken);

    /// <inheritdoc />
    public async ValueTask PutAsync(
        byte[] key,
        byte[] value,
        CancellationToken cancellationToken = default) =>
        await _store.PutAsync(key, value, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async ValueTask DeleteAsync(byte[] key, CancellationToken cancellationToken = default) =>
        await _store.DeleteAsync(key, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async ValueTask WriteAsync(
        WriteBatch batch,
        CancellationToken cancellationToken = default) =>
        await _store.WriteAsync(batch, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public IAsyncEnumerable<KeyValuePair<byte[], byte[]>> ScanAsync(
        byte[]? start = null,
        byte[]? end = null,
        CancellationToken cancellationToken = default) =>
        _store.ScanAsync(start, end, cancellationToken);

    /// <summary>
    /// Flushes the local engine. Not replicated: durability is already guaranteed by the Raft
    /// log, which fsyncs every entry before acknowledging it.
    /// </summary>
    public ValueTask FlushAsync(CancellationToken cancellationToken = default) =>
        _store.Engine.FlushAsync(cancellationToken);

    /// <summary>
    /// Compacts the local engine only.
    /// </summary>
    /// <remarks>
    /// Compaction is deliberately not replicated. It changes how data is stored, not what the
    /// data is, so each node can and should decide independently when to do it — and putting it
    /// through the log would mean every node compacting simultaneously, which is precisely when
    /// you least want it.
    /// </remarks>
    public ValueTask CompactAsync(CancellationToken cancellationToken = default) =>
        _store.Engine.CompactAsync(cancellationToken);

    /// <inheritdoc />
    public EngineStats GetStats()
    {
        var stats = _store.GetEngineStats();
        return stats with { Engine = Name };
    }

    /// <summary>Consensus counters for this node.</summary>
    public RaftStats GetRaftStats() => _node.GetStats();

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await _node.DisposeAsync().ConfigureAwait(false);
        await _store.DisposeAsync().ConfigureAwait(false);
    }
}
