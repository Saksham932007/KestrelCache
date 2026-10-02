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

    /// <summary>Wraps a replicated store as an engine.</summary>
    public ReplicatedEngine(ReplicatedStore store, RaftNode node)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(node);
        _store = store;
        _node = node;
    }

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
