namespace KestrelCache.Raft;

/// <summary>
/// A key-value store whose writes go through Raft before they are applied.
/// </summary>
/// <remarks>
/// <para>
/// This is where consensus meets storage. A write is encoded as a command, replicated to a
/// majority, and only then applied to the local engine — so every node applies the same commands
/// in the same order and the replicas converge. The storage engine itself is untouched by any of
/// this: it does not know it is being replicated, which is why the same engines serve both the
/// single-node and the clustered deployment.
/// </para>
/// <para><b>What the reads do and do not guarantee</b></para>
/// <para>
/// Reads go straight to the local engine, which makes them fast and, on a follower, possibly
/// stale: a follower may not yet have applied an entry the leader has already committed. This is
/// a real and deliberate limitation, not an oversight. Making every read linearizable requires
/// either routing it through the log (a full round trip per read) or having the leader confirm
/// its leadership with a quorum first (the ReadIndex technique), and both trade read latency for
/// a guarantee many callers do not need.
/// </para>
/// <para>
/// What is guaranteed: a read on the <i>leader</i> after a successful write on the leader sees
/// that write, because <see cref="RaftNode.ProposeAsync"/> returns only once the entry has been
/// applied locally. Read-your-writes holds against the leader; cluster-wide linearizability does
/// not.
/// </para>
/// </remarks>
public sealed class ReplicatedStore : IRaftStateMachine, IAsyncDisposable
{
    private readonly IStorageEngine _engine;
    private readonly bool _ownsEngine;
    private RaftNode? _node;

    /// <summary>Wraps a storage engine as a replicated state machine.</summary>
    /// <param name="engine">The local engine committed entries are applied to.</param>
    /// <param name="ownsEngine">Whether disposing this store should dispose the engine.</param>
    public ReplicatedStore(IStorageEngine engine, bool ownsEngine = false)
    {
        ArgumentNullException.ThrowIfNull(engine);
        _engine = engine;
        _ownsEngine = ownsEngine;
    }

    /// <summary>
    /// Associates the consensus node. Called once, after construction, because the node needs
    /// the state machine and the state machine needs the node.
    /// </summary>
    public void Attach(RaftNode node)
    {
        ArgumentNullException.ThrowIfNull(node);
        if (_node is not null)
        {
            throw new InvalidOperationException("This store is already attached to a node.");
        }
        _node = node;
    }

    private RaftNode Node => _node
        ?? throw new InvalidOperationException("Attach a RaftNode before using the store.");

    /// <summary>The local engine, for reads and for inspection.</summary>
    public IStorageEngine Engine => _engine;

    /// <summary>True when this node currently accepts writes.</summary>
    public bool IsLeader => Node.IsLeader;

    /// <summary>The node that currently accepts writes, if known.</summary>
    public string? LeaderId => Node.LeaderId;

    /// <summary>Consensus counters.</summary>
    public RaftStats GetRaftStats() => Node.GetStats();

    /// <summary>Storage counters from the local engine.</summary>
    public EngineStats GetEngineStats() => _engine.GetStats();

    // ---------------------------------------------------------------- writes

    /// <summary>Replicates a put and returns once it has been applied locally.</summary>
    public Task PutAsync(byte[] key, byte[] value, CancellationToken cancellationToken = default) =>
        WriteAsync(new WriteBatch().Put(key, value), cancellationToken);

    /// <summary>Replicates a delete.</summary>
    public Task DeleteAsync(byte[] key, CancellationToken cancellationToken = default) =>
        WriteAsync(new WriteBatch().Delete(key), cancellationToken);

    /// <summary>
    /// Replicates a whole batch as one command, so it stays atomic across the cluster.
    /// </summary>
    /// <exception cref="NotLeaderException">This node is not the leader.</exception>
    public async Task WriteAsync(WriteBatch batch, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (batch.Count == 0) return;

        byte[] command = RaftCommand.Encode(batch);
        await Node.ProposeAsync(command, cancellationToken).ConfigureAwait(false);
    }

    // ---------------------------------------------------------------- reads

    /// <summary>
    /// Reads from the local engine. May be stale on a follower; see the remarks on this type.
    /// </summary>
    public ValueTask<byte[]?> GetAsync(byte[] key, CancellationToken cancellationToken = default) =>
        _engine.GetAsync(key, cancellationToken);

    /// <summary>
    /// Reads, refusing to serve the request unless this node is the leader.
    /// </summary>
    /// <remarks>
    /// Still not linearizable on its own: a leader that has been deposed by a partition it has
    /// not noticed yet will happily answer from stale state. It does rule out the much more
    /// common staleness of reading from a follower that is simply behind, which is usually what
    /// a caller asking for a "consistent read" actually means.
    /// </remarks>
    public ValueTask<byte[]?> GetFromLeaderAsync(
        byte[] key,
        CancellationToken cancellationToken = default)
    {
        if (!Node.IsLeader) throw new NotLeaderException(Node.LeaderId);
        return _engine.GetAsync(key, cancellationToken);
    }

    /// <summary>Streams a key range from the local engine, if it supports ordered iteration.</summary>
    public IAsyncEnumerable<KeyValuePair<byte[], byte[]>> ScanAsync(
        byte[]? start = null,
        byte[]? end = null,
        CancellationToken cancellationToken = default)
    {
        if (_engine is not IScannableStorageEngine scannable)
        {
            throw new NotSupportedException(
                $"The {_engine.Name} engine keeps an unordered index and cannot serve range scans.");
        }

        return scannable.ScanAsync(start, end, cancellationToken);
    }

    // ---------------------------------------------------------------- state machine

    /// <inheritdoc />
    public async ValueTask ApplyAsync(RaftLogEntry entry, CancellationToken cancellationToken)
    {
        if (entry.IsNoOp || entry.Command.Length == 0) return;

        var batch = RaftCommand.Decode(entry.Command);

        // Applied through the engine's own atomic batch, so a crash between replication and
        // application leaves the engine coherent and the entry is simply re-applied on restart.
        //
        // Re-application is safe because every command here is idempotent in the sense that
        // matters: a put sets a key to a value and a delete removes it, so applying the same
        // entry twice reaches the same state as applying it once. Raft replays from lastApplied
        // on restart, and this is why that replay cannot corrupt anything.
        await _engine.WriteAsync(batch, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_ownsEngine)
        {
            await _engine.DisposeAsync().ConfigureAwait(false);
        }
    }
}
