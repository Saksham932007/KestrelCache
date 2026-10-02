namespace KestrelCache;

/// <summary>
/// The storage contract both engines implement: a durable, crash-recoverable map from
/// byte-string keys to byte-string values.
/// </summary>
/// <remarks>
/// Keeping this interface narrow is what lets the Bitcask and LSM engines be benchmarked head
/// to head on identical workloads, which in turn is what makes their trade-offs measurable
/// rather than merely assertable.
/// </remarks>
public interface IStorageEngine : IAsyncDisposable
{
    /// <summary>Human-readable engine name, used in stats and logs.</summary>
    string Name { get; }

    /// <summary>
    /// Returns the value for <paramref name="key"/>, or <c>null</c> if the key is absent.
    /// </summary>
    ValueTask<byte[]?> GetAsync(byte[] key, CancellationToken cancellationToken = default);

    /// <summary>Inserts or overwrites a single key.</summary>
    ValueTask PutAsync(byte[] key, byte[] value, CancellationToken cancellationToken = default);

    /// <summary>Removes a key. Deleting an absent key is a no-op that still succeeds.</summary>
    ValueTask DeleteAsync(byte[] key, CancellationToken cancellationToken = default);

    /// <summary>Applies every mutation in <paramref name="batch"/> atomically.</summary>
    ValueTask WriteAsync(WriteBatch batch, CancellationToken cancellationToken = default);

    /// <summary>
    /// Forces all previously acknowledged writes to stable storage, regardless of the configured
    /// <see cref="SyncPolicy"/>.
    /// </summary>
    ValueTask FlushAsync(CancellationToken cancellationToken = default);

    /// <summary>Reclaims space occupied by stale and deleted records, and waits for it to finish.</summary>
    ValueTask CompactAsync(CancellationToken cancellationToken = default);

    /// <summary>Returns a counter snapshot.</summary>
    EngineStats GetStats();
}

/// <summary>
/// An engine that can additionally iterate keys in sorted order.
/// </summary>
/// <remarks>
/// This capability is split out rather than folded into <see cref="IStorageEngine"/> because it
/// is exactly where the two designs genuinely diverge. A Bitcask-style engine keeps an
/// unordered hash index, so it can answer point lookups in one seek but fundamentally cannot
/// serve an ordered range scan without sorting its entire keyspace first. An LSM tree keeps
/// everything sorted at every level, so scans are a merge of a handful of cursors. Expressing
/// that in the type system means the limitation is visible at compile time instead of
/// surfacing as a runtime <c>NotSupportedException</c>.
/// </remarks>
public interface IScannableStorageEngine : IStorageEngine
{
    /// <summary>
    /// Streams key-value pairs in ascending key order over the half-open range
    /// <c>[start, end)</c>. A <c>null</c> bound is unbounded on that side.
    /// </summary>
    IAsyncEnumerable<KeyValuePair<byte[], byte[]>> ScanAsync(
        byte[]? start = null,
        byte[]? end = null,
        CancellationToken cancellationToken = default);
}
