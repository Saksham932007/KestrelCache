using KestrelCache.Bitcask;

namespace KestrelCache;

/// <summary>
/// The front door: a small, string-friendly wrapper over whichever
/// <see cref="IStorageEngine"/> is in use.
/// </summary>
/// <remarks>
/// The byte-array API on <see cref="IStorageEngine"/> is the real one, because a database has no
/// business assuming its values are text. This wrapper exists so that the common case — UTF-8
/// strings — stays a one-liner, and so that callers have a single disposable object to own.
/// </remarks>
public sealed class KestrelDb : IAsyncDisposable
{
    private readonly IStorageEngine _engine;
    private int _closed;

    private KestrelDb(IStorageEngine engine) => _engine = engine;

    /// <summary>The underlying engine, for callers that need the byte-level or scan APIs.</summary>
    public IStorageEngine Engine => _engine;

    /// <summary>Opens a database at <paramref name="path"/> using the default engine.</summary>
    public static ValueTask<KestrelDb> OpenAsync(string path, EngineKind engine = EngineKind.Bitcask) =>
        OpenAsync(new DatabaseOptions { Path = path }, engine);

    /// <summary>Opens a database with explicit options.</summary>
    public static async ValueTask<KestrelDb> OpenAsync(
        DatabaseOptions options,
        EngineKind engine = EngineKind.Bitcask)
    {
        ArgumentNullException.ThrowIfNull(options);

        IStorageEngine inner = engine switch
        {
            EngineKind.Bitcask => await BitcaskEngine.OpenAsync(options).ConfigureAwait(false),
            EngineKind.Lsm => throw new NotSupportedException("The LSM engine arrives in phase 2."),
            _ => throw new ArgumentOutOfRangeException(nameof(engine), engine, "Unknown engine."),
        };

        return new KestrelDb(inner);
    }

    /// <summary>Wraps an engine the caller constructed itself.</summary>
    public static KestrelDb Wrap(IStorageEngine engine)
    {
        ArgumentNullException.ThrowIfNull(engine);
        return new KestrelDb(engine);
    }

    /// <summary>Reads a UTF-8 string value, or <c>null</c> when the key is absent.</summary>
    public async ValueTask<string?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        byte[]? value = await _engine.GetAsync(ByteKey.From(key), cancellationToken).ConfigureAwait(false);
        return value is null ? null : ByteKey.ToString(value);
    }

    /// <summary>Writes a UTF-8 string value.</summary>
    public ValueTask PutAsync(string key, string value, CancellationToken cancellationToken = default) =>
        _engine.PutAsync(ByteKey.From(key), ByteKey.From(value), cancellationToken);

    /// <summary>Removes a key.</summary>
    public ValueTask DeleteAsync(string key, CancellationToken cancellationToken = default) =>
        _engine.DeleteAsync(ByteKey.From(key), cancellationToken);

    /// <summary>Applies a batch atomically.</summary>
    public ValueTask WriteAsync(WriteBatch batch, CancellationToken cancellationToken = default) =>
        _engine.WriteAsync(batch, cancellationToken);

    /// <summary>Forces every acknowledged write to stable storage.</summary>
    public ValueTask FlushAsync(CancellationToken cancellationToken = default) =>
        _engine.FlushAsync(cancellationToken);

    /// <summary>Reclaims space held by stale and deleted records.</summary>
    public ValueTask CompactAsync(CancellationToken cancellationToken = default) =>
        _engine.CompactAsync(cancellationToken);

    /// <summary>Returns a counter snapshot from the underlying engine.</summary>
    public EngineStats GetStats() => _engine.GetStats();

    /// <summary>
    /// Streams UTF-8 key-value pairs in ascending key order. Only available on engines that
    /// maintain sorted order.
    /// </summary>
    /// <exception cref="NotSupportedException">The engine does not support ordered scans.</exception>
    public async IAsyncEnumerable<KeyValuePair<string, string>> ScanAsync(
        string? startInclusive = null,
        string? endExclusive = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (_engine is not IScannableStorageEngine scannable)
        {
            throw new NotSupportedException(
                $"The {_engine.Name} engine keeps an unordered index and cannot serve range scans. "
                    + "Open the database with EngineKind.Lsm for ordered iteration.");
        }

        var start = startInclusive is null ? null : ByteKey.From(startInclusive);
        var end = endExclusive is null ? null : ByteKey.From(endExclusive);

        await foreach (var pair in scannable.ScanAsync(start, end, cancellationToken).ConfigureAwait(false))
        {
            yield return new KeyValuePair<string, string>(
                ByteKey.ToString(pair.Key),
                ByteKey.ToString(pair.Value));
        }
    }

    /// <summary>
    /// Streams every key-value pair whose key starts with <paramref name="prefix"/>, in order.
    /// </summary>
    public IAsyncEnumerable<KeyValuePair<string, string>> ScanPrefixAsync(
        string prefix,
        CancellationToken cancellationToken = default)
    {
        byte[] start = ByteKey.From(prefix);
        byte[]? end = ByteKey.PrefixUpperBound(start);
        return ScanAsync(
            ByteKey.ToString(start),
            end is null ? null : ByteKey.ToString(end),
            cancellationToken);
    }

    /// <summary>
    /// Flushes and closes the database. Safe to call more than once, and safe to call before
    /// <see cref="DisposeAsync"/>.
    /// </summary>
    public async ValueTask CloseAsync()
    {
        // Guarding with an interlocked flag makes close idempotent. Without it, the documented
        // usage of calling CloseAsync() and then disposing (or using `await using`) threw
        // ObjectDisposedException from the second teardown.
        if (Interlocked.Exchange(ref _closed, 1) != 0) return;
        await _engine.DisposeAsync().ConfigureAwait(false);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync() => CloseAsync();
}
