using System.Collections.Concurrent;

namespace KestrelCache.Lsm;

/// <summary>
/// Keeps SSTable readers open and shared, so that a lookup does not pay to open a file and parse
/// its index and filter every time.
/// </summary>
/// <remarks>
/// <para>
/// Opening a table means a seek to its footer, a read of its Bloom filter and a read of its
/// index block. That is cheap once and ruinous per lookup, so readers are cached for the
/// lifetime of the table. Since a finished SSTable is immutable, a cached reader can never go
/// stale — it is only ever discarded when compaction makes the whole file obsolete.
/// </para>
/// <para>
/// <see cref="Lazy{T}"/> with <see cref="LazyThreadSafetyMode.ExecutionAndPublication"/> handles
/// the thundering herd: when several readers miss on the same table at once, exactly one opens
/// it and the rest wait for that result, instead of all opening the same file independently.
/// </para>
/// </remarks>
internal sealed class TableCache(string directory, BlockCache? blockCache) : IDisposable
{
    private readonly ConcurrentDictionary<ulong, Lazy<SsTableReader>> _readers = new();

    /// <summary>Open readers currently held.</summary>
    internal int Count => _readers.Count;

    /// <summary>Returns a reader for a table, opening it on first use.</summary>
    internal SsTableReader Get(ulong fileNumber)
    {
        var lazy = _readers.GetOrAdd(
            fileNumber,
            number => new Lazy<SsTableReader>(
                () => SsTableReader.Open(
                    Path.Combine(directory, SsTableMeta.FileNameFor(number)),
                    number,
                    blockCache),
                LazyThreadSafetyMode.ExecutionAndPublication));

        try
        {
            return lazy.Value;
        }
        catch
        {
            // A failed open must not be cached, or the failure becomes permanent for the
            // process even after the cause is fixed.
            _readers.TryRemove(fileNumber, out _);
            throw;
        }
    }

    /// <summary>Closes a table's reader and drops its cached blocks.</summary>
    internal void Evict(ulong fileNumber)
    {
        if (_readers.TryRemove(fileNumber, out var lazy) && lazy.IsValueCreated)
        {
            lazy.Value.Dispose();
        }

        blockCache?.EvictFile(fileNumber);
    }

    public void Dispose()
    {
        foreach (var (_, lazy) in _readers)
        {
            if (lazy.IsValueCreated) lazy.Value.Dispose();
        }
        _readers.Clear();
    }
}
