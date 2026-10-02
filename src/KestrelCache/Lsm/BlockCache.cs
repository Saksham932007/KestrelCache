namespace KestrelCache.Lsm;

/// <summary>
/// A size-bounded LRU cache of decompressed SSTable blocks, shared by every reader.
/// </summary>
/// <remarks>
/// <para>
/// The cache holds blocks <i>after</i> decompression, which is the point: a block that is read
/// repeatedly would otherwise be decompressed on every lookup even when the OS page cache has
/// the compressed bytes in memory, and for a hot block the decompression dominates the read.
/// </para>
/// <para>
/// LRU is the right policy here because SSTable access has strong temporal locality: index
/// blocks and the blocks holding frequently-read keys are touched constantly, while a
/// compaction streams through everything exactly once. (A pure LRU is in fact vulnerable to that
/// second pattern — a large scan can evict the whole working set — which is why production
/// engines reach for scan-resistant policies like S3-FIFO or ARC. Compactions here read through
/// their own iterators and bypass the cache entirely, which sidesteps the problem without
/// needing a cleverer policy.)
/// </para>
/// <para>
/// Keying on (file number, offset) rather than on a file path keeps entries valid across a
/// rename and makes eviction on table deletion a simple prefix sweep. File numbers are never
/// reused, so a stale entry can never be mistaken for a live one.
/// </para>
/// </remarks>
internal sealed class BlockCache(long capacityBytes)
{
    private readonly record struct Key(ulong FileNumber, long Offset);

    private sealed class Entry(Key key, byte[] block)
    {
        internal Key CacheKey { get; } = key;
        internal byte[] Block { get; } = block;
        internal int Size => Block.Length;
    }

    private readonly Dictionary<Key, LinkedListNode<Entry>> _index = [];

    // Most-recently-used at the front. A linked list is used rather than, say, a heap because
    // both operations the policy needs -- move to front on a hit, drop from the back on evict --
    // are O(1) on a list given the node, and the dictionary hands us the node.
    private readonly LinkedList<Entry> _order = new();

    private readonly object _gate = new();

    private long _bytes;
    private long _hits;
    private long _misses;

    /// <summary>Configured capacity in bytes.</summary>
    internal long CapacityBytes { get; } = Math.Max(0, capacityBytes);

    /// <summary>Bytes currently held.</summary>
    internal long SizeBytes
    {
        get { lock (_gate) return _bytes; }
    }

    /// <summary>Blocks currently held.</summary>
    internal int Count
    {
        get { lock (_gate) return _index.Count; }
    }

    /// <summary>Cache hits.</summary>
    internal long Hits => Interlocked.Read(ref _hits);

    /// <summary>Cache misses.</summary>
    internal long Misses => Interlocked.Read(ref _misses);

    /// <summary>Looks a block up, promoting it to most-recently-used on a hit.</summary>
    internal bool TryGet(ulong fileNumber, long offset, out byte[]? block)
    {
        if (CapacityBytes == 0)
        {
            block = null;
            Interlocked.Increment(ref _misses);
            return false;
        }

        var key = new Key(fileNumber, offset);

        lock (_gate)
        {
            if (_index.TryGetValue(key, out var node))
            {
                _order.Remove(node);
                _order.AddFirst(node);
                block = node.Value.Block;
                Interlocked.Increment(ref _hits);
                return true;
            }
        }

        block = null;
        Interlocked.Increment(ref _misses);
        return false;
    }

    /// <summary>Inserts a block, evicting from the back until it fits.</summary>
    internal void Put(ulong fileNumber, long offset, byte[] block)
    {
        if (CapacityBytes == 0) return;

        // A single block larger than the whole cache would evict everything and then itself.
        if (block.Length > CapacityBytes) return;

        var key = new Key(fileNumber, offset);

        lock (_gate)
        {
            if (_index.TryGetValue(key, out var existing))
            {
                _order.Remove(existing);
                _bytes -= existing.Value.Size;
                _index.Remove(key);
            }

            var node = _order.AddFirst(new Entry(key, block));
            _index[key] = node;
            _bytes += block.Length;

            while (_bytes > CapacityBytes && _order.Last is { } last)
            {
                _order.RemoveLast();
                _index.Remove(last.Value.CacheKey);
                _bytes -= last.Value.Size;
            }
        }
    }

    /// <summary>
    /// Drops every block belonging to a table, called when compaction makes one obsolete so the
    /// cache does not hold dead bytes until they age out naturally.
    /// </summary>
    internal void EvictFile(ulong fileNumber)
    {
        lock (_gate)
        {
            var doomed = new List<LinkedListNode<Entry>>();
            foreach (var (key, node) in _index)
            {
                if (key.FileNumber == fileNumber) doomed.Add(node);
            }

            foreach (var node in doomed)
            {
                _order.Remove(node);
                _index.Remove(node.Value.CacheKey);
                _bytes -= node.Value.Size;
            }
        }
    }

    /// <summary>Empties the cache.</summary>
    internal void Clear()
    {
        lock (_gate)
        {
            _index.Clear();
            _order.Clear();
            _bytes = 0;
        }
    }
}
