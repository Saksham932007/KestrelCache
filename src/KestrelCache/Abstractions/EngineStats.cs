namespace KestrelCache;

/// <summary>
/// A point-in-time snapshot of engine counters, for metrics endpoints and benchmark reporting.
/// </summary>
public sealed record EngineStats
{
    /// <summary>Which engine produced these numbers.</summary>
    public required string Engine { get; init; }

    /// <summary>
    /// Keys the engine is tracking. Exact only when <see cref="KeyCountIsExact"/> is true.
    /// </summary>
    /// <remarks>
    /// The two engines genuinely cannot answer this question with the same precision, and
    /// reporting an estimate as though it were a count would be worse than admitting the
    /// difference. Bitcask holds every live key in a hash index, so its count is exact and free.
    /// An LSM tree holds every <i>version</i> of every key, spread across a memtable and many
    /// immutable tables, and it has no idea how many of them are superseded without merging them
    /// — so its number is an upper bound that counts stale versions and tombstones too, and
    /// drops toward the true figure as compaction proceeds. Getting an exact count means
    /// scanning, which is what <c>ScanAsync</c> is for.
    /// </remarks>
    public long KeyCount { get; init; }

    /// <summary>Whether <see cref="KeyCount"/> is an exact live-key count or an upper bound.</summary>
    public bool KeyCountIsExact { get; init; }

    /// <summary>Bytes occupied on disk by all files the engine owns.</summary>
    public long DiskSizeBytes { get; init; }

    /// <summary>Bytes of user key+value data still live, ignoring stale and deleted records.</summary>
    public long LiveDataBytes { get; init; }

    /// <summary>Completed read operations.</summary>
    public long Reads { get; init; }

    /// <summary>Completed write operations (a batch counts once per key).</summary>
    public long Writes { get; init; }

    /// <summary>Completed delete operations.</summary>
    public long Deletes { get; init; }

    /// <summary>fsync calls issued.</summary>
    public long Syncs { get; init; }

    /// <summary>Compactions (Bitcask: file rewrites; LSM: merge jobs) completed.</summary>
    public long Compactions { get; init; }

    /// <summary>Bytes rewritten by compaction. Together with <see cref="Writes"/> this gives write amplification.</summary>
    public long CompactionBytesWritten { get; init; }

    /// <summary>Reads served without touching the disk because a Bloom filter ruled the key out.</summary>
    public long BloomFilterNegatives { get; init; }

    /// <summary>Bloom filter said "maybe" but the key was absent — the false-positive count.</summary>
    public long BloomFilterFalsePositives { get; init; }

    /// <summary>Block cache hits.</summary>
    public long BlockCacheHits { get; init; }

    /// <summary>Block cache misses.</summary>
    public long BlockCacheMisses { get; init; }

    /// <summary>Per-level SSTable counts, index 0 being level 0 (LSM only).</summary>
    public IReadOnlyList<int> SsTablesPerLevel { get; init; } = [];

    /// <summary>
    /// Fraction of on-disk bytes that are stale (superseded or deleted). Drives compaction
    /// decisions in the Bitcask engine and is the headline space-amplification number.
    /// </summary>
    public double StaleRatio => DiskSizeBytes == 0 ? 0 : 1.0 - ((double)LiveDataBytes / DiskSizeBytes);

    /// <summary>Block cache hit rate in [0, 1].</summary>
    public double BlockCacheHitRate =>
        BlockCacheHits + BlockCacheMisses == 0
            ? 0
            : (double)BlockCacheHits / (BlockCacheHits + BlockCacheMisses);
}
