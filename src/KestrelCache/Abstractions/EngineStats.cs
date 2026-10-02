namespace KestrelCache;

/// <summary>
/// A point-in-time snapshot of engine counters, for metrics endpoints and benchmark reporting.
/// </summary>
public sealed record EngineStats
{
    /// <summary>Which engine produced these numbers.</summary>
    public required string Engine { get; init; }

    /// <summary>Live keys currently visible to readers.</summary>
    public long KeyCount { get; init; }

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
