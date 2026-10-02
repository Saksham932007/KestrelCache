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

    /// <summary>Total bytes occupied on disk by every file the engine owns.</summary>
    public long DiskSizeBytes { get; init; }

    /// <summary>
    /// Bytes of user key+value data still live. Exact only when <see cref="KeyCountIsExact"/>
    /// is true; see <see cref="StaleRatio"/>.
    /// </summary>
    public long LiveDataBytes { get; init; }

    /// <summary>
    /// Bytes held in the main data files: the log for Bitcask, the SSTables for the LSM engine.
    /// </summary>
    public long DataFileBytes { get; init; }

    /// <summary>
    /// Bytes held in the write-ahead log, which the LSM engine keeps separately from its data
    /// files. Zero for Bitcask, whose log <i>is</i> its data file.
    /// </summary>
    public long WriteAheadLogBytes { get; init; }

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
    /// Fraction of the <i>data files</i> that is stale — superseded values and tombstones. This
    /// is the headline space-amplification number and what drives the Bitcask engine's
    /// compaction decisions.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Two details here were wrong in an earlier version and are worth stating explicitly.
    /// </para>
    /// <para>
    /// First, the denominator is <see cref="DataFileBytes"/> and not
    /// <see cref="DiskSizeBytes"/>. Including the write-ahead log would count it as garbage,
    /// which it is not — it is the durability record for data that has not yet been written to a
    /// table. A freshly written LSM database reported a stale ratio of 100% for precisely that
    /// reason, which is alarming and meaningless.
    /// </para>
    /// <para>
    /// Second, this returns zero when <see cref="KeyCountIsExact"/> is false, because the LSM
    /// engine cannot measure live bytes without merging its levels. Reporting a computed-looking
    /// number from an unknown numerator would be worse than reporting nothing;
    /// <c>SsTablesPerLevel</c> and <c>CompactionBytesWritten</c> are the signals to watch there.
    /// </para>
    /// </remarks>
    public double StaleRatio =>
        !KeyCountIsExact || DataFileBytes == 0
            ? 0
            : Math.Clamp(1.0 - ((double)LiveDataBytes / DataFileBytes), 0, 1);

    /// <summary>Block cache hit rate in [0, 1].</summary>
    public double BlockCacheHitRate =>
        BlockCacheHits + BlockCacheMisses == 0
            ? 0
            : (double)BlockCacheHits / (BlockCacheHits + BlockCacheMisses);
}
