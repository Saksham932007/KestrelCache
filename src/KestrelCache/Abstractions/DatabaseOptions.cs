namespace KestrelCache;

/// <summary>Tunables shared by every storage engine.</summary>
public sealed record DatabaseOptions
{
    /// <summary>Directory (LSM) or file (Bitcask) backing the database.</summary>
    public required string Path { get; init; }

    /// <summary>Durability policy for acknowledged writes. Defaults to bounded-loss interval syncing.</summary>
    public SyncPolicy SyncPolicy { get; init; } = SyncPolicy.Interval;

    /// <summary>How often to fsync when <see cref="SyncPolicy.Interval"/> is in effect.</summary>
    public TimeSpan SyncInterval { get; init; } = TimeSpan.FromMilliseconds(200);

    /// <summary>
    /// Largest accepted key, in bytes. Also the bound used to reject implausible length fields
    /// when replaying a damaged file, which is what keeps a corrupt header from triggering a
    /// multi-gigabyte allocation.
    /// </summary>
    public int MaxKeySize { get; init; } = 64 * 1024;

    /// <summary>Largest accepted value, in bytes. Doubles as a corruption bound, as with <see cref="MaxKeySize"/>.</summary>
    public int MaxValueSize { get; init; } = 64 * 1024 * 1024;

    /// <summary>
    /// When true, a truncated or corrupt record at the <i>tail</i> of the log is treated as a
    /// partially-completed write: the log is truncated back to the last good record and recovery
    /// continues. When false, any damage raises <see cref="CorruptRecordException"/>.
    /// </summary>
    /// <remarks>
    /// A torn tail record is the expected outcome of crashing mid-append, so tolerating it is
    /// correct rather than lax. Damage in the <i>middle</i> of the log is never tolerated under
    /// either setting, because that indicates real corruption rather than an interrupted write.
    /// </remarks>
    public bool TruncateCorruptTail { get; init; } = true;

    /// <summary>
    /// Stale-byte fraction at which the Bitcask engine compacts automatically. Zero disables
    /// automatic compaction. Defaults to 0.5, i.e. compact once half the file is garbage.
    /// </summary>
    public double AutoCompactStaleRatio { get; init; } = 0.5;

    /// <summary>
    /// Floor below which automatic Bitcask compaction is skipped regardless of stale ratio.
    /// A small file is cheap to carry and expensive to rewrite repeatedly.
    /// </summary>
    public long AutoCompactMinBytes { get; init; } = 4 * 1024 * 1024;

    /// <summary>Size a memtable may reach before it is frozen and flushed to an SSTable (LSM only).</summary>
    public long MemtableSizeBytes { get; init; } = 4 * 1024 * 1024;

    /// <summary>
    /// Size a compaction output file is rolled at. Smaller files make each compaction cheaper and
    /// more incremental; larger ones mean fewer files to track and fewer index blocks to hold
    /// (LSM only).
    /// </summary>
    public long TargetFileSizeBytes { get; init; } = 8 * 1024 * 1024;

    /// <summary>Uncompressed size of an SSTable data block (LSM only).</summary>
    public int BlockSizeBytes { get; init; } = 4 * 1024;

    /// <summary>
    /// Whether SSTable data blocks are compressed. Blocks that do not shrink by at least an
    /// eighth are stored verbatim regardless (LSM only).
    /// </summary>
    public bool CompressBlocks { get; init; } = true;

    /// <summary>Bits of Bloom filter per key. 10 bits gives a ~1% false-positive rate (LSM only).</summary>
    public int BloomBitsPerKey { get; init; } = 10;

    /// <summary>Capacity of the decompressed-block cache, in bytes (LSM only).</summary>
    public long BlockCacheBytes { get; init; } = 32 * 1024 * 1024;

    /// <summary>Number of SSTables at level 0 that triggers a compaction (LSM only).</summary>
    public int Level0CompactionTrigger { get; init; } = 4;

    /// <summary>Size multiplier between adjacent LSM levels. 10 is the conventional choice.</summary>
    public int LevelSizeMultiplier { get; init; } = 10;

    /// <summary>Target total size of level 1, in bytes (LSM only).</summary>
    public long BaseLevelSizeBytes { get; init; } = 16 * 1024 * 1024;

    /// <summary>When false, background compaction must be driven manually. Useful for deterministic tests.</summary>
    public bool EnableBackgroundCompaction { get; init; } = true;

    internal void Validate()
    {
        if (string.IsNullOrWhiteSpace(Path))
            throw new ArgumentException("Path must be a non-empty path.", nameof(Path));
        if (MaxKeySize <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxKeySize), "MaxKeySize must be positive.");
        if (MaxValueSize < 0)
            throw new ArgumentOutOfRangeException(nameof(MaxValueSize), "MaxValueSize cannot be negative.");
        if (MemtableSizeBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(MemtableSizeBytes), "MemtableSizeBytes must be positive.");
        if (TargetFileSizeBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(TargetFileSizeBytes), "TargetFileSizeBytes must be positive.");
        if (BlockSizeBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(BlockSizeBytes), "BlockSizeBytes must be positive.");
        if (BloomBitsPerKey < 0)
            throw new ArgumentOutOfRangeException(nameof(BloomBitsPerKey), "BloomBitsPerKey cannot be negative.");
        if (Level0CompactionTrigger <= 0)
            throw new ArgumentOutOfRangeException(nameof(Level0CompactionTrigger), "Level0CompactionTrigger must be positive.");
        if (LevelSizeMultiplier < 2)
            throw new ArgumentOutOfRangeException(nameof(LevelSizeMultiplier), "LevelSizeMultiplier must be at least 2.");
        if (AutoCompactStaleRatio is < 0 or > 1)
            throw new ArgumentOutOfRangeException(nameof(AutoCompactStaleRatio), "AutoCompactStaleRatio must be in [0, 1].");
        if (AutoCompactMinBytes < 0)
            throw new ArgumentOutOfRangeException(nameof(AutoCompactMinBytes), "AutoCompactMinBytes cannot be negative.");
        if (SyncPolicy == SyncPolicy.Interval && SyncInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(SyncInterval), "SyncInterval must be positive.");
    }
}
