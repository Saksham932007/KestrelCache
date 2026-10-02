namespace KestrelCache.Lsm;

/// <summary>
/// Everything the engine needs to know about an SSTable without opening it.
/// </summary>
/// <remarks>
/// <para>
/// This is what the manifest stores, and keeping the key range here rather than inside the file
/// is what makes level lookups cheap. Within any level below zero, tables have disjoint key
/// ranges, so answering "which file could hold key K" is a binary search over this metadata in
/// memory — no file is opened, no block is read, and a read that misses every level costs
/// nothing but comparisons.
/// </para>
/// <para>
/// The sequence range matters for compaction: an entry can only be dropped when nothing older
/// could still need it, which requires knowing the sequence numbers a table spans.
/// </para>
/// </remarks>
internal sealed record SsTableMeta
{
    /// <summary>Monotonic file number; the filename is derived from it.</summary>
    public required ulong FileNumber { get; init; }

    /// <summary>Size on disk, used for level-size accounting.</summary>
    public required long FileSizeBytes { get; init; }

    /// <summary>Smallest internal key in the table.</summary>
    public required byte[] SmallestKey { get; init; }

    /// <summary>Largest internal key in the table.</summary>
    public required byte[] LargestKey { get; init; }

    /// <summary>Lowest sequence number present.</summary>
    public ulong SmallestSequence { get; init; }

    /// <summary>Highest sequence number present.</summary>
    public ulong LargestSequence { get; init; }

    /// <summary>Entries in the table, counting every version of a key.</summary>
    public long EntryCount { get; init; }

    /// <summary>Conventional filename for a table with this number.</summary>
    public string FileName => FileNameFor(FileNumber);

    /// <summary>Conventional filename for a given table number.</summary>
    public static string FileNameFor(ulong fileNumber) => $"{fileNumber:D6}.sst";

    /// <summary>The smallest user key in the table.</summary>
    public ReadOnlySpan<byte> SmallestUserKey => InternalKey.UserKey(SmallestKey);

    /// <summary>The largest user key in the table.</summary>
    public ReadOnlySpan<byte> LargestUserKey => InternalKey.UserKey(LargestKey);

    /// <summary>True when <paramref name="userKey"/> falls inside this table's key range.</summary>
    public bool CouldContain(ReadOnlySpan<byte> userKey) =>
        userKey.SequenceCompareTo(SmallestUserKey) >= 0
        && userKey.SequenceCompareTo(LargestUserKey) <= 0;

    /// <summary>
    /// True when this table's user-key range overlaps the half-open range
    /// <c>[start, end)</c>. A null bound is unbounded on that side.
    /// </summary>
    public bool OverlapsRange(byte[]? start, byte[]? end)
    {
        if (start is not null && LargestUserKey.SequenceCompareTo(start) < 0) return false;
        if (end is not null && SmallestUserKey.SequenceCompareTo(end) >= 0) return false;
        return true;
    }

    /// <summary>True when the two tables' user-key ranges intersect.</summary>
    public bool Overlaps(SsTableMeta other) =>
        LargestUserKey.SequenceCompareTo(other.SmallestUserKey) >= 0
        && SmallestUserKey.SequenceCompareTo(other.LargestUserKey) <= 0;
}
