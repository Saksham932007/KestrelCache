namespace KestrelCache.Lsm;

/// <summary>
/// A forward cursor over internal-key-ordered entries, implemented by memtables, SSTables and
/// the merging iterator that combines them.
/// </summary>
/// <remarks>
/// The interface is deliberately span-based and pull-driven rather than
/// <see cref="IEnumerable{T}"/>: a compaction merges several million entries and allocating a
/// key-value pair object per entry would make the GC, not the disk, the bottleneck. Exposing
/// the current entry as a span into a buffer the iterator already owns keeps the merge
/// allocation-free in the steady state.
/// </remarks>
internal interface IEntryIterator : IDisposable
{
    /// <summary>True when the cursor is positioned on an entry.</summary>
    bool IsValid { get; }

    /// <summary>
    /// The current entry's internal key. Only valid when <see cref="IsValid"/>.
    /// </summary>
    /// <remarks>
    /// Named <c>Key</c> rather than <c>InternalKey</c> so that implementations can refer to the
    /// <see cref="KestrelCache.Lsm.InternalKey"/> helper class without the member shadowing it.
    /// </remarks>
    ReadOnlySpan<byte> Key { get; }

    /// <summary>The current entry's value. Only valid when <see cref="IsValid"/>.</summary>
    ReadOnlySpan<byte> Value { get; }

    /// <summary>Positions the cursor on the first entry.</summary>
    void SeekToFirst();

    /// <summary>Positions the cursor on the first entry at or after <paramref name="internalKey"/>.</summary>
    void Seek(ReadOnlySpan<byte> internalKey);

    /// <summary>Advances the cursor, returning false once it runs off the end.</summary>
    bool MoveNext();
}
