namespace KestrelCache.Lsm;

/// <summary>The outcome of looking a user key up in a memtable.</summary>
internal enum LookupResult
{
    /// <summary>This memtable holds no entry for the key; keep searching older data.</summary>
    NotFound,

    /// <summary>A live value was found; stop searching.</summary>
    Found,

    /// <summary>
    /// A tombstone was found. The key is deleted, and the search must stop here rather than
    /// continue into older levels — the whole purpose of a tombstone is to shadow what is
    /// underneath it.
    /// </summary>
    Deleted,
}

/// <summary>
/// The in-memory write buffer: a sorted collection of recent writes, which becomes an immutable
/// SSTable once it grows past a threshold.
/// </summary>
/// <remarks>
/// <para>
/// The memtable is what lets an LSM tree turn random writes into sequential ones. A write goes
/// to the write-ahead log (one sequential append, for durability) and then into this sorted
/// in-memory structure. Nothing is written to the main data files at all until the memtable
/// fills, at which point its entire contents are already in sorted order and can be dumped to a
/// new file in one sequential pass. A B-tree, by contrast, must seek to the page owning each key
/// and write it there, so a random-key workload becomes random I/O.
/// </para>
/// <para>
/// That is the fundamental trade the whole design makes: writes become cheap and sequential, and
/// in exchange a read may have to look in several places, which is what Bloom filters and
/// compaction then exist to contain.
/// </para>
/// </remarks>
internal sealed class Memtable
{
    private readonly SkipList _entries;
    private long _approximateBytes;

    internal Memtable(ulong logNumber)
    {
        LogNumber = logNumber;
        _entries = new SkipList(InternalKey.Comparer.Instance);
    }

    /// <summary>
    /// The write-ahead log file this memtable's contents were logged to. It cannot be deleted
    /// until this memtable has been flushed to an SSTable and that fact recorded durably.
    /// </summary>
    internal ulong LogNumber { get; }

    /// <summary>Entries held, counting each version of a key separately.</summary>
    internal int Count => _entries.Count;

    /// <summary>
    /// Roughly how much memory the entries occupy, used to decide when to flush. An estimate is
    /// sufficient: the threshold is a tuning knob, not a correctness boundary.
    /// </summary>
    internal long ApproximateBytes => Volatile.Read(ref _approximateBytes);

    /// <summary>
    /// Adds an entry. Callers must serialise with each other; see <see cref="SkipList"/>.
    /// </summary>
    internal void Add(ulong sequence, ValueKind kind, ReadOnlySpan<byte> userKey, ReadOnlySpan<byte> value)
    {
        byte[] internalKey = InternalKey.Encode(userKey, sequence, kind);
        byte[] stored = value.IsEmpty ? [] : value.ToArray();

        _entries.Insert(internalKey, stored);

        // Node overhead is a guess, but a consistent one: two references, the tower, and the
        // two arrays' headers.
        Interlocked.Add(ref _approximateBytes, internalKey.Length + stored.Length + 64);
    }

    /// <summary>
    /// Looks up the newest entry for <paramref name="userKey"/> at or below
    /// <paramref name="snapshotSequence"/>.
    /// </summary>
    internal LookupResult TryGet(
        ReadOnlySpan<byte> userKey,
        ulong snapshotSequence,
        out byte[]? value)
    {
        value = null;

        // Seeking to (userKey, snapshotSequence) lands on the newest entry for that key that the
        // snapshot is allowed to see, because equal user keys sort newest-first and anything
        // newer than the snapshot sorts before this probe.
        byte[] probe = InternalKey.Encode(
            userKey, InternalKey.ClampSnapshot(snapshotSequence), ValueKind.Value);
        var node = _entries.Seek(probe);

        if (node?.Key is null) return LookupResult.NotFound;

        if (!InternalKey.UserKey(node.Key).SequenceEqual(userKey))
        {
            // Landed on a different user key entirely, so this key is absent here.
            return LookupResult.NotFound;
        }

        if (InternalKey.Kind(node.Key) == ValueKind.Deletion)
        {
            return LookupResult.Deleted;
        }

        value = node.Value ?? [];
        return LookupResult.Found;
    }

    /// <summary>Enumerates every entry in internal-key order, newest version of each key first.</summary>
    internal IEnumerable<MemtableEntry> Scan(byte[]? startInternalKey = null)
    {
        foreach (var node in _entries.From(startInternalKey))
        {
            if (node.Key is null) continue;
            yield return new MemtableEntry(node.Key, node.Value ?? []);
        }
    }

    /// <summary>An iterator over this memtable, for merging with SSTable iterators.</summary>
    internal IEntryIterator CreateIterator() => new MemtableIterator(this);

    private sealed class MemtableIterator(Memtable memtable) : IEntryIterator
    {
        private IEnumerator<MemtableEntry>? _enumerator;
        private MemtableEntry _current;
        private bool _valid;

        public bool IsValid => _valid;

        public ReadOnlySpan<byte> Key => _current.InternalKey;

        public ReadOnlySpan<byte> Value => _current.Value;

        public void SeekToFirst() => Reset(startInternalKey: null);

        public void Seek(ReadOnlySpan<byte> internalKey) => Reset(internalKey.ToArray());

        public bool MoveNext()
        {
            if (_enumerator is null)
            {
                SeekToFirst();
                return _valid;
            }

            _valid = _enumerator.MoveNext();
            if (_valid) _current = _enumerator.Current;
            return _valid;
        }

        private void Reset(byte[]? startInternalKey)
        {
            _enumerator = memtable.Scan(startInternalKey).GetEnumerator();
            _valid = _enumerator.MoveNext();
            if (_valid) _current = _enumerator.Current;
        }

        public void Dispose() => _enumerator?.Dispose();
    }
}

/// <summary>One entry as stored: an internal key and its value.</summary>
internal readonly record struct MemtableEntry(byte[] InternalKey, byte[] Value);
