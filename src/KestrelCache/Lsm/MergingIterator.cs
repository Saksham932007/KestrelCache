namespace KestrelCache.Lsm;

/// <summary>
/// Merges several sorted cursors into one, yielding entries in global internal-key order.
/// </summary>
/// <remarks>
/// <para>
/// This is the component that makes an LSM tree readable as a single sorted map despite its data
/// being spread across a memtable and many immutable files. Every input is already sorted, so a
/// k-way merge needs only to repeatedly take the smallest head — which is exactly what a
/// min-heap does in O(log k) per entry.
/// </para>
/// <para>
/// Both range scans and compaction run on this. For a scan the inputs are the memtables plus
/// every table that overlaps the requested range; for a compaction they are the tables being
/// merged. In both cases the ordering guarantee from <see cref="InternalKey.Comparer"/> does the
/// heavy lifting: because equal user keys sort newest-first, the first entry the merge emits for
/// a user key is the current one, and every subsequent entry for that key is provably stale.
/// </para>
/// <para>
/// Ties on the <i>same</i> internal key cannot occur, since sequence numbers are unique. Ties on
/// the same user key are broken by sequence, which the comparator already handles, so the heap
/// needs no secondary tiebreak.
/// </para>
/// </remarks>
internal sealed class MergingIterator : IEntryIterator
{
    private readonly List<IEntryIterator> _inputs;
    private readonly PriorityQueue<int, byte[]> _heap;
    private readonly bool _ownsInputs;

    private int _current = -1;

    internal MergingIterator(IEnumerable<IEntryIterator> inputs, bool ownsInputs = true)
    {
        _inputs = [.. inputs];
        _ownsInputs = ownsInputs;
        _heap = new PriorityQueue<int, byte[]>(_inputs.Count, InternalKey.Comparer.Instance);
    }

    /// <summary>Number of merged inputs.</summary>
    internal int InputCount => _inputs.Count;

    /// <inheritdoc />
    public bool IsValid => _current >= 0;

    /// <inheritdoc />
    public ReadOnlySpan<byte> Key => _inputs[_current].Key;

    /// <inheritdoc />
    public ReadOnlySpan<byte> Value => _inputs[_current].Value;

    /// <inheritdoc />
    public void SeekToFirst()
    {
        _heap.Clear();
        for (int i = 0; i < _inputs.Count; i++)
        {
            _inputs[i].SeekToFirst();
            if (_inputs[i].IsValid)
            {
                _heap.Enqueue(i, _inputs[i].Key.ToArray());
            }
        }
        TakeSmallest();
    }

    /// <inheritdoc />
    public void Seek(ReadOnlySpan<byte> internalKey)
    {
        byte[] target = internalKey.ToArray();
        _heap.Clear();

        for (int i = 0; i < _inputs.Count; i++)
        {
            _inputs[i].Seek(target);
            if (_inputs[i].IsValid)
            {
                _heap.Enqueue(i, _inputs[i].Key.ToArray());
            }
        }
        TakeSmallest();
    }

    /// <inheritdoc />
    public bool MoveNext()
    {
        if (_current < 0) return false;

        // Advance the input the last entry came from and re-enqueue it at its new position.
        var input = _inputs[_current];
        if (input.MoveNext() && input.IsValid)
        {
            _heap.Enqueue(_current, input.Key.ToArray());
        }

        _current = -1;
        TakeSmallest();
        return IsValid;
    }

    private void TakeSmallest()
    {
        if (_heap.TryDequeue(out int index, out _))
        {
            _current = index;
        }
        else
        {
            _current = -1;
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (!_ownsInputs) return;
        foreach (var input in _inputs)
        {
            input.Dispose();
        }
    }
}

/// <summary>
/// Collapses a merged stream down to the one visible version of each user key, dropping stale
/// versions and tombstones.
/// </summary>
/// <remarks>
/// A raw merge yields every version of every key that still exists anywhere in the tree. A
/// reader wants one entry per key — the newest the snapshot can see — and wants deleted keys to
/// disappear rather than surfacing as tombstones. Separating that logic from the merge itself
/// keeps compaction able to use the raw stream, since compaction has to see tombstones in order
/// to decide whether it is allowed to drop them.
/// </remarks>
internal static class VisibleEntries
{
    /// <summary>
    /// Streams the live user-key/value pairs visible at <paramref name="snapshotSequence"/>
    /// within the half-open range <c>[start, end)</c>.
    /// </summary>
    internal static IEnumerable<KeyValuePair<byte[], byte[]>> Enumerate(
        IEntryIterator merged,
        ulong snapshotSequence,
        byte[]? start,
        byte[]? end)
    {
        if (start is null)
        {
            merged.SeekToFirst();
        }
        else
        {
            merged.Seek(InternalKey.Encode(
                start, InternalKey.ClampSnapshot(snapshotSequence), ValueKind.Value));
        }

        byte[]? lastUserKey = null;

        while (merged.IsValid)
        {
            var internalKey = merged.Key;

            if (!InternalKey.IsWellFormed(internalKey))
            {
                if (!merged.MoveNext()) break;
                continue;
            }

            var userKey = InternalKey.UserKey(internalKey);

            if (end is not null && userKey.SequenceCompareTo(end) >= 0)
            {
                break;
            }

            bool sameAsLast = lastUserKey is not null && userKey.SequenceEqual(lastUserKey);

            if (!sameAsLast
                && InternalKey.Sequence(internalKey) <= snapshotSequence)
            {
                // First entry for this user key that the snapshot can see, so it is the current
                // one. Everything after it for the same key is older by construction.
                lastUserKey = userKey.ToArray();

                if (InternalKey.Kind(internalKey) == ValueKind.Value)
                {
                    yield return new KeyValuePair<byte[], byte[]>(lastUserKey, merged.Value.ToArray());
                }

                // A tombstone yields nothing but still marks the key as resolved, so older
                // versions underneath it stay hidden.
            }

            if (!merged.MoveNext()) break;
        }
    }
}
