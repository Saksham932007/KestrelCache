namespace KestrelCache.Lsm;

/// <summary>
/// An immutable snapshot of which SSTables make up the tree, reference-counted so that readers
/// using it are never disturbed by a compaction.
/// </summary>
/// <remarks>
/// <para>
/// Compaction and memtable flushes change the <i>set</i> of live tables, but a read in flight
/// has already decided which tables it is going to consult. Mutating a shared list of tables
/// underneath it would be a race with no good outcome: the reader could miss a key that moved
/// from one level to another, or follow a reference to a file that has just been deleted.
/// </para>
/// <para>
/// Making the table set immutable removes the race entirely. Every change produces a new version
/// rather than editing the old one, and a reader atomically grabs the current version and works
/// within it from start to finish. Files stay on disk until no live version mentions them, which
/// is exactly the condition under which no reader can still want them.
/// </para>
/// <para><b>Why levels have different shapes</b></para>
/// <para>
/// Level 0 tables come straight from memtable flushes, so their key ranges overlap freely — two
/// consecutive flushes both contain writes to the same hot key. A lookup must therefore check
/// every level-0 table, newest first. Every deeper level is maintained by compaction so that its
/// tables have <i>disjoint</i> key ranges, which means a lookup there is a binary search that
/// reads at most one table. That difference is why level 0 is kept small and why its file count
/// is what triggers compaction.
/// </para>
/// </remarks>
internal sealed class LsmVersion
{
    private int _refs = 1;

    internal LsmVersion(IReadOnlyList<IReadOnlyList<SsTableMeta>> levels)
    {
        Levels = Normalise(levels);
    }

    /// <summary>An empty tree.</summary>
    internal static LsmVersion Empty { get; } = new([]);

    /// <summary>Tables per level, index 0 being level 0.</summary>
    internal IReadOnlyList<IReadOnlyList<SsTableMeta>> Levels { get; }

    /// <summary>Number of levels currently holding tables.</summary>
    internal int LevelCount => Levels.Count;

    /// <summary>Tables in a level, or an empty list when the level does not exist.</summary>
    internal IReadOnlyList<SsTableMeta> Level(int level) =>
        level >= 0 && level < Levels.Count ? Levels[level] : [];

    /// <summary>Total bytes held by a level.</summary>
    internal long LevelSizeBytes(int level) => Level(level).Sum(table => table.FileSizeBytes);

    /// <summary>Total bytes held by all levels.</summary>
    internal long TotalSizeBytes => Levels.Sum(level => level.Sum(table => table.FileSizeBytes));

    /// <summary>Total entries across all tables, counting every version of a key.</summary>
    internal long TotalEntryCount => Levels.Sum(level => level.Sum(table => table.EntryCount));

    /// <summary>Every file number this version references.</summary>
    internal IEnumerable<ulong> FileNumbers =>
        Levels.SelectMany(level => level).Select(table => table.FileNumber);

    /// <summary>Tables per level, for stats reporting.</summary>
    internal IReadOnlyList<int> TableCountsPerLevel => [.. Levels.Select(level => level.Count)];

    /// <summary>
    /// Returns the tables that could hold <paramref name="userKey"/>, in the order they must be
    /// consulted: newest data first, so the first hit is the answer.
    /// </summary>
    internal IEnumerable<SsTableMeta> TablesFor(byte[] userKey)
    {
        // Level 0 overlaps, so every table whose range covers the key must be checked, newest
        // file number first because a higher number means a later flush.
        foreach (var table in Level(0))
        {
            if (table.CouldContain(userKey)) yield return table;
        }

        for (int level = 1; level < Levels.Count; level++)
        {
            var tables = Levels[level];
            if (tables.Count == 0) continue;

            // Disjoint and sorted, so at most one table can hold the key.
            int index = FindTable(tables, userKey);
            if (index >= 0 && tables[index].CouldContain(userKey))
            {
                yield return tables[index];
            }
        }
    }

    /// <summary>
    /// Binary search for the first table whose largest user key is at or above
    /// <paramref name="userKey"/>. Valid only for levels with disjoint ranges.
    /// </summary>
    private static int FindTable(IReadOnlyList<SsTableMeta> tables, byte[] userKey)
    {
        int low = 0;
        int high = tables.Count - 1;
        int found = -1;

        while (low <= high)
        {
            int mid = low + ((high - low) / 2);
            if (tables[mid].LargestUserKey.SequenceCompareTo(userKey) >= 0)
            {
                found = mid;
                high = mid - 1;
            }
            else
            {
                low = mid + 1;
            }
        }

        return found;
    }

    /// <summary>Tables in <paramref name="level"/> whose ranges overlap <c>[start, end)</c>.</summary>
    internal IReadOnlyList<SsTableMeta> Overlapping(int level, byte[]? start, byte[]? end) =>
        [.. Level(level).Where(table => table.OverlapsRange(start, end))];

    /// <summary>Tables in <paramref name="level"/> whose ranges overlap any of the given tables.</summary>
    internal IReadOnlyList<SsTableMeta> Overlapping(int level, IReadOnlyList<SsTableMeta> tables)
    {
        if (tables.Count == 0) return [];

        byte[] start = tables.MinBy(t => t.SmallestUserKey.ToArray(), ByteKeyComparer.Instance)!
            .SmallestUserKey.ToArray();
        byte[] end = tables.MaxBy(t => t.LargestUserKey.ToArray(), ByteKeyComparer.Instance)!
            .LargestUserKey.ToArray();

        // Inclusive of `end`, so the exclusive-bound overload cannot be reused directly.
        return
        [
            .. Level(level).Where(table =>
                table.LargestUserKey.SequenceCompareTo(start) >= 0
                && table.SmallestUserKey.SequenceCompareTo(end) <= 0),
        ];
    }

    /// <summary>Produces a new version with <paramref name="added"/> inserted and <paramref name="removed"/> gone.</summary>
    internal LsmVersion With(
        int level,
        IReadOnlyList<SsTableMeta> added,
        IReadOnlyList<SsTableMeta> removed)
    {
        var removedNumbers = removed.Select(t => t.FileNumber).ToHashSet();
        int depth = Math.Max(Levels.Count, level + 1);
        var levels = new List<IReadOnlyList<SsTableMeta>>(depth);

        for (int i = 0; i < depth; i++)
        {
            var tables = Level(i).Where(t => !removedNumbers.Contains(t.FileNumber)).ToList();
            if (i == level) tables.AddRange(added);
            levels.Add(tables);
        }

        return new LsmVersion(levels);
    }

    /// <summary>
    /// Produces a new version with tables removed from one level and added to another, which is
    /// what a compaction does.
    /// </summary>
    internal LsmVersion WithCompaction(
        int sourceLevel,
        IReadOnlyList<SsTableMeta> sourceRemoved,
        int targetLevel,
        IReadOnlyList<SsTableMeta> targetRemoved,
        IReadOnlyList<SsTableMeta> targetAdded)
    {
        var removedNumbers = sourceRemoved.Concat(targetRemoved)
            .Select(t => t.FileNumber)
            .ToHashSet();

        int depth = Math.Max(Levels.Count, Math.Max(sourceLevel, targetLevel) + 1);
        var levels = new List<IReadOnlyList<SsTableMeta>>(depth);

        for (int i = 0; i < depth; i++)
        {
            var tables = Level(i).Where(t => !removedNumbers.Contains(t.FileNumber)).ToList();
            if (i == targetLevel) tables.AddRange(targetAdded);
            levels.Add(tables);
        }

        return new LsmVersion(levels);
    }

    /// <summary>
    /// Imposes each level's required ordering: level 0 newest-file-first, deeper levels sorted
    /// by key so they can be binary searched.
    /// </summary>
    private static IReadOnlyList<IReadOnlyList<SsTableMeta>> Normalise(
        IReadOnlyList<IReadOnlyList<SsTableMeta>> levels)
    {
        var result = new List<IReadOnlyList<SsTableMeta>>(levels.Count);

        for (int level = 0; level < levels.Count; level++)
        {
            var tables = levels[level].ToList();

            if (level == 0)
            {
                tables.Sort((a, b) => b.FileNumber.CompareTo(a.FileNumber));
            }
            else
            {
                tables.Sort((a, b) =>
                {
                    int byKey = a.SmallestUserKey.SequenceCompareTo(b.SmallestUserKey);
                    return byKey != 0 ? byKey : a.FileNumber.CompareTo(b.FileNumber);
                });
            }

            result.Add(tables);
        }

        // Drop trailing empty levels so LevelCount means something.
        while (result.Count > 0 && result[^1].Count == 0)
        {
            result.RemoveAt(result.Count - 1);
        }

        return result;
    }

    /// <summary>
    /// Takes a reference, returning false if this version has already been retired — in which
    /// case the caller should re-read the engine's current version.
    /// </summary>
    internal bool TryAddRef()
    {
        int current = Volatile.Read(ref _refs);
        while (current > 0)
        {
            int previous = Interlocked.CompareExchange(ref _refs, current + 1, current);
            if (previous == current) return true;
            current = previous;
        }
        return false;
    }

    /// <summary>Drops a reference. Returns true when this was the last one.</summary>
    internal bool Release() => Interlocked.Decrement(ref _refs) == 0;

    /// <summary>True while anything still holds a reference.</summary>
    internal bool IsLive => Volatile.Read(ref _refs) > 0;

    /// <summary>Rebuilds a version from a manifest.</summary>
    internal static LsmVersion FromManifest(ManifestState state) =>
        new([.. state.Levels.Select(level =>
            (IReadOnlyList<SsTableMeta>)[.. level.Select(record => record.ToMeta())])]);
}
