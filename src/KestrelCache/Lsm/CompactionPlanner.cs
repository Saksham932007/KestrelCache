namespace KestrelCache.Lsm;

/// <summary>A compaction the planner has decided to run.</summary>
internal sealed record CompactionJob
{
    /// <summary>Level the inputs are taken from.</summary>
    public required int SourceLevel { get; init; }

    /// <summary>Level the output is written to, always <see cref="SourceLevel"/> + 1.</summary>
    public int TargetLevel => SourceLevel + 1;

    /// <summary>Tables being merged out of the source level.</summary>
    public required IReadOnlyList<SsTableMeta> SourceTables { get; init; }

    /// <summary>Tables in the target level whose key ranges overlap the inputs.</summary>
    public required IReadOnlyList<SsTableMeta> TargetTables { get; init; }

    /// <summary>Why this job was chosen, for logging and tests.</summary>
    public required string Reason { get; init; }

    /// <summary>Total input bytes, which is the read volume the job will incur.</summary>
    public long InputBytes =>
        SourceTables.Sum(t => t.FileSizeBytes) + TargetTables.Sum(t => t.FileSizeBytes);

    /// <summary>Every input table, source and target together.</summary>
    public IEnumerable<SsTableMeta> AllInputs => SourceTables.Concat(TargetTables);
}

/// <summary>
/// Decides which compaction, if any, is most worth running.
/// </summary>
/// <remarks>
/// <para>
/// Compaction is the price an LSM tree pays for cheap writes, and the planner is where that
/// price gets controlled. Without it, writes accumulate as an ever-growing pile of overlapping
/// tables: reads slow down because every lookup has more places to check, and the disk fills
/// with superseded values and tombstones that nothing will ever read.
/// </para>
/// <para><b>Why levelled, and not tiered</b></para>
/// <para>
/// The two standard strategies trade the same three quantities in opposite directions:
/// </para>
/// <list type="bullet">
/// <item>
/// <b>Levelled</b> (this implementation) keeps each level's tables non-overlapping, so a read
/// touches at most one table per level. Read and space amplification are low; write
/// amplification is higher, because moving a table down a level means rewriting the overlapping
/// tables beneath it — roughly the level multiplier's worth of extra writing per level.
/// </item>
/// <item>
/// <b>Tiered</b> accumulates same-size tables and merges them only when several have piled up.
/// Write amplification is much lower, but a read may have to check every table in a level, and
/// a key's old versions survive longer, so space amplification is worse.
/// </item>
/// </list>
/// <para>
/// Levelled is chosen here because this engine's reason for existing alongside the Bitcask one
/// is to make reads and scans cheap at scale. Choosing tiered would improve the number that
/// Bitcask already wins on and worsen the one it loses on.
/// </para>
/// <para><b>The two triggers</b></para>
/// <para>
/// Level 0 is scored by <i>file count</i>, not by size, because its tables overlap: four tables
/// at level 0 means four lookups for every read, whatever they weigh. Deeper levels are scored
/// by total size against a target that grows by the level multiplier, which is what makes the
/// tree's depth logarithmic in the data volume and bounds how many levels a read must consult.
/// </para>
/// </remarks>
internal static class CompactionPlanner
{
    /// <summary>
    /// Returns the highest-scoring compaction, or <c>null</c> when the tree is in good shape.
    /// </summary>
    internal static CompactionJob? Plan(LsmVersion version, DatabaseOptions options)
    {
        int bestLevel = -1;

        // A score of 1.0 means a level is exactly at its budget, which is the point at which it
        // should be compacted -- not one file or one byte past it. Candidates must therefore
        // score at or above 1.0, and among those the worst offender wins.
        const double threshold = 1.0;
        double bestScore = 0;
        string reason = string.Empty;

        // Level 0: scored by file count.
        int level0Count = version.Level(0).Count;
        if (level0Count > 0)
        {
            double score = (double)level0Count / options.Level0CompactionTrigger;
            if (score >= threshold && score > bestScore)
            {
                bestScore = score;
                bestLevel = 0;
                reason = $"level 0 holds {level0Count} file(s), trigger is {options.Level0CompactionTrigger}";
            }
        }

        // Deeper levels: scored by bytes against an exponentially growing target.
        for (int level = 1; level < version.LevelCount; level++)
        {
            long size = version.LevelSizeBytes(level);
            long target = TargetSizeFor(level, options);
            double score = (double)size / target;

            if (score >= threshold && score > bestScore)
            {
                bestScore = score;
                bestLevel = level;
                reason = $"level {level} holds {size:N0} B against a {target:N0} B target";
            }
        }

        if (bestLevel < 0) return null;

        var sourceTables = SelectSourceTables(version, bestLevel, options);
        if (sourceTables.Count == 0) return null;

        var targetTables = version.Overlapping(bestLevel + 1, sourceTables);

        return new CompactionJob
        {
            SourceLevel = bestLevel,
            SourceTables = sourceTables,
            TargetTables = targetTables,
            Reason = reason,
        };
    }

    /// <summary>
    /// Target total size for a level. Level 1 gets <see cref="DatabaseOptions.BaseLevelSizeBytes"/>
    /// and each level below is <see cref="DatabaseOptions.LevelSizeMultiplier"/> times larger.
    /// </summary>
    /// <remarks>
    /// The conventional multiplier is 10, and the reason is a genuine optimum rather than a
    /// round number: total write amplification is roughly the multiplier times the number of
    /// levels, and the number of levels is <c>log_multiplier(dataSize)</c>. A larger multiplier
    /// means fewer levels but more rewriting at each one; a smaller one the reverse. The product
    /// is flat-bottomed around 10, so the choice is forgiving but the shape of the trade is
    /// real.
    /// </remarks>
    internal static long TargetSizeFor(int level, DatabaseOptions options)
    {
        if (level <= 0) return options.BaseLevelSizeBytes;

        long target = options.BaseLevelSizeBytes;
        for (int i = 1; i < level; i++)
        {
            target *= options.LevelSizeMultiplier;

            // Guard against overflow on absurdly deep trees.
            if (target > long.MaxValue / options.LevelSizeMultiplier) break;
        }
        return target;
    }

    private static IReadOnlyList<SsTableMeta> SelectSourceTables(
        LsmVersion version,
        int level,
        DatabaseOptions options)
    {
        var tables = version.Level(level);
        if (tables.Count == 0) return [];

        if (level == 0)
        {
            // Level-0 tables overlap each other, so they have to be compacted as a group: merging
            // only some of them would leave a key's newer version at level 0 and its older one at
            // level 1, with no guarantee the reader checks them in the right order.
            return tables;
        }

        // Deeper levels: take one table and let it carry its overlapping neighbours with it.
        // Picking the oldest file number rather than the largest table spreads compaction work
        // evenly across the keyspace over time instead of repeatedly rewriting one hot region.
        var chosen = tables.MinBy(t => t.FileNumber)!;
        return [chosen];
    }

    /// <summary>
    /// Whether a key's older versions can be discarded during a compaction.
    /// </summary>
    /// <remarks>
    /// This is the subtlest rule in the engine. A tombstone cannot simply be dropped when
    /// encountered: if an older value for the same key survives in a deeper level, removing the
    /// tombstone would resurrect it. The tombstone may only go when the compaction output lands
    /// in the deepest level that could hold such a value — that is, when no level below the
    /// target contains the key's range. Getting this wrong produces deleted data coming back to
    /// life, which is why the condition is isolated here and tested directly.
    /// </remarks>
    internal static bool CanDropTombstone(
        LsmVersion version,
        int targetLevel,
        ReadOnlySpan<byte> userKey,
        ulong oldestSnapshot,
        ulong tombstoneSequence)
    {
        // A snapshot older than the deletion must still be able to see the value it hid.
        if (tombstoneSequence > oldestSnapshot) return false;

        for (int level = targetLevel + 1; level < version.LevelCount; level++)
        {
            foreach (var table in version.Level(level))
            {
                if (table.CouldContain(userKey)) return false;
            }
        }

        return true;
    }
}
