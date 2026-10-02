using System.Collections.Concurrent;
using KestrelCache.Lsm;
using Xunit;
using Xunit.Abstractions;

namespace KestrelCache.Tests;

/// <summary>
/// Tests for the LSM engine's pieces in isolation: the skip list's concurrency guarantee, the
/// write-ahead log's framing, internal-key ordering, and the compaction planner's rules.
/// </summary>
public sealed class LsmComponentTests(ITestOutputHelper output)
{
    // ------------------------------------------------------------ internal keys

    [Fact]
    public void An_internal_key_round_trips_its_user_key_sequence_and_kind()
    {
        byte[] encoded = InternalKey.Encode(ByteKey.From("user:1"), 12_345, ValueKind.Deletion);

        Assert.Equal("user:1", ByteKey.ToString(InternalKey.UserKey(encoded)));
        Assert.Equal(12_345UL, InternalKey.Sequence(encoded));
        Assert.Equal(ValueKind.Deletion, InternalKey.Kind(encoded));
    }

    /// <summary>
    /// The ordering the whole engine rests on: user key ascending, sequence descending. The
    /// descending half is what makes the newest version of a key the first one any scan or
    /// lookup encounters.
    /// </summary>
    [Fact]
    public void Internal_keys_sort_by_user_key_then_newest_sequence_first()
    {
        var keys = new[]
        {
            InternalKey.Encode(ByteKey.From("b"), 1, ValueKind.Value),
            InternalKey.Encode(ByteKey.From("a"), 1, ValueKind.Value),
            InternalKey.Encode(ByteKey.From("a"), 9, ValueKind.Value),
            InternalKey.Encode(ByteKey.From("a"), 5, ValueKind.Value),
        };

        Array.Sort(keys, InternalKey.Comparer.Instance);

        Assert.Equal(
            [("a", 9UL), ("a", 5UL), ("a", 1UL), ("b", 1UL)],
            keys.Select(k => (ByteKey.ToString(InternalKey.UserKey(k)), InternalKey.Sequence(k))));
    }

    [Fact]
    public void A_sequence_beyond_the_encodable_range_is_rejected()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => InternalKey.Encode(ByteKey.From("k"), InternalKey.MaxSequence + 1, ValueKind.Value));
    }

    // ------------------------------------------------------------ skip list

    [Fact]
    public void The_skip_list_keeps_entries_in_order()
    {
        var list = new SkipList(ByteKeyComparer.Instance);

        foreach (int i in Enumerable.Range(0, 1_000).OrderBy(_ => Random.Shared.Next()))
        {
            list.Insert(ByteKey.From($"key:{i:D6}"), ByteKey.From($"v{i}"));
        }

        var keys = list.From(null).Select(node => ByteKey.ToString(node.Key!)).ToList();

        Assert.Equal(1_000, keys.Count);
        Assert.Equal(keys.OrderBy(k => k, StringComparer.Ordinal), keys);
    }

    [Fact]
    public void The_skip_list_seeks_to_the_first_key_at_or_after_a_target()
    {
        var list = new SkipList(ByteKeyComparer.Instance);

        for (int i = 0; i < 100; i++)
        {
            list.Insert(ByteKey.From($"key:{i * 10:D5}"), []);
        }

        Assert.Equal("key:00500", ByteKey.ToString(list.Seek(ByteKey.From("key:00500"))!.Key!));
        Assert.Equal("key:00510", ByteKey.ToString(list.Seek(ByteKey.From("key:00501"))!.Key!));
        Assert.Null(list.Seek(ByteKey.From("zzz")));
    }

    [Fact]
    public void The_skip_list_refuses_a_duplicate_key()
    {
        var list = new SkipList(ByteKeyComparer.Instance);
        list.Insert(ByteKey.From("k"), []);

        // Duplicate internal keys would mean a sequence number was reused, which is an invariant
        // violation upstream rather than something to paper over.
        Assert.Throws<InvalidOperationException>(() => list.Insert(ByteKey.From("k"), []));
    }

    /// <summary>
    /// The skip list's reason for existing: one writer and many readers, with no locks on the
    /// read path. A reader may miss a concurrent insert, but it must never see a half-built node,
    /// crash, or return a key that was never inserted.
    /// </summary>
    [Fact]
    public async Task Readers_traverse_the_skip_list_safely_while_a_writer_inserts()
    {
        var list = new SkipList(ByteKeyComparer.Instance);
        const int total = 20_000;

        using var stop = new CancellationTokenSource();
        var failures = new ConcurrentBag<string>();

        var readers = Enumerable.Range(0, 4).Select(id => Task.Run(() =>
        {
            byte[]? previous = null;
            long traversals = 0;

            while (!stop.IsCancellationRequested)
            {
                previous = null;

                foreach (var node in list.From(null))
                {
                    byte[] key = node.Key!;

                    // Ordering must hold at every instant, not merely once writing stops.
                    if (previous is not null
                        && ByteKeyComparer.Instance.Compare(previous, key) >= 0)
                    {
                        failures.Add(
                            $"reader {id} saw {ByteKey.ToString(key)} after "
                                + $"{ByteKey.ToString(previous)}");
                        return;
                    }

                    // Every node must be fully initialised: a published-before-written node
                    // would show up here as a null or mismatched value.
                    string expected = $"v:{ByteKey.ToString(key)}";
                    if (node.Value is null || ByteKey.ToString(node.Value) != expected)
                    {
                        failures.Add($"reader {id} saw a partially initialised node for "
                            + ByteKey.ToString(key));
                        return;
                    }

                    previous = key;
                }

                traversals++;
            }

            output.WriteLine($"reader {id} completed {traversals:N0} full traversals");
        })).ToArray();

        var writer = Task.Run(() =>
        {
            for (int i = 0; i < total; i++)
            {
                byte[] key = ByteKey.From($"key:{i:D8}");
                list.Insert(key, ByteKey.From($"v:key:{i:D8}"));
            }
        });

        await writer;
        await stop.CancelAsync();
        await Task.WhenAll(readers);

        Assert.Empty(failures);
        Assert.Equal(total, list.Count);
    }

    // ------------------------------------------------------------ write-ahead log

    [Fact]
    public async Task The_log_replays_what_was_written()
    {
        using var dir = new TempDirectory();
        var options = new DatabaseOptions { Path = dir.Path };

        var log = WriteAheadLog.Create(dir.Path, 1);
        await log.AppendAsync(1, [
            new WriteOp { Key = ByteKey.From("a"), Value = ByteKey.From("1") },
            new WriteOp { Key = ByteKey.From("b"), Value = ByteKey.From("2") },
        ]);
        await log.AppendAsync(3, [
            new WriteOp { Key = ByteKey.From("a"), Value = null },
        ]);
        log.Sync();
        await log.DisposeAsync();

        var replayed = new List<LogEntry>();
        var result = WriteAheadLog.Replay(log.Path, replayed.Add, options);

        Assert.Equal(2, result.Records);
        Assert.Equal(3, result.Entries);
        Assert.Equal(3UL, result.LastSequence);
        Assert.Equal(0, result.BytesTruncated);

        Assert.Equal(["a", "b", "a"], replayed.Select(e => ByteKey.ToString(e.Key)));
        Assert.Equal([1UL, 2UL, 3UL], replayed.Select(e => e.Sequence));
        Assert.Equal(ValueKind.Deletion, replayed[2].Kind);
    }

    /// <summary>
    /// Because a whole batch occupies one checksummed record, a crash mid-append can only
    /// destroy that record — never half of a batch.
    /// </summary>
    [Fact]
    public async Task A_torn_log_tail_discards_the_whole_unfinished_batch()
    {
        using var dir = new TempDirectory();
        var options = new DatabaseOptions { Path = dir.Path };

        var log = WriteAheadLog.Create(dir.Path, 1);
        await log.AppendAsync(1, [
            new WriteOp { Key = ByteKey.From("committed"), Value = ByteKey.From("yes") },
        ]);

        var bigBatch = Enumerable.Range(0, 20)
            .Select(i => new WriteOp { Key = ByteKey.From($"torn:{i}"), Value = new byte[64] })
            .ToList();
        await log.AppendAsync(2, bigBatch);
        log.Sync();
        string path = log.Path;
        await log.DisposeAsync();

        // Lop off the end of the second record, as an interrupted write would.
        using (var stream = new FileStream(path, FileMode.Open, FileAccess.Write))
        {
            stream.SetLength(stream.Length - 100);
        }

        var replayed = new List<LogEntry>();
        var result = WriteAheadLog.Replay(path, replayed.Add, options);

        output.WriteLine(
            $"replayed {result.Records} record(s), discarded {result.BytesTruncated} byte(s): "
                + result.StopReason);

        // The first batch survives whole; the second vanishes entirely, not partially.
        Assert.Equal(1, result.Records);
        Assert.Single(replayed);
        Assert.Equal("committed", ByteKey.ToString(replayed[0].Key));
        Assert.True(result.BytesTruncated > 0);
    }

    [Fact]
    public async Task A_corrupt_log_record_is_detected_by_its_checksum()
    {
        using var dir = new TempDirectory();
        var options = new DatabaseOptions { Path = dir.Path };

        var log = WriteAheadLog.Create(dir.Path, 1);
        await log.AppendAsync(1, [
            new WriteOp { Key = ByteKey.From("k"), Value = ByteKey.From("v") },
        ]);
        log.Sync();
        string path = log.Path;
        await log.DisposeAsync();

        using (var stream = new FileStream(path, FileMode.Open, FileAccess.ReadWrite))
        {
            stream.Position = 10;
            int original = stream.ReadByte();
            stream.Position = 10;
            stream.WriteByte((byte)(original ^ 0xFF));
        }

        var replayed = new List<LogEntry>();
        var result = WriteAheadLog.Replay(path, replayed.Add, options);

        Assert.Empty(replayed);
        Assert.Equal("checksum mismatch", result.StopReason);
    }

    // ------------------------------------------------------------ compaction planner

    [Fact]
    public void The_planner_leaves_a_healthy_tree_alone()
    {
        var options = new DatabaseOptions { Path = "/unused", Level0CompactionTrigger = 4 };
        var version = new LsmVersion([[Table(1, "a", "b")]]);

        Assert.Null(CompactionPlanner.Plan(version, options));
    }

    [Fact]
    public void The_planner_compacts_level_zero_once_its_file_count_reaches_the_trigger()
    {
        var options = new DatabaseOptions { Path = "/unused", Level0CompactionTrigger = 3 };

        var version = new LsmVersion([[
            Table(1, "a", "m"),
            Table(2, "b", "n"),
            Table(3, "c", "o"),
            Table(4, "d", "p"),
        ]]);

        var job = CompactionPlanner.Plan(version, options);

        Assert.NotNull(job);
        Assert.Equal(0, job.SourceLevel);
        Assert.Equal(1, job.TargetLevel);

        // All of level 0 at once: its tables overlap each other, so compacting a subset would
        // leave a key's newer and older versions at the same level with no ordering between them.
        Assert.Equal(4, job.SourceTables.Count);
    }

    [Fact]
    public void Level_targets_grow_by_the_configured_multiplier()
    {
        var options = new DatabaseOptions
        {
            Path = "/unused",
            BaseLevelSizeBytes = 1_000,
            LevelSizeMultiplier = 10,
        };

        Assert.Equal(1_000, CompactionPlanner.TargetSizeFor(1, options));
        Assert.Equal(10_000, CompactionPlanner.TargetSizeFor(2, options));
        Assert.Equal(100_000, CompactionPlanner.TargetSizeFor(3, options));
    }

    [Fact]
    public void A_compaction_pulls_in_the_overlapping_tables_from_the_level_below()
    {
        var options = new DatabaseOptions { Path = "/unused", Level0CompactionTrigger = 1 };

        var version = new LsmVersion([
            [Table(10, "d", "f")],
            [Table(1, "a", "c"), Table(2, "e", "g"), Table(3, "h", "k")],
        ]);

        var job = CompactionPlanner.Plan(version, options);

        Assert.NotNull(job);

        // Only the level-1 table whose range intersects [d, f] is rewritten. Pulling in the
        // others would turn an incremental compaction into a full rewrite of the level.
        Assert.Equal([2UL], job.TargetTables.Select(t => t.FileNumber));
    }

    /// <summary>
    /// The rule that stops deleted data coming back to life.
    /// </summary>
    /// <remarks>
    /// A tombstone may only be discarded once no deeper level could still hold the value it is
    /// shadowing. Drop it too early and the older value below becomes the newest surviving
    /// version of that key, so a deleted record silently reappears.
    /// </remarks>
    [Fact]
    public void A_tombstone_cannot_be_dropped_while_a_deeper_level_could_hold_the_key()
    {
        var version = new LsmVersion([
            [],
            [Table(1, "a", "z")],
            [Table(2, "a", "z")], // level 2 covers the same keys
        ]);

        Assert.False(
            CompactionPlanner.CanDropTombstone(
                version, targetLevel: 1, ByteKey.From("m"), oldestSnapshot: 100, tombstoneSequence: 50),
            "a tombstone was dropped although level 2 could still hold the value it shadows");

        // Compacting into the deepest level, there is nothing below to resurrect.
        Assert.True(
            CompactionPlanner.CanDropTombstone(
                version, targetLevel: 2, ByteKey.From("m"), oldestSnapshot: 100, tombstoneSequence: 50));
    }

    [Fact]
    public void A_tombstone_cannot_be_dropped_while_a_snapshot_predates_it()
    {
        var version = new LsmVersion([[], [Table(1, "a", "z")]]);

        // A snapshot taken at 10 must still see the value the deletion at 50 hid.
        Assert.False(
            CompactionPlanner.CanDropTombstone(
                version, targetLevel: 1, ByteKey.From("m"), oldestSnapshot: 10, tombstoneSequence: 50));

        Assert.True(
            CompactionPlanner.CanDropTombstone(
                version, targetLevel: 1, ByteKey.From("m"), oldestSnapshot: 60, tombstoneSequence: 50));
    }

    [Fact]
    public void A_tombstone_can_be_dropped_when_no_deeper_level_covers_its_key()
    {
        var version = new LsmVersion([
            [],
            [Table(1, "a", "z")],
            [Table(2, "x", "z")], // level 2 holds a different key range
        ]);

        Assert.True(
            CompactionPlanner.CanDropTombstone(
                version, targetLevel: 1, ByteKey.From("m"), oldestSnapshot: 100, tombstoneSequence: 50));
    }

    // ------------------------------------------------------------ version metadata

    [Fact]
    public void Deeper_levels_are_sorted_so_a_lookup_can_binary_search_them()
    {
        var version = new LsmVersion([
            [],
            [Table(3, "m", "p"), Table(1, "a", "c"), Table(2, "e", "g")],
        ]);

        Assert.Equal(
            ["a", "e", "m"],
            version.Level(1).Select(t => ByteKey.ToString(t.SmallestUserKey)));
    }

    [Fact]
    public void Level_zero_is_ordered_newest_file_first()
    {
        var version = new LsmVersion([[Table(1, "a", "z"), Table(5, "a", "z"), Table(3, "a", "z")]]);

        Assert.Equal([5UL, 3UL, 1UL], version.Level(0).Select(t => t.FileNumber));
    }

    [Fact]
    public void A_lookup_consults_only_the_one_table_per_deep_level_that_could_hold_the_key()
    {
        var version = new LsmVersion([
            [Table(10, "a", "z")], // level 0 overlaps everything, so it is always consulted
            [Table(1, "a", "c"), Table(2, "e", "g"), Table(3, "m", "p")],
        ]);

        var consulted = version.TablesFor(ByteKey.From("f")).Select(t => t.FileNumber).ToList();

        Assert.Equal([10UL, 2UL], consulted);
    }

    [Fact]
    public void A_key_outside_every_range_consults_nothing()
    {
        var version = new LsmVersion([
            [Table(10, "m", "p")],
            [Table(1, "a", "c")],
        ]);

        Assert.Empty(version.TablesFor(ByteKey.From("zzz")));
    }

    private static SsTableMeta Table(ulong fileNumber, string smallest, string largest) => new()
    {
        FileNumber = fileNumber,
        FileSizeBytes = 1_024,
        SmallestKey = InternalKey.Encode(ByteKey.From(smallest), 1, ValueKind.Value),
        LargestKey = InternalKey.Encode(ByteKey.From(largest), 1, ValueKind.Value),
        EntryCount = 10,
    };
}
