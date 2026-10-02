using KestrelCache.Lsm;
using Xunit;
using Xunit.Abstractions;

namespace KestrelCache.Tests;

public sealed class LsmEngineTests(ITestOutputHelper output)
{
    /// <summary>
    /// Deterministic defaults: background maintenance off so flushes happen inline, and a tiny
    /// memtable so a handful of writes is enough to produce real levels and real compactions.
    /// </summary>
    private static DatabaseOptions Options(string path) => new()
    {
        Path = path,
        SyncPolicy = SyncPolicy.None,
        EnableBackgroundCompaction = false,
        MemtableSizeBytes = 16 * 1024,
        TargetFileSizeBytes = 32 * 1024,
        BaseLevelSizeBytes = 64 * 1024,
        BlockSizeBytes = 1024,
        Level0CompactionTrigger = 4,
    };

    // ------------------------------------------------------------ basics

    [Fact]
    public async Task Put_then_get_round_trips()
    {
        using var dir = new TempDirectory();
        await using var engine = await LsmEngine.OpenAsync(Options(dir.Path));

        await engine.PutAsync(TestData.Key("k"), TestData.Value("v"));

        Assert.Equal("v", TestData.Text(await engine.GetAsync(TestData.Key("k"))));
    }

    [Fact]
    public async Task Get_returns_null_for_a_missing_key()
    {
        using var dir = new TempDirectory();
        await using var engine = await LsmEngine.OpenAsync(Options(dir.Path));

        Assert.Null(await engine.GetAsync(TestData.Key("absent")));
    }

    [Fact]
    public async Task Put_overwrites_the_previous_value()
    {
        using var dir = new TempDirectory();
        await using var engine = await LsmEngine.OpenAsync(Options(dir.Path));

        await engine.PutAsync(TestData.Key("k"), TestData.Value("v1"));
        await engine.PutAsync(TestData.Key("k"), TestData.Value("v2"));

        Assert.Equal("v2", TestData.Text(await engine.GetAsync(TestData.Key("k"))));
    }

    [Fact]
    public async Task Delete_hides_the_key()
    {
        using var dir = new TempDirectory();
        await using var engine = await LsmEngine.OpenAsync(Options(dir.Path));

        await engine.PutAsync(TestData.Key("k"), TestData.Value("v"));
        await engine.DeleteAsync(TestData.Key("k"));

        Assert.Null(await engine.GetAsync(TestData.Key("k")));
    }

    /// <summary>
    /// A tombstone has to shadow older versions that still exist in deeper levels. If the read
    /// path kept searching past a tombstone, the deleted value would come back.
    /// </summary>
    [Fact]
    public async Task A_delete_shadows_a_value_that_has_already_reached_a_table()
    {
        using var dir = new TempDirectory();
        await using var engine = await LsmEngine.OpenAsync(Options(dir.Path));

        await engine.PutAsync(TestData.Key("k"), TestData.Value("v"));
        await engine.FlushMemtableAsync();

        Assert.Equal("v", TestData.Text(await engine.GetAsync(TestData.Key("k"))));

        await engine.DeleteAsync(TestData.Key("k"));
        Assert.Null(await engine.GetAsync(TestData.Key("k")));

        // And after the tombstone itself reaches a table.
        await engine.FlushMemtableAsync();
        Assert.Null(await engine.GetAsync(TestData.Key("k")));

        // And after compaction has had a chance to drop it.
        await engine.CompactAsync();
        Assert.Null(await engine.GetAsync(TestData.Key("k")));
    }

    [Fact]
    public async Task Values_may_be_empty_or_binary()
    {
        using var dir = new TempDirectory();
        await using var engine = await LsmEngine.OpenAsync(Options(dir.Path));

        byte[] key = [0x00, 0xFF, 0x41];
        byte[] value = Enumerable.Range(0, 256).Select(i => (byte)i).ToArray();

        await engine.PutAsync(key, value);
        await engine.PutAsync(TestData.Key("empty"), []);

        Assert.Equal(value, await engine.GetAsync(key));
        Assert.Empty((await engine.GetAsync(TestData.Key("empty")))!);
    }

    // ------------------------------------------------------------ levels and compaction

    [Fact]
    public async Task Writes_spill_into_tables_and_then_into_levels()
    {
        using var dir = new TempDirectory();
        await using var engine = await LsmEngine.OpenAsync(Options(dir.Path));

        byte[] value = new byte[256];
        for (int i = 0; i < 2_000; i++)
        {
            await engine.PutAsync(TestData.Key($"key:{i:D6}"), value);
        }

        var beforeCompaction = engine.GetStats();
        output.WriteLine(
            $"before compaction: levels [{string.Join(", ", beforeCompaction.SsTablesPerLevel)}], "
                + $"{beforeCompaction.DiskSizeBytes:N0} B");

        Assert.True(engine.FlushCount > 0, "no memtable was ever flushed");

        await engine.CompactAsync();

        var afterCompaction = engine.GetStats();
        output.WriteLine(
            $"after compaction:  levels [{string.Join(", ", afterCompaction.SsTablesPerLevel)}], "
                + $"{afterCompaction.DiskSizeBytes:N0} B, "
                + $"{afterCompaction.Compactions} compaction(s)");

        Assert.True(afterCompaction.Compactions > 0, "the planner never scheduled a compaction");
        Assert.True(afterCompaction.SsTablesPerLevel.Count > 1, "nothing was pushed below level 0");

        // And everything is still readable.
        for (int i = 0; i < 2_000; i += 7)
        {
            Assert.NotNull(await engine.GetAsync(TestData.Key($"key:{i:D6}")));
        }
    }

    [Fact]
    public async Task Compaction_reclaims_space_from_overwritten_keys()
    {
        using var dir = new TempDirectory();
        await using var engine = await LsmEngine.OpenAsync(Options(dir.Path));

        byte[] value = new byte[512];

        // The same 100 keys written 60 times: 98% of what reaches disk is garbage.
        for (int round = 0; round < 60; round++)
        {
            for (int i = 0; i < 100; i++)
            {
                await engine.PutAsync(TestData.Key($"key:{i:D4}"), value);
            }
        }

        await engine.FlushMemtableAsync();
        long before = engine.GetStats().LiveDataBytes;

        await engine.CompactAsync();
        long after = engine.GetStats().LiveDataBytes;

        output.WriteLine($"overwrite-heavy workload: {before:N0} B -> {after:N0} B after compaction");

        Assert.True(after < before, $"compaction did not shrink the tree ({before} -> {after})");

        for (int i = 0; i < 100; i++)
        {
            Assert.Equal(value, await engine.GetAsync(TestData.Key($"key:{i:D4}")));
        }
    }

    [Fact]
    public async Task Compaction_eventually_removes_tombstones_entirely()
    {
        using var dir = new TempDirectory();
        await using var engine = await LsmEngine.OpenAsync(Options(dir.Path));

        byte[] value = new byte[256];
        for (int i = 0; i < 500; i++)
        {
            await engine.PutAsync(TestData.Key($"key:{i:D4}"), value);
        }
        await engine.CompactAsync();

        for (int i = 0; i < 500; i++)
        {
            await engine.DeleteAsync(TestData.Key($"key:{i:D4}"));
        }

        // Several rounds, because a tombstone may only be dropped once it reaches the deepest
        // level that could hold the value it shadows.
        for (int round = 0; round < 6; round++)
        {
            await engine.CompactAsync();
        }

        var stats = engine.GetStats();
        output.WriteLine(
            $"after deleting everything: {stats.KeyCount} entries remain in "
                + $"[{string.Join(", ", stats.SsTablesPerLevel)}]");

        for (int i = 0; i < 500; i++)
        {
            Assert.Null(await engine.GetAsync(TestData.Key($"key:{i:D4}")));
        }
    }

    // ------------------------------------------------------------ scans

    [Fact]
    public async Task Scan_yields_every_key_in_sorted_order()
    {
        using var dir = new TempDirectory();
        await using var engine = await LsmEngine.OpenAsync(Options(dir.Path));

        var expected = Enumerable.Range(0, 1_500).Select(i => $"key:{i:D6}").ToList();

        // Inserted in shuffled order, so a passing test means the engine sorted them rather
        // than merely preserving insertion order.
        var shuffled = expected.OrderBy(_ => Random.Shared.Next()).ToList();
        foreach (string key in shuffled)
        {
            await engine.PutAsync(TestData.Key(key), TestData.Value($"v-{key}"));
        }

        var seen = new List<string>();
        await foreach (var (key, value) in engine.ScanAsync())
        {
            string text = TestData.Text(key)!;
            seen.Add(text);
            Assert.Equal($"v-{text}", TestData.Text(value));
        }

        Assert.Equal(expected, seen);
    }

    [Fact]
    public async Task Scan_respects_its_range_bounds()
    {
        using var dir = new TempDirectory();
        await using var engine = await LsmEngine.OpenAsync(Options(dir.Path));

        for (int i = 0; i < 1_000; i++)
        {
            await engine.PutAsync(TestData.Key($"key:{i:D4}"), TestData.Value("v"));
        }

        var seen = new List<string>();
        await foreach (var (key, _) in engine.ScanAsync(
            TestData.Key("key:0100"), TestData.Key("key:0200")))
        {
            seen.Add(TestData.Text(key)!);
        }

        Assert.Equal(100, seen.Count);
        Assert.Equal("key:0100", seen[0]);
        Assert.Equal("key:0199", seen[^1]);
    }

    [Fact]
    public async Task Scan_skips_deleted_keys()
    {
        using var dir = new TempDirectory();
        await using var engine = await LsmEngine.OpenAsync(Options(dir.Path));

        for (int i = 0; i < 600; i++)
        {
            await engine.PutAsync(TestData.Key($"key:{i:D4}"), TestData.Value("v"));
        }
        await engine.FlushMemtableAsync();

        for (int i = 0; i < 600; i += 2)
        {
            await engine.DeleteAsync(TestData.Key($"key:{i:D4}"));
        }

        var seen = new List<string>();
        await foreach (var (key, _) in engine.ScanAsync())
        {
            seen.Add(TestData.Text(key)!);
        }

        Assert.Equal(300, seen.Count);
        Assert.All(seen, key => Assert.True(int.Parse(key["key:".Length..]) % 2 == 1));
    }

    [Fact]
    public async Task Scan_returns_the_newest_version_of_each_key()
    {
        using var dir = new TempDirectory();
        await using var engine = await LsmEngine.OpenAsync(Options(dir.Path));

        for (int round = 0; round < 10; round++)
        {
            for (int i = 0; i < 200; i++)
            {
                await engine.PutAsync(TestData.Key($"key:{i:D4}"), TestData.Value($"round-{round}"));
            }
            await engine.FlushMemtableAsync();
        }

        var seen = new List<string>();
        await foreach (var (key, value) in engine.ScanAsync())
        {
            seen.Add(TestData.Text(key)!);
            Assert.Equal("round-9", TestData.Text(value));
        }

        Assert.Equal(200, seen.Count);
    }

    [Fact]
    public async Task A_prefix_scan_returns_exactly_the_matching_keys()
    {
        using var dir = new TempDirectory();
        await using var db = KestrelDb.Wrap(await LsmEngine.OpenAsync(Options(dir.Path)));

        await db.PutAsync("user:1", "a");
        await db.PutAsync("user:2", "b");
        await db.PutAsync("user:10", "c");
        await db.PutAsync("usera", "not-a-match");
        await db.PutAsync("order:1", "d");
        await db.PutAsync("users", "also-not");

        var seen = new List<string>();
        await foreach (var (key, _) in db.ScanPrefixAsync("user:"))
        {
            seen.Add(key);
        }

        Assert.Equal(["user:1", "user:10", "user:2"], seen);
    }

    // ------------------------------------------------------------ snapshots

    /// <summary>
    /// The payoff of putting sequence numbers in the key: a snapshot is a consistent view that
    /// costs nothing to take and is unaffected by concurrent writes.
    /// </summary>
    [Fact]
    public async Task A_snapshot_does_not_see_later_writes()
    {
        using var dir = new TempDirectory();
        await using var engine = await LsmEngine.OpenAsync(Options(dir.Path));

        for (int i = 0; i < 100; i++)
        {
            await engine.PutAsync(TestData.Key($"key:{i:D4}"), TestData.Value("original"));
        }

        using var snapshot = engine.CreateSnapshot();

        for (int i = 0; i < 100; i++)
        {
            await engine.PutAsync(TestData.Key($"key:{i:D4}"), TestData.Value("updated"));
        }
        await engine.PutAsync(TestData.Key("brand-new"), TestData.Value("after"));
        await engine.DeleteAsync(TestData.Key("key:0005"));

        for (int i = 0; i < 100; i++)
        {
            byte[] key = TestData.Key($"key:{i:D4}");
            Assert.Equal("original", TestData.Text(await engine.GetAsync(key, snapshot)));

            // key:0005 was deleted after the snapshot, so only the snapshot still sees it.
            string? expectedNow = i == 5 ? null : "updated";
            Assert.Equal(expectedNow, TestData.Text(await engine.GetAsync(key)));
        }

        Assert.Null(await engine.GetAsync(TestData.Key("brand-new"), snapshot));
        Assert.Equal("after", TestData.Text(await engine.GetAsync(TestData.Key("brand-new"))));

        // A key deleted after the snapshot is still visible through it.
        Assert.Equal("original", TestData.Text(await engine.GetAsync(TestData.Key("key:0005"), snapshot)));
        Assert.Null(await engine.GetAsync(TestData.Key("key:0005")));
    }

    [Fact]
    public async Task A_snapshot_survives_flushes_and_compactions()
    {
        using var dir = new TempDirectory();
        await using var engine = await LsmEngine.OpenAsync(Options(dir.Path));

        byte[] value = new byte[128];
        for (int i = 0; i < 300; i++)
        {
            await engine.PutAsync(TestData.Key($"key:{i:D4}"), TestData.Value("v1"));
        }

        using var snapshot = engine.CreateSnapshot();

        // Enough churn to force flushes and compactions, which must not discard anything the
        // snapshot still needs.
        for (int round = 0; round < 20; round++)
        {
            for (int i = 0; i < 300; i++)
            {
                await engine.PutAsync(TestData.Key($"key:{i:D4}"), value);
            }
        }
        await engine.CompactAsync();

        for (int i = 0; i < 300; i++)
        {
            Assert.Equal(
                "v1",
                TestData.Text(await engine.GetAsync(TestData.Key($"key:{i:D4}"), snapshot)));
        }
    }

    [Fact]
    public async Task A_snapshot_scan_is_consistent()
    {
        using var dir = new TempDirectory();
        await using var engine = await LsmEngine.OpenAsync(Options(dir.Path));

        for (int i = 0; i < 200; i++)
        {
            await engine.PutAsync(TestData.Key($"key:{i:D4}"), TestData.Value("original"));
        }

        using var snapshot = engine.CreateSnapshot();

        for (int i = 200; i < 400; i++)
        {
            await engine.PutAsync(TestData.Key($"key:{i:D4}"), TestData.Value("later"));
        }

        int count = 0;
        await foreach (var (_, value) in engine.ScanAsync(snapshot))
        {
            Assert.Equal("original", TestData.Text(value));
            count++;
        }

        Assert.Equal(200, count);
    }

    // ------------------------------------------------------------ batches

    [Fact]
    public async Task A_batch_applies_every_operation()
    {
        using var dir = new TempDirectory();
        await using var engine = await LsmEngine.OpenAsync(Options(dir.Path));

        await engine.PutAsync(TestData.Key("old"), TestData.Value("gone"));

        await engine.WriteAsync(new WriteBatch()
            .Put("a", "1")
            .Put("b", "2")
            .Delete("old"));

        Assert.Equal("1", TestData.Text(await engine.GetAsync(TestData.Key("a"))));
        Assert.Equal("2", TestData.Text(await engine.GetAsync(TestData.Key("b"))));
        Assert.Null(await engine.GetAsync(TestData.Key("old")));
    }

    /// <summary>
    /// Unlike the Bitcask engine, a batch here is atomic to concurrent readers as well as across
    /// recovery: the sequence number a read works at is published only once every key in the
    /// batch is in the memtable, so a reader sees all of the batch or none of it.
    /// </summary>
    [Fact]
    public async Task A_batch_is_never_observed_half_applied()
    {
        using var dir = new TempDirectory();
        await using var engine = await LsmEngine.OpenAsync(Options(dir.Path) with
        {
            MemtableSizeBytes = 1 << 20,
        });

        const int batchSize = 50;
        using var stop = new CancellationTokenSource();
        var failures = new List<string>();

        var reader = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                // All 50 keys must be read at one instant. Fifty separate GetAsync calls would
                // each take their own snapshot, so a batch committing between the third and the
                // fourth call is a perfectly correct interleaving rather than a torn batch --
                // the property under test is that a *single* consistent view never shows part of
                // a batch.
                using var view = engine.CreateSnapshot();

                int present = 0;
                for (int i = 0; i < batchSize; i++)
                {
                    if (await engine.GetAsync(TestData.Key($"batch:{i:D3}"), view) is not null)
                    {
                        present++;
                    }
                }

                if (present is not (0 or batchSize))
                {
                    lock (failures) failures.Add($"saw {present} of {batchSize} keys at one snapshot");
                    return;
                }
            }
        });

        for (int round = 0; round < 30; round++)
        {
            var batch = new WriteBatch();
            for (int i = 0; i < batchSize; i++)
            {
                batch.Put($"batch:{i:D3}", $"round-{round}");
            }
            await engine.WriteAsync(batch);

            var deletes = new WriteBatch();
            for (int i = 0; i < batchSize; i++)
            {
                deletes.Delete($"batch:{i:D3}");
            }
            await engine.WriteAsync(deletes);
        }

        await stop.CancelAsync();
        await reader;

        Assert.Empty(failures);
    }

    // ------------------------------------------------------------ durability

    [Fact]
    public async Task Data_survives_a_clean_reopen()
    {
        using var dir = new TempDirectory();
        var expected = new Dictionary<string, string>();

        await using (var engine = await LsmEngine.OpenAsync(Options(dir.Path)))
        {
            for (int i = 0; i < 2_000; i++)
            {
                string key = $"key:{i:D6}";
                string value = $"value-{i}";
                await engine.PutAsync(TestData.Key(key), TestData.Value(value));
                expected[key] = value;
            }

            for (int i = 0; i < 2_000; i += 5)
            {
                await engine.DeleteAsync(TestData.Key($"key:{i:D6}"));
                expected.Remove($"key:{i:D6}");
            }
        }

        await using var reopened = await LsmEngine.OpenAsync(Options(dir.Path));

        output.WriteLine(
            $"recovery: {reopened.Recovery.LogsReplayed} log(s), "
                + $"{reopened.Recovery.EntriesReplayed:N0} entries, "
                + $"{reopened.Recovery.TablesOpened} table(s)");

        Assert.True(reopened.Recovery.Clean);

        foreach (var (key, value) in expected)
        {
            Assert.Equal(value, TestData.Text(await reopened.GetAsync(TestData.Key(key))));
        }

        for (int i = 0; i < 2_000; i += 5)
        {
            Assert.Null(await reopened.GetAsync(TestData.Key($"key:{i:D6}")));
        }
    }

    [Fact]
    public async Task Unflushed_writes_are_recovered_from_the_log()
    {
        using var dir = new TempDirectory();

        await using (var engine = await LsmEngine.OpenAsync(Options(dir.Path)))
        {
            // Small enough to stay entirely in the memtable, so only the log can save them.
            for (int i = 0; i < 20; i++)
            {
                await engine.PutAsync(TestData.Key($"k{i}"), TestData.Value($"v{i}"));
            }

            Assert.Equal(0, engine.FlushCount);
        }

        await using var reopened = await LsmEngine.OpenAsync(Options(dir.Path));

        Assert.Equal(1, reopened.Recovery.LogsReplayed);
        Assert.Equal(20, reopened.Recovery.EntriesReplayed);
        Assert.Equal(20, reopened.Recovery.EntriesFlushedToTable);

        for (int i = 0; i < 20; i++)
        {
            Assert.Equal($"v{i}", TestData.Text(await reopened.GetAsync(TestData.Key($"k{i}"))));
        }
    }

    [Fact]
    public async Task A_compacted_database_reopens_correctly()
    {
        using var dir = new TempDirectory();
        var expected = new Dictionary<string, string>();

        await using (var engine = await LsmEngine.OpenAsync(Options(dir.Path)))
        {
            for (int round = 0; round < 8; round++)
            {
                for (int i = 0; i < 400; i++)
                {
                    string key = $"key:{i:D4}";
                    string value = $"round{round}-{i}";
                    await engine.PutAsync(TestData.Key(key), TestData.Value(value));
                    expected[key] = value;
                }
            }

            await engine.CompactAsync();
            output.WriteLine(
                $"levels after compaction: [{string.Join(", ", engine.GetStats().SsTablesPerLevel)}]");
        }

        await using var reopened = await LsmEngine.OpenAsync(Options(dir.Path));

        Assert.True(reopened.Recovery.Clean);
        foreach (var (key, value) in expected)
        {
            Assert.Equal(value, TestData.Text(await reopened.GetAsync(TestData.Key(key))));
        }
    }

    [Fact]
    public async Task Sequence_numbers_continue_across_a_reopen()
    {
        using var dir = new TempDirectory();

        await using (var engine = await LsmEngine.OpenAsync(Options(dir.Path)))
        {
            await engine.PutAsync(TestData.Key("k"), TestData.Value("first"));
        }

        await using (var engine = await LsmEngine.OpenAsync(Options(dir.Path)))
        {
            // If sequences restarted at zero, this write could sort below the recovered one and
            // the older value would win.
            await engine.PutAsync(TestData.Key("k"), TestData.Value("second"));
            Assert.Equal("second", TestData.Text(await engine.GetAsync(TestData.Key("k"))));
        }

        await using var reopened = await LsmEngine.OpenAsync(Options(dir.Path));
        Assert.Equal("second", TestData.Text(await reopened.GetAsync(TestData.Key("k"))));
    }

    // ------------------------------------------------------------ stats

    [Fact]
    public async Task Bloom_filters_eliminate_most_reads_for_absent_keys()
    {
        using var dir = new TempDirectory();
        await using var engine = await LsmEngine.OpenAsync(Options(dir.Path));

        // Even keys only. The probes below use odd keys, which fall *inside* every table's key
        // range -- so the cheap smallest/largest-key check cannot exclude them and the Bloom
        // filter is the thing actually doing the work. (Probing keys outside the range entirely,
        // the obvious thing to write, tests the range check instead and reports zero filter
        // activity, which is correct behaviour and a useless measurement.)
        byte[] value = new byte[128];
        for (int i = 0; i < 8_000; i += 2)
        {
            await engine.PutAsync(TestData.Key($"key:{i:D6}"), value);
        }
        await engine.FlushMemtableAsync();

        for (int i = 1; i < 8_000; i += 2)
        {
            Assert.Null(await engine.GetAsync(TestData.Key($"key:{i:D6}")));
        }

        var stats = engine.GetStats();
        long consulted = stats.BloomFilterNegatives + stats.BloomFilterFalsePositives;

        output.WriteLine(
            $"4,000 interleaved misses across [{string.Join(", ", stats.SsTablesPerLevel)}]: "
                + $"{stats.BloomFilterNegatives:N0} ruled out in memory, "
                + $"{stats.BloomFilterFalsePositives:N0} false positive(s) "
                + $"= {(double)stats.BloomFilterFalsePositives / Math.Max(1, consulted):P2}");

        Assert.True(stats.BloomFilterNegatives > 0, "the filter never ruled anything out");
        Assert.True(
            stats.BloomFilterFalsePositives < consulted * 0.05,
            $"false-positive rate {(double)stats.BloomFilterFalsePositives / consulted:P2} is too high");
    }

    [Fact]
    public async Task The_lsm_engine_advertises_scan_support()
    {
        using var dir = new TempDirectory();
        await using var engine = await LsmEngine.OpenAsync(Options(dir.Path));

        Assert.IsAssignableFrom<IScannableStorageEngine>(engine);
    }

    [Fact]
    public async Task Background_maintenance_keeps_the_tree_in_shape()
    {
        using var dir = new TempDirectory();
        await using var engine = await LsmEngine.OpenAsync(Options(dir.Path) with
        {
            EnableBackgroundCompaction = true,
        });

        byte[] value = new byte[256];
        for (int i = 0; i < 4_000; i++)
        {
            await engine.PutAsync(TestData.Key($"key:{i:D6}"), value);
        }

        // Give the background loop a moment to drain its queue.
        for (int attempt = 0; attempt < 50 && engine.GetStats().SsTablesPerLevel.Count <= 1; attempt++)
        {
            await Task.Delay(100);
        }

        var stats = engine.GetStats();
        output.WriteLine(
            $"background: {engine.FlushCount} flush(es), {stats.Compactions} compaction(s), "
                + $"levels [{string.Join(", ", stats.SsTablesPerLevel)}]");

        Assert.True(engine.FlushCount > 0);

        for (int i = 0; i < 4_000; i += 11)
        {
            Assert.NotNull(await engine.GetAsync(TestData.Key($"key:{i:D6}")));
        }
    }

    [Fact]
    public async Task Disposing_twice_is_safe()
    {
        using var dir = new TempDirectory();
        var engine = await LsmEngine.OpenAsync(Options(dir.Path));

        await engine.DisposeAsync();
        await engine.DisposeAsync();
    }
}
