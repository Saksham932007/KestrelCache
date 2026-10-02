using KestrelCache.Bitcask;
using Xunit;

namespace KestrelCache.Tests;

public sealed class BitcaskEngineTests
{
    private static DatabaseOptions Options(string path) => new()
    {
        Path = path,
        SyncPolicy = SyncPolicy.EveryWrite,
        AutoCompactStaleRatio = 0, // deterministic: tests drive compaction explicitly
    };

    [Fact]
    public async Task Get_returns_null_for_a_missing_key()
    {
        using var dir = new TempDirectory();
        await using var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        Assert.Null(await engine.GetAsync(TestData.Key("nope")));
    }

    [Fact]
    public async Task Put_then_get_round_trips()
    {
        using var dir = new TempDirectory();
        await using var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        await engine.PutAsync(TestData.Key("user:1"), TestData.Value("alice"));

        Assert.Equal("alice", TestData.Text(await engine.GetAsync(TestData.Key("user:1"))));
    }

    [Fact]
    public async Task Put_overwrites_the_previous_value()
    {
        using var dir = new TempDirectory();
        await using var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        await engine.PutAsync(TestData.Key("k"), TestData.Value("v1"));
        await engine.PutAsync(TestData.Key("k"), TestData.Value("v2"));

        Assert.Equal("v2", TestData.Text(await engine.GetAsync(TestData.Key("k"))));
    }

    [Fact]
    public async Task Delete_hides_the_key()
    {
        using var dir = new TempDirectory();
        await using var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        await engine.PutAsync(TestData.Key("k"), TestData.Value("v"));
        await engine.DeleteAsync(TestData.Key("k"));

        Assert.Null(await engine.GetAsync(TestData.Key("k")));
    }

    [Fact]
    public async Task Deleting_an_absent_key_succeeds()
    {
        using var dir = new TempDirectory();
        await using var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        await engine.DeleteAsync(TestData.Key("ghost"));

        Assert.Null(await engine.GetAsync(TestData.Key("ghost")));
    }

    [Fact]
    public async Task Empty_keys_are_rejected()
    {
        using var dir = new TempDirectory();
        await using var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        await Assert.ThrowsAsync<ArgumentException>(
            async () => await engine.PutAsync([], TestData.Value("v")));
    }

    [Fact]
    public async Task Oversized_values_are_rejected()
    {
        using var dir = new TempDirectory();
        var options = Options(dir.DbPath) with { MaxValueSize = 16 };
        await using var engine = await BitcaskEngine.OpenAsync(options);

        await Assert.ThrowsAsync<KeyOrValueTooLargeException>(
            async () => await engine.PutAsync(TestData.Key("k"), new byte[17]));
    }

    [Fact]
    public async Task Values_may_be_empty()
    {
        using var dir = new TempDirectory();
        await using var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        await engine.PutAsync(TestData.Key("k"), []);

        byte[]? read = await engine.GetAsync(TestData.Key("k"));
        Assert.NotNull(read);
        Assert.Empty(read);
    }

    [Fact]
    public async Task Values_may_be_arbitrary_binary()
    {
        using var dir = new TempDirectory();
        await using var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        byte[] key = [0x00, 0xFF, 0x7F, 0x80];
        byte[] value = Enumerable.Range(0, 256).Select(i => (byte)i).ToArray();

        await engine.PutAsync(key, value);

        Assert.Equal(value, await engine.GetAsync(key));
    }

    // ------------------------------------------------------------ durability

    [Fact]
    public async Task Data_survives_a_clean_reopen()
    {
        using var dir = new TempDirectory();

        await using (var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath)))
        {
            await engine.PutAsync(TestData.Key("a"), TestData.Value("1"));
            await engine.PutAsync(TestData.Key("b"), TestData.Value("2"));
        }

        await using var reopened = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        Assert.Equal("1", TestData.Text(await reopened.GetAsync(TestData.Key("a"))));
        Assert.Equal("2", TestData.Text(await reopened.GetAsync(TestData.Key("b"))));
        Assert.True(reopened.Recovery.Clean);
    }

    [Fact]
    public async Task Deletes_survive_a_reopen()
    {
        using var dir = new TempDirectory();

        await using (var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath)))
        {
            await engine.PutAsync(TestData.Key("a"), TestData.Value("1"));
            await engine.DeleteAsync(TestData.Key("a"));
        }

        await using var reopened = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        Assert.Null(await reopened.GetAsync(TestData.Key("a")));
    }

    /// <summary>
    /// Regression test for the tombstone replay off-by-one.
    /// </summary>
    /// <remarks>
    /// The original <c>LoadIndexAsync</c> advanced its offset cursor with
    /// <c>position + valueLength</c>, and a tombstone encoded its value length as the sentinel
    /// <c>-1</c>. So every record written after a delete was indexed one byte before its real
    /// start, and after a restart a lookup for it would seek into the middle of the preceding
    /// record's framing. The bug was invisible in the original demo only because the delete
    /// happened to be the final write in the file.
    /// </remarks>
    [Fact]
    public async Task Keys_written_after_a_delete_are_readable_after_a_restart()
    {
        using var dir = new TempDirectory();

        await using (var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath)))
        {
            await engine.PutAsync(TestData.Key("user:1"), TestData.Value("alice"));
            await engine.PutAsync(TestData.Key("user:2"), TestData.Value("bob"));
            await engine.DeleteAsync(TestData.Key("user:2"));

            // Everything from here on would land at a shifted offset under the old cursor maths.
            await engine.PutAsync(TestData.Key("user:3"), TestData.Value("carol"));
            await engine.PutAsync(TestData.Key("user:4"), TestData.Value("dave"));
        }

        await using var reopened = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        Assert.True(reopened.Recovery.Clean);
        Assert.Equal("alice", TestData.Text(await reopened.GetAsync(TestData.Key("user:1"))));
        Assert.Null(await reopened.GetAsync(TestData.Key("user:2")));
        Assert.Equal("carol", TestData.Text(await reopened.GetAsync(TestData.Key("user:3"))));
        Assert.Equal("dave", TestData.Text(await reopened.GetAsync(TestData.Key("user:4"))));
    }

    /// <summary>
    /// The same bug, amplified: many interleaved deletes and writes compound the drift, so this
    /// would fail even if a single off-by-one happened to land on a readable boundary.
    /// </summary>
    [Fact]
    public async Task Many_interleaved_deletes_and_writes_survive_a_restart()
    {
        using var dir = new TempDirectory();
        var expected = new Dictionary<string, string>();

        await using (var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath)))
        {
            for (int i = 0; i < 200; i++)
            {
                string key = $"key:{i:D4}";
                await engine.PutAsync(TestData.Key(key), TestData.Value($"value-{i}"));
                expected[key] = $"value-{i}";

                if (i % 3 == 0)
                {
                    await engine.DeleteAsync(TestData.Key(key));
                    expected.Remove(key);
                }
            }
        }

        await using var reopened = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        Assert.True(reopened.Recovery.Clean);
        for (int i = 0; i < 200; i++)
        {
            string key = $"key:{i:D4}";
            string? actual = TestData.Text(await reopened.GetAsync(TestData.Key(key)));
            Assert.Equal(expected.GetValueOrDefault(key), actual);
        }
    }

    // ------------------------------------------------------------ batches

    [Fact]
    public async Task A_batch_applies_every_operation()
    {
        using var dir = new TempDirectory();
        await using var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        var batch = new WriteBatch()
            .Put("a", "1")
            .Put("b", "2")
            .Put("c", "3");

        await engine.WriteAsync(batch);

        Assert.Equal("1", TestData.Text(await engine.GetAsync(TestData.Key("a"))));
        Assert.Equal("2", TestData.Text(await engine.GetAsync(TestData.Key("b"))));
        Assert.Equal("3", TestData.Text(await engine.GetAsync(TestData.Key("c"))));
    }

    [Fact]
    public async Task A_batch_may_mix_puts_and_deletes_and_later_ops_win()
    {
        using var dir = new TempDirectory();
        await using var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        await engine.PutAsync(TestData.Key("old"), TestData.Value("gone"));

        await engine.WriteAsync(new WriteBatch()
            .Put("k", "first")
            .Put("k", "second")
            .Delete("old"));

        Assert.Equal("second", TestData.Text(await engine.GetAsync(TestData.Key("k"))));
        Assert.Null(await engine.GetAsync(TestData.Key("old")));
    }

    [Fact]
    public async Task A_batch_survives_a_reopen()
    {
        using var dir = new TempDirectory();

        await using (var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath)))
        {
            await engine.WriteAsync(new WriteBatch().Put("a", "1").Put("b", "2").Delete("a"));
        }

        await using var reopened = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        Assert.True(reopened.Recovery.Clean);
        Assert.Null(await reopened.GetAsync(TestData.Key("a")));
        Assert.Equal("2", TestData.Text(await reopened.GetAsync(TestData.Key("b"))));
    }

    [Fact]
    public async Task An_empty_batch_is_a_no_op()
    {
        using var dir = new TempDirectory();
        await using var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        await engine.WriteAsync(new WriteBatch());

        Assert.Equal(0, engine.GetStats().KeyCount);
    }

    // ------------------------------------------------------------ compaction

    [Fact]
    public async Task Compaction_reclaims_space_held_by_overwrites()
    {
        using var dir = new TempDirectory();
        await using var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        byte[] value = new byte[1024];
        for (int i = 0; i < 100; i++)
        {
            await engine.PutAsync(TestData.Key("hot"), value);
        }

        long before = engine.GetStats().DiskSizeBytes;
        await engine.CompactAsync();
        long after = engine.GetStats().DiskSizeBytes;

        Assert.True(after < before / 10, $"expected a large reduction, got {before} -> {after}");
        Assert.Equal(1, engine.GetStats().KeyCount);
        Assert.Equal(value, await engine.GetAsync(TestData.Key("hot")));
    }

    [Fact]
    public async Task Compaction_drops_tombstoned_records()
    {
        using var dir = new TempDirectory();
        await using var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        for (int i = 0; i < 50; i++)
        {
            await engine.PutAsync(TestData.Key($"k{i}"), new byte[512]);
        }
        for (int i = 0; i < 50; i++)
        {
            await engine.DeleteAsync(TestData.Key($"k{i}"));
        }

        await engine.CompactAsync();

        var stats = engine.GetStats();
        Assert.Equal(0, stats.KeyCount);
        Assert.Equal(0, stats.LiveDataBytes);
    }

    [Fact]
    public async Task A_compacted_database_reopens_correctly()
    {
        using var dir = new TempDirectory();
        var expected = new Dictionary<string, string>();

        await using (var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath)))
        {
            for (int round = 0; round < 5; round++)
            {
                for (int i = 0; i < 40; i++)
                {
                    string key = $"k{i}";
                    string value = $"round{round}-value{i}";
                    await engine.PutAsync(TestData.Key(key), TestData.Value(value));
                    expected[key] = value;
                }
            }

            for (int i = 0; i < 40; i += 4)
            {
                await engine.DeleteAsync(TestData.Key($"k{i}"));
                expected.Remove($"k{i}");
            }

            await engine.CompactAsync();
        }

        await using var reopened = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        Assert.True(reopened.Recovery.Clean);
        Assert.Equal(expected.Count, reopened.GetStats().KeyCount);
        foreach (var (key, value) in expected)
        {
            Assert.Equal(value, TestData.Text(await reopened.GetAsync(TestData.Key(key))));
        }
    }

    /// <summary>
    /// Regression test: compaction must strip batch framing from the records it keeps.
    /// </summary>
    /// <remarks>
    /// Records in a batch all carry a continuation flag except the final one, which terminates
    /// it. Compaction keeps only live records, so it routinely drops a batch's terminator while
    /// keeping an earlier member of that batch. If the kept record still carries its flag, the
    /// next replay sees a batch that never ends and discards it -- losing live data on a
    /// perfectly clean shutdown. Found by the model-based fuzzer, not by inspection.
    /// </remarks>
    [Fact]
    public async Task Compaction_strips_batch_framing_so_the_log_replays_cleanly()
    {
        using var dir = new TempDirectory();

        await using (var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath)))
        {
            // A batch whose later members are then superseded, so compaction keeps the first
            // record of the batch but drops the one that terminated it.
            await engine.WriteAsync(new WriteBatch()
                .Put("keep", "original")
                .Put("shadowed-1", "a")
                .Put("shadowed-2", "b"));

            await engine.PutAsync(TestData.Key("shadowed-1"), TestData.Value("a2"));
            await engine.PutAsync(TestData.Key("shadowed-2"), TestData.Value("b2"));

            await engine.CompactAsync();
        }

        await using var reopened = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        Assert.True(
            reopened.Recovery.Clean,
            $"recovery was not clean: {reopened.Recovery.TruncationReason}");
        Assert.Equal(0, reopened.Recovery.UncommittedBatchRecordsDiscarded);
        Assert.Equal("original", TestData.Text(await reopened.GetAsync(TestData.Key("keep"))));
        Assert.Equal("a2", TestData.Text(await reopened.GetAsync(TestData.Key("shadowed-1"))));
        Assert.Equal("b2", TestData.Text(await reopened.GetAsync(TestData.Key("shadowed-2"))));
    }

    [Fact]
    public async Task Reads_in_flight_during_a_compaction_still_succeed()
    {
        using var dir = new TempDirectory();
        await using var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath) with
        {
            SyncPolicy = SyncPolicy.None,
        });

        for (int i = 0; i < 500; i++)
        {
            await engine.PutAsync(TestData.Key($"k{i}"), TestData.Value($"v{i}"));
        }

        using var stop = new CancellationTokenSource();

        var readers = Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
        {
            var random = new Random(Environment.CurrentManagedThreadId);
            while (!stop.IsCancellationRequested)
            {
                int i = random.Next(500);
                string? value = TestData.Text(await engine.GetAsync(TestData.Key($"k{i}")));
                Assert.Equal($"v{i}", value);
            }
        })).ToArray();

        for (int round = 0; round < 5; round++)
        {
            await engine.CompactAsync();
        }

        await stop.CancelAsync();
        await Task.WhenAll(readers);
    }

    // ------------------------------------------------------------ lifecycle

    [Fact]
    public async Task Disposing_twice_is_safe()
    {
        using var dir = new TempDirectory();
        var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        await engine.DisposeAsync();
        await engine.DisposeAsync();
    }

    /// <summary>
    /// Regression test: the documented usage in the original README called
    /// <c>CloseAsync()</c> and then disposed, which threw because teardown ran twice.
    /// </summary>
    [Fact]
    public async Task Close_then_dispose_is_safe()
    {
        using var dir = new TempDirectory();
        var db = await KestrelDb.OpenAsync(new DatabaseOptions { Path = dir.DbPath }, EngineKind.Bitcask);

        await db.PutAsync("k", "v");
        await db.CloseAsync();
        await db.DisposeAsync();
    }

    [Fact]
    public async Task Stats_track_operation_counts()
    {
        using var dir = new TempDirectory();
        await using var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        await engine.PutAsync(TestData.Key("a"), TestData.Value("1"));
        await engine.PutAsync(TestData.Key("b"), TestData.Value("2"));
        await engine.DeleteAsync(TestData.Key("a"));
        await engine.GetAsync(TestData.Key("b"));

        var stats = engine.GetStats();
        Assert.Equal("bitcask", stats.Engine);
        Assert.Equal(2, stats.Writes);
        Assert.Equal(1, stats.Deletes);
        Assert.Equal(1, stats.Reads);
        Assert.Equal(1, stats.KeyCount);
    }

    [Fact]
    public async Task The_bitcask_engine_does_not_claim_to_support_scans()
    {
        using var dir = new TempDirectory();
        await using var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        Assert.IsNotAssignableFrom<IScannableStorageEngine>(engine);
    }
}
