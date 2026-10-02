using System.Collections.Concurrent;
using KestrelCache.Bitcask;
using Xunit;

namespace KestrelCache.Tests;

/// <summary>
/// Stress tests for the concurrency model.
/// </summary>
/// <remarks>
/// The original engine read its index outside the write lock while writers mutated the same
/// plain <see cref="Dictionary{TKey,TValue}"/> under it. That is not merely a stale-read risk:
/// a reader traversing a bucket chain during a resize can return a wrong entry or spin
/// indefinitely. Races are probabilistic, so these tests lean on many threads, many operations
/// and a tight keyspace to make the window as wide as possible.
/// </remarks>
public sealed class ConcurrencyTests
{
    private static DatabaseOptions Options(string path) => new()
    {
        Path = path,
        SyncPolicy = SyncPolicy.None,
        AutoCompactStaleRatio = 0,
    };

    [Fact]
    public async Task Concurrent_readers_and_writers_never_observe_a_torn_value()
    {
        using var dir = new TempDirectory();
        await using var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        const int keyspace = 64;
        const int generations = 400;

        // Every value is self-describing, so a reader can check that what it got back is a value
        // that was genuinely written for that key rather than a splice of two of them.
        static byte[] ValueFor(int key, int generation) =>
            TestData.Value($"key={key};gen={generation};" + new string('p', 64 + (generation % 128)));

        for (int k = 0; k < keyspace; k++)
        {
            await engine.PutAsync(TestData.Key($"k{k}"), ValueFor(k, 0));
        }

        using var stop = new CancellationTokenSource();
        var failures = new ConcurrentBag<string>();

        var readers = Enumerable.Range(0, 8).Select(id => Task.Run(async () =>
        {
            var random = new Random(id * 7919);
            while (!stop.IsCancellationRequested)
            {
                int k = random.Next(keyspace);
                byte[]? raw = await engine.GetAsync(TestData.Key($"k{k}"));
                if (raw is null)
                {
                    failures.Add($"k{k} vanished");
                    return;
                }

                string value = TestData.Text(raw)!;
                if (!value.StartsWith($"key={k};gen=", StringComparison.Ordinal))
                {
                    failures.Add($"k{k} returned a value belonging elsewhere: '{Trim(value)}'");
                    return;
                }
            }
        })).ToArray();

        var writers = Enumerable.Range(0, 4).Select(id => Task.Run(async () =>
        {
            for (int generation = 1; generation <= generations; generation++)
            {
                for (int k = id; k < keyspace; k += 4)
                {
                    await engine.PutAsync(TestData.Key($"k{k}"), ValueFor(k, generation));
                }
            }
        })).ToArray();

        await Task.WhenAll(writers);
        await stop.CancelAsync();
        await Task.WhenAll(readers);

        Assert.Empty(failures);
    }

    [Fact]
    public async Task Concurrent_writers_do_not_lose_writes()
    {
        using var dir = new TempDirectory();
        await using var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        const int writerCount = 8;
        const int perWriter = 500;

        await Task.WhenAll(Enumerable.Range(0, writerCount).Select(id => Task.Run(async () =>
        {
            for (int i = 0; i < perWriter; i++)
            {
                await engine.PutAsync(
                    TestData.Key($"w{id}:i{i}"),
                    TestData.Value($"{id}-{i}"));
            }
        })));

        Assert.Equal(writerCount * perWriter, engine.GetStats().KeyCount);

        for (int id = 0; id < writerCount; id++)
        {
            for (int i = 0; i < perWriter; i += 37)
            {
                Assert.Equal(
                    $"{id}-{i}",
                    TestData.Text(await engine.GetAsync(TestData.Key($"w{id}:i{i}"))));
            }
        }
    }

    [Fact]
    public async Task Concurrent_writes_replay_consistently_after_a_restart()
    {
        using var dir = new TempDirectory();

        const int writerCount = 6;
        const int perWriter = 400;

        await using (var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath)))
        {
            await Task.WhenAll(Enumerable.Range(0, writerCount).Select(id => Task.Run(async () =>
            {
                for (int i = 0; i < perWriter; i++)
                {
                    await engine.PutAsync(
                        TestData.Key($"w{id}:i{i}"),
                        TestData.Value($"{id}-{i}"));
                }
            })));
        }

        await using var reopened = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        Assert.True(reopened.Recovery.Clean);
        Assert.Equal(writerCount * perWriter, reopened.GetStats().KeyCount);

        for (int id = 0; id < writerCount; id++)
        {
            for (int i = 0; i < perWriter; i++)
            {
                Assert.Equal(
                    $"{id}-{i}",
                    TestData.Text(await reopened.GetAsync(TestData.Key($"w{id}:i{i}"))));
            }
        }
    }

    [Fact]
    public async Task Concurrent_deletes_and_reads_stay_consistent()
    {
        using var dir = new TempDirectory();
        await using var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        const int count = 2_000;
        for (int i = 0; i < count; i++)
        {
            await engine.PutAsync(TestData.Key($"k{i}"), TestData.Value($"v{i}"));
        }

        var failures = new ConcurrentBag<string>();

        var deleter = Task.Run(async () =>
        {
            for (int i = 0; i < count; i++)
            {
                await engine.DeleteAsync(TestData.Key($"k{i}"));
            }
        });

        var readers = Enumerable.Range(0, 6).Select(id => Task.Run(async () =>
        {
            var random = new Random(id * 104_729);
            for (int n = 0; n < 4_000; n++)
            {
                int i = random.Next(count);
                byte[]? raw = await engine.GetAsync(TestData.Key($"k{i}"));

                // A key is either still present with its own value, or already deleted. It must
                // never come back carrying someone else's value.
                if (raw is not null && TestData.Text(raw) != $"v{i}")
                {
                    failures.Add($"k{i} read back as '{Trim(TestData.Text(raw)!)}'");
                }
            }
        })).ToArray();

        await Task.WhenAll(readers.Append(deleter));

        Assert.Empty(failures);
        Assert.Equal(0, engine.GetStats().KeyCount);
    }

    [Fact]
    public async Task Compaction_runs_safely_alongside_readers_and_writers()
    {
        using var dir = new TempDirectory();
        await using var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        const int keyspace = 200;
        for (int k = 0; k < keyspace; k++)
        {
            await engine.PutAsync(TestData.Key($"k{k}"), TestData.Value($"v{k}-0"));
        }

        using var stop = new CancellationTokenSource();
        var failures = new ConcurrentBag<string>();

        var readers = Enumerable.Range(0, 6).Select(id => Task.Run(async () =>
        {
            var random = new Random(id * 31_337);
            while (!stop.IsCancellationRequested)
            {
                int k = random.Next(keyspace);
                byte[]? raw = await engine.GetAsync(TestData.Key($"k{k}"));
                if (raw is null)
                {
                    failures.Add($"k{k} vanished during compaction");
                    return;
                }
                if (!TestData.Text(raw)!.StartsWith($"v{k}-", StringComparison.Ordinal))
                {
                    failures.Add($"k{k} returned '{Trim(TestData.Text(raw)!)}'");
                    return;
                }
            }
        })).ToArray();

        var writer = Task.Run(async () =>
        {
            for (int generation = 1; generation <= 40; generation++)
            {
                for (int k = 0; k < keyspace; k++)
                {
                    await engine.PutAsync(TestData.Key($"k{k}"), TestData.Value($"v{k}-{generation}"));
                }
            }
        });

        var compactor = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                await engine.CompactAsync();
                await Task.Delay(5);
            }
        });

        await writer;
        await stop.CancelAsync();
        await compactor;
        await Task.WhenAll(readers);

        Assert.Empty(failures);
    }

    [Fact]
    public async Task Concurrent_batches_remain_internally_consistent_after_a_restart()
    {
        using var dir = new TempDirectory();

        const int writerCount = 4;
        const int batchCount = 150;
        const int batchSize = 10;

        await using (var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath)))
        {
            await Task.WhenAll(Enumerable.Range(0, writerCount).Select(id => Task.Run(async () =>
            {
                for (int b = 0; b < batchCount; b++)
                {
                    var batch = new WriteBatch();
                    for (int i = 0; i < batchSize; i++)
                    {
                        batch.Put($"w{id}:b{b}:i{i}", $"{id}-{b}-{i}");
                    }
                    await engine.WriteAsync(batch);
                }
            })));
        }

        await using var reopened = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        Assert.True(reopened.Recovery.Clean);
        Assert.Equal(writerCount * batchCount * batchSize, reopened.GetStats().KeyCount);
    }

    private static string Trim(string value) =>
        value.Length <= 48 ? value : value[..48] + "...";
}
