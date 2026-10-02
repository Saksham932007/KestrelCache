using System.Diagnostics;
using KestrelCache.Bitcask;
using KestrelCache.Diagnostics;

namespace KestrelCache.Benchmarks;

/// <summary>
/// The tail-latency suite: real workloads against a real database file, reporting percentiles.
/// </summary>
public sealed class LatencySuite(string root, int operations, int valueSize)
{
    private readonly List<LatencyHistogram> _results = [];

    /// <summary>Everything measured so far.</summary>
    public IReadOnlyList<LatencyHistogram> Results => _results;

    public async Task RunAllAsync()
    {
        Console.WriteLine($"root            {root}");
        Console.WriteLine($"operations      {operations:N0}");
        Console.WriteLine($"value size      {valueSize} B");
        Console.WriteLine($"filesystem      {DescribeFilesystem(root)}");
        Console.WriteLine();
        Console.WriteLine(LatencyHistogram.Header);

        await WritesBySyncPolicyAsync();
        await BatchSizeSweepAsync();
        await RandomReadsAsync();
        await ConcurrentReadScalingAsync();
        await ReadsDuringCompactionAsync();
        await RecoveryTimeAsync();

        Console.WriteLine();
        Console.WriteLine("Markdown:");
        Console.WriteLine();
        Console.WriteLine(LatencyHistogram.MarkdownHeader);
        foreach (var result in _results)
        {
            Console.WriteLine(result.ToMarkdownRow());
        }
    }

    // ------------------------------------------------------------ write latency

    /// <summary>
    /// The same write workload under all three durability policies. This is the measurement that
    /// turns the fsync trade-off from an assertion into a number.
    /// </summary>
    private async Task WritesBySyncPolicyAsync()
    {
        foreach (var policy in (SyncPolicy[])[SyncPolicy.None, SyncPolicy.Interval, SyncPolicy.EveryWrite])
        {
            // fsync-per-write is ~1 ms on a real device, so a full-length run would take minutes
            // for no extra information. The percentiles are stable well before that.
            int count = policy == SyncPolicy.EveryWrite
                ? Math.Min(operations, 3_000)
                : operations;

            string path = Path.Combine(root, $"write-{policy}.kc");
            Delete(path);

            await using var engine = await BitcaskEngine.OpenAsync(new DatabaseOptions
            {
                Path = path,
                SyncPolicy = policy,
                AutoCompactStaleRatio = 0,
            });

            byte[] value = Payload(valueSize);
            var histogram = new LatencyHistogram($"put, sync={policy.ToString().ToLowerInvariant()}", count);
            var wall = Stopwatch.StartNew();

            for (int i = 0; i < count; i++)
            {
                byte[] key = KestrelCache.ByteKey.From($"key:{i:D12}");
                long start = Stopwatch.GetTimestamp();
                await engine.PutAsync(key, value);
                histogram.Record(Stopwatch.GetTimestamp() - start);
            }

            wall.Stop();
            histogram.Elapsed = wall.Elapsed;
            Report(histogram);
            Delete(path);
        }
    }

    /// <summary>
    /// How much batching buys, measured per key rather than per batch, so the numbers are
    /// directly comparable with the unbatched row above.
    /// </summary>
    private async Task BatchSizeSweepAsync()
    {
        foreach (int batchSize in (int[])[1, 10, 100, 1_000])
        {
            int count = Math.Min(operations, 20_000);
            string path = Path.Combine(root, $"batch-{batchSize}.kc");
            Delete(path);

            await using var engine = await BitcaskEngine.OpenAsync(new DatabaseOptions
            {
                Path = path,
                SyncPolicy = SyncPolicy.EveryWrite,
                AutoCompactStaleRatio = 0,
            });

            byte[] value = Payload(valueSize);
            var histogram = new LatencyHistogram(
                $"put, everywrite, batch={batchSize}", count / batchSize + 1);
            var wall = Stopwatch.StartNew();

            var batch = new WriteBatch();
            for (int i = 0; i < count; i++)
            {
                batch.Put(KestrelCache.ByteKey.From($"key:{i:D12}"), value);
                if (batch.Count < batchSize) continue;

                long start = Stopwatch.GetTimestamp();
                await engine.WriteAsync(batch);
                long elapsed = Stopwatch.GetTimestamp() - start;

                // Attribute the batch's cost evenly across its keys: the question being answered
                // is "what does one key cost", not "what does one syscall cost".
                for (int k = 0; k < batch.Count; k++)
                {
                    histogram.Record(elapsed / batch.Count);
                }
                batch.Clear();
            }

            if (batch.Count > 0) await engine.WriteAsync(batch);

            wall.Stop();
            histogram.Elapsed = wall.Elapsed;
            Report(histogram);
            Delete(path);
        }
    }

    // ------------------------------------------------------------ read latency

    private async Task RandomReadsAsync()
    {
        string path = Path.Combine(root, "reads.kc");
        Delete(path);

        int keyCount = Math.Min(operations, 200_000);

        await using var engine = await BitcaskEngine.OpenAsync(new DatabaseOptions
        {
            Path = path,
            SyncPolicy = SyncPolicy.None,
            AutoCompactStaleRatio = 0,
        });

        await PopulateAsync(engine, keyCount);

        var random = new Random(12345);
        var histogram = new LatencyHistogram("get, random, single thread", operations);
        var wall = Stopwatch.StartNew();

        for (int i = 0; i < operations; i++)
        {
            byte[] key = KestrelCache.ByteKey.From($"key:{random.Next(keyCount):D12}");
            long start = Stopwatch.GetTimestamp();
            _ = await engine.GetAsync(key);
            histogram.Record(Stopwatch.GetTimestamp() - start);
        }

        wall.Stop();
        histogram.Elapsed = wall.Elapsed;
        Report(histogram);

        // A miss should be cheaper than a hit: it is answered by the index alone, with no I/O.
        var misses = new LatencyHistogram("get, miss (index only)", operations);
        var missWall = Stopwatch.StartNew();

        for (int i = 0; i < operations; i++)
        {
            byte[] key = KestrelCache.ByteKey.From($"absent:{i:D12}");
            long start = Stopwatch.GetTimestamp();
            _ = await engine.GetAsync(key);
            misses.Record(Stopwatch.GetTimestamp() - start);
        }

        missWall.Stop();
        misses.Elapsed = missWall.Elapsed;
        Report(misses);

        Delete(path);
    }

    /// <summary>
    /// Read throughput against thread count.
    /// </summary>
    /// <remarks>
    /// This is the benchmark that justifies the positional-I/O read path. The original engine
    /// took the write lock on every read, so adding threads could not add throughput — the
    /// numbers here should scale with core count instead of flattening at one thread's worth.
    /// </remarks>
    private async Task ConcurrentReadScalingAsync()
    {
        string path = Path.Combine(root, "concurrent-reads.kc");
        Delete(path);

        int keyCount = Math.Min(operations, 200_000);

        await using var engine = await BitcaskEngine.OpenAsync(new DatabaseOptions
        {
            Path = path,
            SyncPolicy = SyncPolicy.None,
            AutoCompactStaleRatio = 0,
        });

        await PopulateAsync(engine, keyCount);

        foreach (int threads in (int[])[1, 2, 4, 8])
        {
            int perThread = Math.Max(1, operations / threads);
            var histograms = new LatencyHistogram[threads];
            var wall = Stopwatch.StartNew();

            await Task.WhenAll(Enumerable.Range(0, threads).Select(id => Task.Run(async () =>
            {
                var random = new Random(id * 7919);
                var local = new LatencyHistogram($"thread-{id}", perThread);

                for (int i = 0; i < perThread; i++)
                {
                    byte[] key = KestrelCache.ByteKey.From($"key:{random.Next(keyCount):D12}");
                    long start = Stopwatch.GetTimestamp();
                    _ = await engine.GetAsync(key);
                    local.Record(Stopwatch.GetTimestamp() - start);
                }

                histograms[id] = local;
            })));

            wall.Stop();

            var merged = new LatencyHistogram($"get, {threads} thread(s)", perThread * threads);
            // Percentiles have to come from the pooled samples, not from averaging each
            // thread's percentiles -- the average of eight p99s is not a p99 of anything.
            foreach (var histogram in histograms)
            {
                MergeInto(merged, histogram);
            }
            merged.Elapsed = wall.Elapsed;
            Report(merged);
        }

        Delete(path);
    }

    /// <summary>
    /// Read latency while a compaction runs underneath. A storage engine's worst tail latency is
    /// almost always "a read that happened during background maintenance", so measuring it
    /// separately is the only way to know whether the design actually keeps readers out of the
    /// compactor's way.
    /// </summary>
    private async Task ReadsDuringCompactionAsync()
    {
        string path = Path.Combine(root, "compaction-reads.kc");
        Delete(path);

        int keyCount = Math.Min(operations, 50_000);

        await using var engine = await BitcaskEngine.OpenAsync(new DatabaseOptions
        {
            Path = path,
            SyncPolicy = SyncPolicy.None,
            AutoCompactStaleRatio = 0,
        });

        // Write each key several times so there is real garbage for compaction to reclaim.
        for (int generation = 0; generation < 3; generation++)
        {
            await PopulateAsync(engine, keyCount);
        }

        using var stop = new CancellationTokenSource();
        var compactor = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                await engine.CompactAsync(stop.Token);
            }
        }, stop.Token);

        var random = new Random(999);
        var histogram = new LatencyHistogram("get, during compaction", operations);
        var wall = Stopwatch.StartNew();

        for (int i = 0; i < operations; i++)
        {
            byte[] key = KestrelCache.ByteKey.From($"key:{random.Next(keyCount):D12}");
            long start = Stopwatch.GetTimestamp();
            _ = await engine.GetAsync(key);
            histogram.Record(Stopwatch.GetTimestamp() - start);
        }

        wall.Stop();
        await stop.CancelAsync();

        try
        {
            await compactor;
        }
        catch (OperationCanceledException)
        {
            // Expected.
        }

        histogram.Elapsed = wall.Elapsed;
        Report(histogram);
        Delete(path);
    }

    // ------------------------------------------------------------ recovery

    /// <summary>
    /// Startup time against log size.
    /// </summary>
    /// <remarks>
    /// Recovery cost is the Bitcask design's defining weakness and belongs in any honest
    /// benchmark of it: the index lives only in memory, so every restart replays the entire log
    /// to rebuild it. Reads and writes are O(1), but startup is O(records) — which is fine at a
    /// million keys and ruinous at a billion, and is one of the reasons the LSM engine exists.
    /// </remarks>
    private async Task RecoveryTimeAsync()
    {
        foreach (int records in (int[])[10_000, 100_000, 500_000])
        {
            string path = Path.Combine(root, $"recovery-{records}.kc");
            Delete(path);

            await using (var engine = await BitcaskEngine.OpenAsync(new DatabaseOptions
            {
                Path = path,
                SyncPolicy = SyncPolicy.None,
                AutoCompactStaleRatio = 0,
            }))
            {
                var batch = new WriteBatch();
                byte[] value = Payload(valueSize);
                for (int i = 0; i < records; i++)
                {
                    batch.Put(KestrelCache.ByteKey.From($"key:{i:D12}"), value);
                    if (batch.Count >= 1_000)
                    {
                        await engine.WriteAsync(batch);
                        batch.Clear();
                    }
                }
                if (batch.Count > 0) await engine.WriteAsync(batch);
            }

            var histogram = new LatencyHistogram($"open, replay {records:N0} records", 1);
            var wall = Stopwatch.StartNew();
            long start = Stopwatch.GetTimestamp();

            await using (var reopened = await BitcaskEngine.OpenAsync(new DatabaseOptions
            {
                Path = path,
                SyncPolicy = SyncPolicy.None,
                AutoCompactStaleRatio = 0,
            }))
            {
                histogram.Record(Stopwatch.GetTimestamp() - start);
                wall.Stop();

                if (reopened.GetStats().KeyCount != records)
                {
                    throw new InvalidOperationException(
                        $"recovery lost keys: expected {records}, found {reopened.GetStats().KeyCount}");
                }
            }

            histogram.Elapsed = wall.Elapsed;
            Report(histogram);
            Delete(path);
        }
    }

    // ------------------------------------------------------------ plumbing

    private static void MergeInto(LatencyHistogram target, LatencyHistogram source)
    {
        foreach (long sample in source.RawSamples)
        {
            target.Record(sample);
        }
    }

    private static async Task PopulateAsync(BitcaskEngine engine, int count)
    {
        var batch = new WriteBatch();
        for (int i = 0; i < count; i++)
        {
            string key = $"key:{i:D12}";
            batch.Put(KestrelCache.ByteKey.From(key), DeterministicPayload.BytesFor(key, 128));
            if (batch.Count >= 1_000)
            {
                await engine.WriteAsync(batch);
                batch.Clear();
            }
        }
        if (batch.Count > 0) await engine.WriteAsync(batch);
        await engine.FlushAsync();
    }

    private static byte[] Payload(int size)
    {
        var value = new byte[size];
        new Random(4242).NextBytes(value);
        return value;
    }

    private void Report(LatencyHistogram histogram)
    {
        _results.Add(histogram);
        Console.WriteLine(histogram.ToRow());
    }

    private static void Delete(string path)
    {
        if (File.Exists(path)) File.Delete(path);
        if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
    }

    private static string DescribeFilesystem(string path)
    {
        try
        {
            string full = Path.GetFullPath(path);
            DriveInfo? owner = null;
            foreach (var drive in DriveInfo.GetDrives())
            {
                string driveRoot = drive.RootDirectory.FullName;
                if (!full.StartsWith(driveRoot, StringComparison.Ordinal)) continue;
                if (owner is null || driveRoot.Length > owner.RootDirectory.FullName.Length)
                {
                    owner = drive;
                }
            }

            string format = owner?.DriveFormat ?? "unknown";
            string warning = format is "tmpfs" or "ramfs"
                ? "  <-- memory-backed: fsync is a no-op here, durability numbers are meaningless"
                : string.Empty;
            return format + warning;
        }
        catch (IOException)
        {
            return "unknown";
        }
    }
}
