using System.Diagnostics;
using System.Globalization;
using KestrelCache.Bitcask;
using KestrelCache.Diagnostics;
using KestrelCache.Lsm;

namespace KestrelCache.Benchmarks;

/// <summary>
/// Runs identical workloads against both engines and reports the difference.
/// </summary>
/// <remarks>
/// This suite is the reason both engines exist in one repository. Design trade-offs are easy to
/// assert and hard to demonstrate, and "an LSM tree scales better" means nothing without a
/// number attached. Running the same keys, the same value sizes and the same access patterns
/// through both, on the same device, turns each claimed trade-off into a measurement — including
/// the ones where the simpler engine wins, which it does on point lookups and on write latency.
/// </remarks>
public sealed class EngineComparison(string root, int operations, int valueSize)
{
    private readonly List<ComparisonRow> _rows = [];

    private sealed record ComparisonRow(
        string Scenario,
        string Unit,
        double Bitcask,
        double Lsm,
        bool LowerIsBetter)
    {
        internal string Winner => Bitcask == Lsm
            ? "tie"
            : (LowerIsBetter ? Bitcask < Lsm : Bitcask > Lsm) ? "bitcask" : "lsm";

        internal double Ratio
        {
            get
            {
                double best = LowerIsBetter ? Math.Min(Bitcask, Lsm) : Math.Max(Bitcask, Lsm);
                double worst = LowerIsBetter ? Math.Max(Bitcask, Lsm) : Math.Min(Bitcask, Lsm);
                return worst == 0 ? 0 : (LowerIsBetter ? worst / best : best / worst);
            }
        }
    }

    public async Task RunAllAsync()
    {
        Console.WriteLine($"root            {root}");
        Console.WriteLine($"operations      {operations:N0}");
        Console.WriteLine($"value size      {valueSize} B");
        Console.WriteLine();

        await CompareWriteThroughputAsync();
        await CompareRandomReadsAsync();
        await CompareMissesAsync();
        await CompareStartupTimeAsync();
        await CompareSpaceAfterChurnAsync();
        await ReportScanThroughputAsync();

        Console.WriteLine();
        Console.WriteLine(
            "{0,-40} {1,-10} {2,14} {3,14} {4,10} {5,8}",
            "scenario", "unit", "bitcask", "lsm", "winner", "factor");
        Console.WriteLine(new string('-', 100));

        foreach (var row in _rows)
        {
            Console.WriteLine(
                "{0,-40} {1,-10} {2,14:N1} {3,14:N1} {4,10} {5,7:F1}x",
                row.Scenario, row.Unit, row.Bitcask, row.Lsm, row.Winner, row.Ratio);
        }

        Console.WriteLine();
        Console.WriteLine("Markdown:");
        Console.WriteLine();
        Console.WriteLine("| scenario | unit | bitcask | lsm | winner |");
        Console.WriteLine("| --- | --- | ---: | ---: | --- |");
        foreach (var row in _rows)
        {
            Console.WriteLine(string.Format(
                CultureInfo.InvariantCulture,
                "| {0} | {1} | {2:N1} | {3:N1} | {4} ({5:F1}x) |",
                row.Scenario, row.Unit, row.Bitcask, row.Lsm, row.Winner, row.Ratio));
        }
    }

    // ------------------------------------------------------------ scenarios

    private async Task CompareWriteThroughputAsync()
    {
        double bitcask = await MeasureAsync(EngineKind.Bitcask, "write", async engine =>
        {
            byte[] value = Payload(valueSize);
            var stopwatch = Stopwatch.StartNew();

            for (int i = 0; i < operations; i++)
            {
                await engine.PutAsync(KestrelCache.ByteKey.From($"key:{i:D12}"), value);
            }

            await engine.FlushAsync();
            stopwatch.Stop();
            return operations / stopwatch.Elapsed.TotalSeconds;
        });

        double lsm = await MeasureAsync(EngineKind.Lsm, "write", async engine =>
        {
            byte[] value = Payload(valueSize);
            var stopwatch = Stopwatch.StartNew();

            for (int i = 0; i < operations; i++)
            {
                await engine.PutAsync(KestrelCache.ByteKey.From($"key:{i:D12}"), value);
            }

            await engine.FlushAsync();
            stopwatch.Stop();
            return operations / stopwatch.Elapsed.TotalSeconds;
        });

        Add("sequential put", "ops/sec", bitcask, lsm, lowerIsBetter: false);
    }

    private async Task CompareRandomReadsAsync()
    {
        double bitcask = await MeasureAsync(EngineKind.Bitcask, "read", async engine =>
        {
            await PopulateAsync(engine, operations);
            return await TimeRandomReadsAsync(engine, operations);
        });

        double lsm = await MeasureAsync(EngineKind.Lsm, "read", async engine =>
        {
            await PopulateAsync(engine, operations);
            await engine.CompactAsync();
            return await TimeRandomReadsAsync(engine, operations);
        });

        Add("random get (hit)", "ops/sec", bitcask, lsm, lowerIsBetter: false);
    }

    /// <summary>
    /// Reads for keys that are not there. Bitcask answers from its hash index with no I/O at
    /// all; the LSM engine answers from Bloom filters and key-range metadata, which is nearly
    /// as cheap but not quite. This is the scenario Bloom filters exist for.
    /// </summary>
    private async Task CompareMissesAsync()
    {
        double bitcask = await MeasureAsync(EngineKind.Bitcask, "miss", async engine =>
        {
            await PopulateEvenKeysAsync(engine, operations);
            return await TimeMissesAsync(engine, operations);
        });

        double lsm = await MeasureAsync(EngineKind.Lsm, "miss", async engine =>
        {
            await PopulateEvenKeysAsync(engine, operations);
            await engine.CompactAsync();
            double throughput = await TimeMissesAsync(engine, operations);

            var stats = engine.GetStats();
            long consulted = stats.BloomFilterNegatives + stats.BloomFilterFalsePositives;
            Console.WriteLine(
                $"  lsm bloom: {stats.BloomFilterNegatives:N0} ruled out, "
                    + $"{stats.BloomFilterFalsePositives:N0} false positive(s)"
                    + (consulted > 0
                        ? $" = {(double)stats.BloomFilterFalsePositives / consulted:P2}"
                        : string.Empty));

            return throughput;
        });

        Add("random get (interleaved miss)", "ops/sec", bitcask, lsm, lowerIsBetter: false);
    }

    /// <summary>
    /// Startup time, and the sharpest difference between the two designs.
    /// </summary>
    /// <remarks>
    /// Bitcask keeps its index only in memory, so every restart replays the entire log to rebuild
    /// it — startup is O(records) and unavoidable. The LSM engine reads a manifest and opens a
    /// handful of files, so startup is essentially constant and independent of how much data it
    /// holds. At a million keys the first is a nuisance; at a billion it is the end of the
    /// design.
    /// </remarks>
    private async Task CompareStartupTimeAsync()
    {
        int records = Math.Min(operations, 300_000);

        double bitcask = await MeasureStartupAsync(EngineKind.Bitcask, records);
        double lsm = await MeasureStartupAsync(EngineKind.Lsm, records);

        Add($"open after {records:N0} writes", "ms", bitcask, lsm, lowerIsBetter: true);
    }

    private async Task<double> MeasureStartupAsync(EngineKind kind, int records)
    {
        string path = PathFor(kind, $"startup-{kind}");
        Reset(path);

        await using (var engine = await OpenAsync(kind, path))
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

            // Give the LSM engine the chance to get its data into tables, which is the state a
            // real restart would find it in.
            if (engine is LsmEngine lsm) await lsm.CompactAsync();
            await engine.FlushAsync();
        }

        var stopwatch = Stopwatch.StartNew();
        await using (var reopened = await OpenAsync(kind, path))
        {
            stopwatch.Stop();
            _ = reopened.GetStats();
        }

        Reset(path);
        return stopwatch.Elapsed.TotalMilliseconds;
    }

    /// <summary>
    /// Disk occupied after writing the same small key set over and over, which is where
    /// reclamation strategy shows up. Both engines reclaim, by different mechanisms: Bitcask
    /// rewrites its whole log, the LSM engine merges levels.
    /// </summary>
    private async Task CompareSpaceAfterChurnAsync()
    {
        const int keys = 2_000;
        int rounds = Math.Max(2, Math.Min(40, operations / keys));

        double bitcask = await MeasureAsync(EngineKind.Bitcask, "churn", async engine =>
        {
            await ChurnAsync(engine, keys, rounds);
            await engine.CompactAsync();
            return engine.GetStats().DiskSizeBytes / 1024.0 / 1024.0;
        });

        double lsm = await MeasureAsync(EngineKind.Lsm, "churn", async engine =>
        {
            await ChurnAsync(engine, keys, rounds);
            await engine.CompactAsync();
            return engine.GetStats().DiskSizeBytes / 1024.0 / 1024.0;
        });

        Add($"disk after {rounds} rewrites of {keys:N0} keys", "MiB", bitcask, lsm, lowerIsBetter: true);
    }

    /// <summary>
    /// Ordered iteration, which only one of the two engines can do at all. Reported on its own
    /// rather than as a comparison, because a hash index cannot produce sorted output without
    /// first sorting its entire keyspace — so there is no Bitcask number to put beside it.
    /// </summary>
    private async Task ReportScanThroughputAsync()
    {
        string path = PathFor(EngineKind.Lsm, "scan");
        Reset(path);

        await using var engine = await LsmEngine.OpenAsync(LsmOptions(path));
        await PopulateAsync(engine, operations);
        await engine.CompactAsync();

        var stopwatch = Stopwatch.StartNew();
        long scanned = 0;
        await foreach (var _ in engine.ScanAsync())
        {
            scanned++;
        }
        stopwatch.Stop();

        double throughput = scanned / stopwatch.Elapsed.TotalSeconds;
        Console.WriteLine(
            $"  lsm full scan: {scanned:N0} keys in {stopwatch.ElapsedMilliseconds:N0} ms "
                + $"= {throughput:N0} keys/sec");

        // A narrow range scan, to show the cost tracks the range rather than the data volume.
        int rangeStart = operations / 2;
        var rangeWatch = Stopwatch.StartNew();
        long inRange = 0;
        await foreach (var _ in engine.ScanAsync(
            KestrelCache.ByteKey.From($"key:{rangeStart:D12}"),
            KestrelCache.ByteKey.From($"key:{rangeStart + 1_000:D12}")))
        {
            inRange++;
        }
        rangeWatch.Stop();

        Console.WriteLine(
            $"  lsm range scan: {inRange:N0} keys in {rangeWatch.Elapsed.TotalMilliseconds:F1} ms "
                + $"out of {scanned:N0} total");

        Reset(path);

        _rows.Add(new ComparisonRow("full ordered scan", "keys/sec", 0, throughput, false));
    }

    // ------------------------------------------------------------ plumbing

    private async Task<double> MeasureAsync(
        EngineKind kind,
        string label,
        Func<IStorageEngine, Task<double>> body)
    {
        string path = PathFor(kind, $"{label}-{kind}");
        Reset(path);

        await using var engine = await OpenAsync(kind, path);
        double result = await body(engine);

        await engine.DisposeAsync();
        Reset(path);
        return result;
    }

    private static async Task<double> TimeRandomReadsAsync(IStorageEngine engine, int keyCount)
    {
        var random = new Random(12345);
        var stopwatch = Stopwatch.StartNew();

        for (int i = 0; i < keyCount; i++)
        {
            _ = await engine.GetAsync(KestrelCache.ByteKey.From($"key:{random.Next(keyCount):D12}"));
        }

        stopwatch.Stop();
        return keyCount / stopwatch.Elapsed.TotalSeconds;
    }

    private static async Task<double> TimeMissesAsync(IStorageEngine engine, int probes)
    {
        var stopwatch = Stopwatch.StartNew();

        for (int i = 1; i < probes * 2; i += 2)
        {
            _ = await engine.GetAsync(KestrelCache.ByteKey.From($"key:{i:D12}"));
        }

        stopwatch.Stop();
        return probes / stopwatch.Elapsed.TotalSeconds;
    }

    private static async Task ChurnAsync(IStorageEngine engine, int keys, int rounds)
    {
        byte[] value = Payload(512);

        for (int round = 0; round < rounds; round++)
        {
            var batch = new WriteBatch();
            for (int i = 0; i < keys; i++)
            {
                batch.Put(KestrelCache.ByteKey.From($"key:{i:D12}"), value);
                if (batch.Count >= 500)
                {
                    await engine.WriteAsync(batch);
                    batch.Clear();
                }
            }
            if (batch.Count > 0) await engine.WriteAsync(batch);
        }
    }

    private static async Task PopulateAsync(IStorageEngine engine, int count)
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

    /// <summary>
    /// Writes only even-numbered keys, so the miss benchmark can probe odd ones that fall inside
    /// every table's key range. Probing keys outside the range entirely would be answered by the
    /// cheap smallest/largest-key check and never reach a Bloom filter.
    /// </summary>
    private static async Task PopulateEvenKeysAsync(IStorageEngine engine, int count)
    {
        var batch = new WriteBatch();
        byte[] value = Payload(128);

        for (int i = 0; i < count * 2; i += 2)
        {
            batch.Put(KestrelCache.ByteKey.From($"key:{i:D12}"), value);
            if (batch.Count >= 1_000)
            {
                await engine.WriteAsync(batch);
                batch.Clear();
            }
        }
        if (batch.Count > 0) await engine.WriteAsync(batch);
        await engine.FlushAsync();
    }

    private string PathFor(EngineKind kind, string label) =>
        Path.Combine(root, kind == EngineKind.Bitcask ? $"{label}.kc" : label);

    private static ValueTask<IStorageEngine> OpenAsync(EngineKind kind, string path) =>
        kind == EngineKind.Bitcask
            ? Open(BitcaskEngine.OpenAsync(BitcaskOptions(path)))
            : Open(LsmEngine.OpenAsync(LsmOptions(path)));

    private static async ValueTask<IStorageEngine> Open<T>(ValueTask<T> task) where T : IStorageEngine =>
        await task.ConfigureAwait(false);

    private static DatabaseOptions BitcaskOptions(string path) => new()
    {
        Path = path,
        SyncPolicy = SyncPolicy.None,
        AutoCompactStaleRatio = 0,
    };

    private static DatabaseOptions LsmOptions(string path) => new()
    {
        Path = path,
        SyncPolicy = SyncPolicy.None,
        EnableBackgroundCompaction = false,
    };

    private static byte[] Payload(int size)
    {
        var value = new byte[size];
        new Random(4242).NextBytes(value);
        return value;
    }

    private void Add(string scenario, string unit, double bitcask, double lsm, bool lowerIsBetter) =>
        _rows.Add(new ComparisonRow(scenario, unit, bitcask, lsm, lowerIsBetter));

    private static void Reset(string path)
    {
        if (File.Exists(path)) File.Delete(path);
        if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
    }
}
