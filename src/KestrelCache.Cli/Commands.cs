using System.Diagnostics;
using System.Text;
using KestrelCache.Diagnostics;

namespace KestrelCache.Cli;

internal static class Commands
{
    private const string DefaultPath = "./kc-data";

    private static DatabaseOptions BuildOptions(Args args) => new()
    {
        Path = args.String("path", DefaultPath),
        SyncPolicy = args.Enum("sync", SyncPolicy.Interval),
    };

    private static EngineKind Engine(Args args) => args.Enum("engine", EngineKind.Bitcask);

    private static ValueTask<KestrelDb> OpenAsync(Args args) =>
        KestrelDb.OpenAsync(BuildOptions(args), Engine(args));

    // ---------------------------------------------------------------- basics

    internal static async Task<int> PutAsync(Args args)
    {
        await using var db = await OpenAsync(args);
        await db.PutAsync(args.Require("key"), args.Require("value"));
        Console.WriteLine("OK");
        return 0;
    }

    internal static async Task<int> GetAsync(Args args)
    {
        await using var db = await OpenAsync(args);
        string? value = await db.GetAsync(args.Require("key"));

        if (value is null)
        {
            Console.Error.WriteLine("(nil)");
            return 1;
        }

        Console.WriteLine(value);
        return 0;
    }

    internal static async Task<int> DeleteAsync(Args args)
    {
        await using var db = await OpenAsync(args);
        await db.DeleteAsync(args.Require("key"));
        Console.WriteLine("OK");
        return 0;
    }

    internal static async Task<int> ScanAsync(Args args)
    {
        await using var db = await OpenAsync(args);

        int limit = args.Int("limit", int.MaxValue);
        int shown = 0;

        var pairs = args.Value("prefix") is { } prefix
            ? db.ScanPrefixAsync(prefix)
            : db.ScanAsync(args.Value("start"), args.Value("end"));

        await foreach (var (key, value) in pairs)
        {
            Console.WriteLine($"{key}\t{value}");
            if (++shown >= limit) break;
        }

        Console.Error.WriteLine($"({shown} key(s))");
        return 0;
    }

    internal static async Task<int> StatsAsync(Args args)
    {
        await using var db = await OpenAsync(args);
        var stats = db.GetStats();

        Console.WriteLine($"engine                  {stats.Engine}");
        Console.WriteLine($"keys                    {stats.KeyCount:N0}");
        Console.WriteLine($"disk size               {Humanise(stats.DiskSizeBytes)}");
        Console.WriteLine($"live data               {Humanise(stats.LiveDataBytes)}");
        Console.WriteLine($"stale ratio             {stats.StaleRatio:P1}");
        Console.WriteLine($"reads / writes / dels   {stats.Reads:N0} / {stats.Writes:N0} / {stats.Deletes:N0}");
        Console.WriteLine($"fsyncs                  {stats.Syncs:N0}");
        Console.WriteLine($"compactions             {stats.Compactions:N0}");

        if (stats.SsTablesPerLevel.Count > 0)
        {
            Console.WriteLine($"sstables per level      [{string.Join(", ", stats.SsTablesPerLevel)}]");
            Console.WriteLine($"block cache hit rate    {stats.BlockCacheHitRate:P1}");
            Console.WriteLine($"bloom negatives         {stats.BloomFilterNegatives:N0}");
            Console.WriteLine($"bloom false positives   {stats.BloomFilterFalsePositives:N0}");
        }

        return 0;
    }

    internal static async Task<int> CompactAsync(Args args)
    {
        await using var db = await OpenAsync(args);

        var before = db.GetStats();
        var stopwatch = Stopwatch.StartNew();
        await db.CompactAsync();
        stopwatch.Stop();
        var after = db.GetStats();

        Console.WriteLine(
            $"compacted in {stopwatch.ElapsedMilliseconds:N0} ms: "
                + $"{Humanise(before.DiskSizeBytes)} -> {Humanise(after.DiskSizeBytes)}");
        return 0;
    }

    // ---------------------------------------------------------------- load generator

    internal static async Task<int> LoadAsync(Args args)
    {
        int count = args.Int("count", 100_000);
        int valueSize = args.Int("value-size", 128);
        int batchSize = args.Int("batch", 1);

        await using var db = await OpenAsync(args);

        byte[] payload = new byte[valueSize];
        Random.Shared.NextBytes(payload);
        string value = Convert.ToBase64String(payload)[..Math.Min(valueSize, 4 * valueSize / 3)];

        var stopwatch = Stopwatch.StartNew();

        if (batchSize <= 1)
        {
            for (int i = 0; i < count; i++)
            {
                await db.PutAsync($"key:{i:D12}", value);
            }
        }
        else
        {
            var batch = new WriteBatch();
            for (int i = 0; i < count; i++)
            {
                batch.Put($"key:{i:D12}", value);
                if (batch.Count >= batchSize)
                {
                    await db.WriteAsync(batch);
                    batch.Clear();
                }
            }
            if (batch.Count > 0) await db.WriteAsync(batch);
        }

        await db.FlushAsync();
        stopwatch.Stop();

        double seconds = stopwatch.Elapsed.TotalSeconds;
        Console.WriteLine(
            $"wrote {count:N0} keys of {valueSize} B in {seconds:F2} s "
                + $"= {count / seconds:N0} ops/s (batch size {Math.Max(1, batchSize)})");

        var stats = db.GetStats();
        Console.WriteLine($"disk {Humanise(stats.DiskSizeBytes)}, fsyncs {stats.Syncs:N0}");
        return 0;
    }

    // ---------------------------------------------------------------- crash harness

    /// <summary>
    /// Writes keys forever, printing a line to stdout only after each write has been
    /// acknowledged, and is designed to be killed with SIGKILL partway through.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the other half of the crash-consistency tests. The durability contract under test
    /// is: <i>if a write was acknowledged, it must still be there after the process dies</i>. The
    /// only way to test that honestly is to actually kill a real process, because the thing being
    /// verified is what reached the filesystem, and an in-process test can never distinguish
    /// "flushed to the page cache" from "written to disk" — the page cache survives the test
    /// either way.
    /// </para>
    /// <para>
    /// Hence the separate executable: the parent test spawns it, records every ACK line it
    /// prints, sends SIGKILL at an unpredictable moment, then reopens the database and checks
    /// that every acknowledged key is present and correct. <c>kill -9</c> gives the child no
    /// chance to flush anything, so nothing can be papered over by orderly shutdown.
    /// </para>
    /// </remarks>
    internal static async Task<int> CrashWriterAsync(Args args)
    {
        int count = args.Int("count", int.MaxValue);
        int valueSize = args.Int("value-size", 64);
        int batchSize = args.Int("batch", 1);

        await using var db = await OpenAsync(args);

        // Unbuffered stdout: a buffered ACK is a lie, because the test would believe a write was
        // acknowledged when the line was still sitting in this process's memory.
        var stdout = new StreamWriter(Console.OpenStandardOutput(), new UTF8Encoding(false))
        {
            AutoFlush = true,
        };

        stdout.WriteLine("READY");

        var batch = new WriteBatch();
        var pending = new List<string>(Math.Max(1, batchSize));

        for (int i = 0; i < count; i++)
        {
            string key = $"key:{i:D9}";
            string value = DeterministicPayload.ValueFor(key, valueSize);

            if (batchSize <= 1)
            {
                await db.PutAsync(key, value);
                stdout.WriteLine($"ACK {key}");
            }
            else
            {
                batch.Put(key, value);
                pending.Add(key);

                if (batch.Count >= batchSize)
                {
                    await db.WriteAsync(batch);
                    foreach (string acked in pending)
                    {
                        stdout.WriteLine($"ACK {acked}");
                    }
                    batch.Clear();
                    pending.Clear();
                }
            }
        }

        stdout.WriteLine("DONE");
        return 0;
    }

    // ---------------------------------------------------------------- demo

    internal static async Task<int> DemoAsync(Args args)
    {
        string path = args.String("path", Path.Combine(Path.GetTempPath(), $"kestrel-demo-{Guid.NewGuid():N}"));
        var engineKind = Engine(args);

        Console.WriteLine($"Opening a {engineKind} database at {path}");
        await using var db = await KestrelDb.OpenAsync(
            new DatabaseOptions { Path = path, SyncPolicy = SyncPolicy.EveryWrite },
            engineKind);

        Console.WriteLine();
        Console.WriteLine("1. Writing two records");
        await db.PutAsync("user:1", """{"name":"Alice","email":"alice@example.com"}""");
        await db.PutAsync("user:2", """{"name":"Bob","email":"bob@example.com"}""");
        Console.WriteLine($"   user:1 -> {await db.GetAsync("user:1")}");

        Console.WriteLine();
        Console.WriteLine("2. Deleting one of them");
        await db.DeleteAsync("user:2");
        Console.WriteLine($"   user:2 -> {await db.GetAsync("user:2") ?? "(nil)"}");

        Console.WriteLine();
        Console.WriteLine("3. An atomic batch");
        await db.WriteAsync(new WriteBatch()
            .Put("order:1", """{"total":42.00}""")
            .Put("order:2", """{"total":17.50}""")
            .Delete("user:1"));
        Console.WriteLine($"   order:1 -> {await db.GetAsync("order:1")}");
        Console.WriteLine($"   user:1  -> {await db.GetAsync("user:1") ?? "(nil)"}");

        if (db.Engine is IScannableStorageEngine)
        {
            Console.WriteLine();
            Console.WriteLine("4. An ordered prefix scan");
            await foreach (var (key, value) in db.ScanPrefixAsync("order:"))
            {
                Console.WriteLine($"   {key} -> {value}");
            }
        }
        else
        {
            Console.WriteLine();
            Console.WriteLine($"4. Ordered scans are unavailable: the {db.Engine.Name} engine keeps");
            Console.WriteLine("   an unordered hash index. Re-run with --engine lsm.");
        }

        Console.WriteLine();
        Console.WriteLine("5. Counters");
        var stats = db.GetStats();
        Console.WriteLine($"   {stats.KeyCount} live key(s), {Humanise(stats.DiskSizeBytes)} on disk, "
            + $"{stats.Syncs} fsync(s)");

        Console.WriteLine();
        Console.WriteLine("Done.");
        return 0;
    }

    internal static string Humanise(long bytes) => bytes switch
    {
        < 1024 => $"{bytes} B",
        < 1024 * 1024 => $"{bytes / 1024.0:F1} KiB",
        < 1024L * 1024 * 1024 => $"{bytes / (1024.0 * 1024):F1} MiB",
        _ => $"{bytes / (1024.0 * 1024 * 1024):F2} GiB",
    };
}
