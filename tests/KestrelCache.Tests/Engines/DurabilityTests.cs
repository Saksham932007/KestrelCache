using System.Diagnostics;
using Xunit.Sdk;
using KestrelCache.Bitcask;
using Xunit;
using Xunit.Abstractions;

namespace KestrelCache.Tests;

/// <summary>
/// Asserts that each <see cref="SyncPolicy"/> issues the number of fsyncs it promises, and that
/// batching actually amortises them.
/// </summary>
/// <remarks>
/// Counting syscalls is the complement to the SIGKILL tests in
/// <see cref="CrashConsistencyTests"/>. Those prove the data comes back; these prove it was
/// made durable by the mechanism claimed, rather than surviving by the accident of a page cache
/// that outlived the test. Without this, a build where every <c>Flush(true)</c> had quietly
/// become <c>Flush()</c> would pass the entire rest of the suite.
/// </remarks>
[Collection(TimingSensitiveCollection.Name)]
public sealed class DurabilityTests(ITestOutputHelper output)
{
    private static DatabaseOptions Options(string path, SyncPolicy policy) => new()
    {
        Path = path,
        SyncPolicy = policy,
        AutoCompactStaleRatio = 0,
    };

    [Fact]
    public async Task EveryWrite_issues_one_fsync_per_write()
    {
        using var dir = new TempDirectory();
        await using var engine = await BitcaskEngine.OpenAsync(
            Options(dir.DbPath, SyncPolicy.EveryWrite));

        const int writes = 50;
        for (int i = 0; i < writes; i++)
        {
            await engine.PutAsync(TestData.Key($"k{i}"), TestData.Value("v"));
        }

        Assert.Equal(writes, engine.GetStats().Syncs);
    }

    [Fact]
    public async Task None_issues_no_fsync_until_asked()
    {
        using var dir = new TempDirectory();
        await using var engine = await BitcaskEngine.OpenAsync(
            Options(dir.DbPath, SyncPolicy.None));

        for (int i = 0; i < 50; i++)
        {
            await engine.PutAsync(TestData.Key($"k{i}"), TestData.Value("v"));
        }

        Assert.Equal(0, engine.GetStats().Syncs);

        await engine.FlushAsync();

        Assert.Equal(1, engine.GetStats().Syncs);
    }

    [Fact]
    public async Task Interval_syncs_in_the_background_far_less_often_than_it_writes()
    {
        using var dir = new TempDirectory();
        await using var engine = await BitcaskEngine.OpenAsync(
            Options(dir.DbPath, SyncPolicy.Interval) with
            {
                SyncInterval = TimeSpan.FromMilliseconds(50),
            });

        const int writes = 2_000;
        for (int i = 0; i < writes; i++)
        {
            await engine.PutAsync(TestData.Key($"k{i}"), TestData.Value("v"));
        }

        // Polled rather than slept on. A fixed delay assumes the background loop gets scheduled
        // within it, which on a loaded machine it sometimes does not -- and "0 fsyncs" then looks
        // like a durability bug rather than a starved thread.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (engine.GetStats().Syncs == 0 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(25);
        }

        long syncs = engine.GetStats().Syncs;
        output.WriteLine($"{writes} writes produced {syncs} fsync(s)");

        Assert.True(syncs > 0, "the interval policy never synced");
        Assert.True(syncs < writes / 10, $"expected far fewer than {writes / 10} syncs, got {syncs}");
    }

    /// <summary>
    /// The headline reason batching exists: an fsync costs milliseconds, an append costs
    /// microseconds, so a batch of N keys should cost one fsync rather than N.
    /// </summary>
    [Fact]
    public async Task A_batch_costs_one_fsync_regardless_of_its_size()
    {
        using var dir = new TempDirectory();
        await using var engine = await BitcaskEngine.OpenAsync(
            Options(dir.DbPath, SyncPolicy.EveryWrite));

        var batch = new WriteBatch();
        for (int i = 0; i < 500; i++)
        {
            batch.Put($"k{i}", $"v{i}");
        }

        await engine.WriteAsync(batch);

        var stats = engine.GetStats();
        Assert.Equal(1, stats.Syncs);
        Assert.Equal(500, stats.Writes);
        Assert.Equal(500, stats.KeyCount);
    }

    /// <summary>
    /// Confirms that fsync is a real syscall with a real cost, and therefore that the policy
    /// knob is worth having at all.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the one test that can tell <c>Flush(flushToDisk: true)</c> apart from plain
    /// <c>Flush()</c>. The counter assertions above only prove the engine <i>believes</i> it
    /// synced; a build where every fsync had silently become a page-cache flush would pass all
    /// of them, and would pass the SIGKILL tests too, because the page cache outlives a killed
    /// process. Only the cost gives it away.
    /// </para>
    /// <para>
    /// Two things have to be handled for the comparison to mean anything. First, warm-up: the
    /// first policy measured otherwise pays for JIT and file creation and can easily come out
    /// slower than the second whatever the policy. Second, the filesystem: on a tmpfs there is
    /// no device to write back to, so fsync returns immediately and the comparison is vacuous.
    /// <see cref="TempDirectory"/> defaults to a disk-backed directory for exactly this reason,
    /// but the check stays here because the location is overridable.
    /// </para>
    /// </remarks>
    [SkippableFact]
    public async Task Syncing_every_write_costs_measurably_more_than_not_syncing()
    {
        Skip.IfNot(
            TempDirectory.BackedByRealDevice,
            $"'{TempDirectory.Root}' is memory-backed, where fsync is a no-op. "
                + $"Set {TempDirectory.BaseDirectoryVariable} to a path on a real device.");

        const int writes = 300;
        using var dir = new TempDirectory("fsync-cost");

        // Warm up both paths so neither pays for JIT or first-touch allocation in the measurement.
        await TimeWritesAsync(dir.File("warm-none.kc"), SyncPolicy.None, 50);
        await TimeWritesAsync(dir.File("warm-sync.kc"), SyncPolicy.EveryWrite, 50);

        var unsynced = await TimeWritesAsync(dir.File("none.kc"), SyncPolicy.None, writes);
        var synced = await TimeWritesAsync(dir.File("sync.kc"), SyncPolicy.EveryWrite, writes);

        double perSyncMicroseconds =
            (synced - unsynced).TotalMicroseconds / writes;

        output.WriteLine(
            $"{writes} writes on {TempDirectory.Root}: "
                + $"none = {unsynced.TotalMilliseconds:F1} ms, "
                + $"everywrite = {synced.TotalMilliseconds:F1} ms "
                + $"=> ~{perSyncMicroseconds:F0} us per fsync");

        Assert.True(
            synced > unsynced,
            $"syncing every write ({synced.TotalMilliseconds:F1} ms) was not slower than never "
                + $"syncing ({unsynced.TotalMilliseconds:F1} ms), which means fsync is not "
                + "reaching the device");
    }

    private static async Task<TimeSpan> TimeWritesAsync(string path, SyncPolicy policy, int writes)
    {
        await using var engine = await BitcaskEngine.OpenAsync(Options(path, policy));

        byte[] value = new byte[256];
        var stopwatch = Stopwatch.StartNew();

        for (int i = 0; i < writes; i++)
        {
            await engine.PutAsync(TestData.Key($"k{i}"), value);
        }

        stopwatch.Stop();
        return stopwatch.Elapsed;
    }

    [Fact]
    public async Task Flush_forces_durability_regardless_of_policy()
    {
        using var dir = new TempDirectory();
        await using var engine = await BitcaskEngine.OpenAsync(
            Options(dir.DbPath, SyncPolicy.None));

        await engine.PutAsync(TestData.Key("k"), TestData.Value("v"));
        await engine.FlushAsync();

        Assert.True(engine.GetStats().Syncs >= 1);
    }
}
