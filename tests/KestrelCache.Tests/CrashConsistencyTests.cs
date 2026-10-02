using KestrelCache.Bitcask;
using KestrelCache.Diagnostics;
using Xunit;
using Xunit.Abstractions;

namespace KestrelCache.Tests;

/// <summary>
/// Durability tests that kill a real process with SIGKILL partway through writing.
/// </summary>
/// <remarks>
/// <para>
/// The contract under test is the one a database lives or dies by: <b>an acknowledged write must
/// still be there afterwards</b>. Verifying it requires a separate process, because the
/// distinction that matters — data in the OS page cache versus data on the device — is invisible
/// from inside the process that wrote it. An in-process test passes whether or not fsync was
/// ever called, since the page cache outlives the test either way.
/// </para>
/// <para>
/// These tests kill the writer with SIGKILL, which no handler can intercept, then reopen the
/// database in this process and check every acknowledgement the child managed to print.
/// </para>
/// <para>
/// One honest limitation worth stating: SIGKILL simulates a process crash, not a power cut.
/// Writes that reached the page cache but not the platter still survive it, so these tests prove
/// crash consistency and prove the recovery path, but they cannot prove that fsync was issued.
/// Proving that needs the kernel out of the picture too — a VM reset, or a device-mapper
/// flakey target. The fsync call site is covered instead by asserting the syscall count in
/// <see cref="DurabilityTests"/>.
/// </para>
/// </remarks>
[Collection("crash")]
public sealed class CrashConsistencyTests(ITestOutputHelper output)
{
    private const int ValueSize = 64;

    private static DatabaseOptions ReadOptions(string path) => new()
    {
        Path = path,
        SyncPolicy = SyncPolicy.None,
        AutoCompactStaleRatio = 0,
    };

    [Theory]
    [InlineData(200)]
    [InlineData(1_000)]
    [InlineData(5_000)]
    public async Task Every_acknowledged_write_survives_sigkill(int acknowledgementsBeforeKill)
    {
        using var dir = new TempDirectory("crash");

        using var writer = await CrashWriterProcess.StartAsync(
            dir.DbPath, EngineKind.Bitcask, SyncPolicy.EveryWrite, valueSize: ValueSize);

        await writer.WaitForAcknowledgementsAsync(
            acknowledgementsBeforeKill, TimeSpan.FromSeconds(60));

        var acknowledged = await writer.KillAsync();
        output.WriteLine($"child acknowledged {acknowledged.Count} write(s) before SIGKILL");

        await using var engine = await BitcaskEngine.OpenAsync(ReadOptions(dir.DbPath));
        output.WriteLine(
            $"recovery: {engine.Recovery.RecordsReplayed} record(s) replayed, "
                + $"{engine.Recovery.BytesTruncated} byte(s) of torn tail discarded");

        foreach (string key in acknowledged)
        {
            byte[]? raw = await engine.GetAsync(TestData.Key(key));
            Assert.NotNull(raw);
            Assert.Equal(DeterministicPayload.ValueFor(key, ValueSize), TestData.Text(raw));
        }
    }

    /// <summary>
    /// A crash may lose unacknowledged writes, but it must never leave the database unopenable
    /// or holding a value that was never written.
    /// </summary>
    [Fact]
    public async Task A_crash_leaves_a_recoverable_database_even_without_fsync()
    {
        using var dir = new TempDirectory("crash-nosync");

        using var writer = await CrashWriterProcess.StartAsync(
            dir.DbPath, EngineKind.Bitcask, SyncPolicy.None, valueSize: ValueSize);

        await writer.WaitForAcknowledgementsAsync(5_000, TimeSpan.FromSeconds(60));
        var acknowledged = await writer.KillAsync();

        await using var engine = await BitcaskEngine.OpenAsync(ReadOptions(dir.DbPath));

        int present = 0;
        foreach (string key in acknowledged)
        {
            byte[]? raw = await engine.GetAsync(TestData.Key(key));
            if (raw is null) continue;

            // Whatever survived must be exactly what was written. A wrong value is a far worse
            // failure than a missing one.
            Assert.Equal(DeterministicPayload.ValueFor(key, ValueSize), TestData.Text(raw));
            present++;
        }

        output.WriteLine(
            $"{present} of {acknowledged.Count} acknowledged write(s) survived without fsync");

        // The database must still be usable afterwards.
        await engine.PutAsync(TestData.Key("post-crash"), TestData.Value("ok"));
        Assert.Equal("ok", TestData.Text(await engine.GetAsync(TestData.Key("post-crash"))));
    }

    /// <summary>
    /// Writes land in the log in order, so what survives a crash must be a prefix of what was
    /// written. A gap — key 500 present while key 400 is missing — would mean the log was
    /// replayed out of order or a record was skipped.
    /// </summary>
    [Fact]
    public async Task What_survives_a_crash_is_a_prefix_of_what_was_written()
    {
        using var dir = new TempDirectory("crash-prefix");

        using var writer = await CrashWriterProcess.StartAsync(
            dir.DbPath, EngineKind.Bitcask, SyncPolicy.Interval, valueSize: ValueSize);

        await writer.WaitForAcknowledgementsAsync(8_000, TimeSpan.FromSeconds(60));
        var acknowledged = await writer.KillAsync();

        await using var engine = await BitcaskEngine.OpenAsync(ReadOptions(dir.DbPath));

        bool seenMissing = false;
        for (int i = 0; i < acknowledged.Count; i++)
        {
            string key = acknowledged[i];
            bool present = await engine.GetAsync(TestData.Key(key)) is not null;

            if (!present)
            {
                seenMissing = true;
                continue;
            }

            Assert.False(
                seenMissing,
                $"'{key}' (ack #{i}) survived although an earlier acknowledged write did not; "
                    + "the surviving set is not a prefix of the written sequence");
        }
    }

    /// <summary>
    /// Batch atomicity under crash: the writer commits in batches of ten, so recovery must never
    /// surface a partially-applied batch.
    /// </summary>
    [Fact]
    public async Task A_crash_never_leaves_a_half_applied_batch()
    {
        const int batchSize = 10;
        using var dir = new TempDirectory("crash-batch");

        using var writer = await CrashWriterProcess.StartAsync(
            dir.DbPath,
            EngineKind.Bitcask,
            SyncPolicy.EveryWrite,
            batchSize: batchSize,
            valueSize: ValueSize);

        await writer.WaitForAcknowledgementsAsync(2_000, TimeSpan.FromSeconds(60));
        await writer.KillAsync();

        await using var engine = await BitcaskEngine.OpenAsync(ReadOptions(dir.DbPath));
        long recovered = engine.GetStats().KeyCount;

        output.WriteLine(
            $"recovered {recovered} key(s); discarded "
                + $"{engine.Recovery.UncommittedBatchRecordsDiscarded} record(s) of a torn batch");

        // Keys are written in a strict sequence, so an atomic batch boundary is a multiple of the
        // batch size. Anything else means a batch was applied in part.
        Assert.Equal(0, recovered % batchSize);

        for (long i = 0; i < recovered; i++)
        {
            string key = $"key:{i:D9}";
            byte[]? raw = await engine.GetAsync(TestData.Key(key));
            Assert.NotNull(raw);
            Assert.Equal(DeterministicPayload.ValueFor(key, ValueSize), TestData.Text(raw));
        }
    }

    /// <summary>
    /// Killing the process repeatedly, each time resuming from whatever the last crash left
    /// behind, exercises recovery against a file that has already been recovered once.
    /// </summary>
    [Fact]
    public async Task Repeated_crashes_and_recoveries_never_corrupt_the_database()
    {
        using var dir = new TempDirectory("crash-repeat");
        var allAcknowledged = new List<string>();

        for (int round = 0; round < 5; round++)
        {
            using var writer = await CrashWriterProcess.StartAsync(
                dir.DbPath, EngineKind.Bitcask, SyncPolicy.EveryWrite, valueSize: ValueSize);

            await writer.WaitForAcknowledgementsAsync(300, TimeSpan.FromSeconds(60));
            var acknowledged = await writer.KillAsync();
            allAcknowledged.AddRange(acknowledged);

            await using var engine = await BitcaskEngine.OpenAsync(ReadOptions(dir.DbPath));

            foreach (string key in acknowledged)
            {
                byte[]? raw = await engine.GetAsync(TestData.Key(key));
                Assert.NotNull(raw);
                Assert.Equal(DeterministicPayload.ValueFor(key, ValueSize), TestData.Text(raw));
            }

            output.WriteLine(
                $"round {round}: {acknowledged.Count} ack(s), "
                    + $"{engine.Recovery.BytesTruncated} byte(s) truncated");
        }

        // The writer restarts its key sequence each round, so the union is what must be present.
        await using var final = await BitcaskEngine.OpenAsync(ReadOptions(dir.DbPath));
        foreach (string key in allAcknowledged.Distinct())
        {
            Assert.NotNull(await final.GetAsync(TestData.Key(key)));
        }
    }
}

/// <summary>
/// Crash tests spawn processes and are I/O bound, so they run one at a time rather than
/// competing with each other for the disk.
/// </summary>
[CollectionDefinition("crash", DisableParallelization = true)]
public sealed class CrashCollection;
