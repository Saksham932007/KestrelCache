using KestrelCache.Bitcask;
using Xunit;

namespace KestrelCache.Tests;

/// <summary>
/// Tests for what happens when the file on disk is not what the engine left behind: torn tails
/// from interrupted appends, and genuine corruption from bit rot or a bad device.
/// </summary>
public sealed class RecoveryTests
{
    private static DatabaseOptions Options(string path) => new()
    {
        Path = path,
        SyncPolicy = SyncPolicy.EveryWrite,
        AutoCompactStaleRatio = 0,
    };

    private static async Task<string> SeedAsync(TempDirectory dir, int count = 10)
    {
        await using var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath));
        for (int i = 0; i < count; i++)
        {
            await engine.PutAsync(TestData.Key($"key:{i:D3}"), TestData.Value($"value-{i}"));
        }
        return dir.DbPath;
    }

    private static void Truncate(string path, long bytesToRemove)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.ReadWrite);
        stream.SetLength(stream.Length - bytesToRemove);
    }

    private static void FlipByte(string path, long offset)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.ReadWrite);
        stream.Position = offset;
        int original = stream.ReadByte();
        stream.Position = offset;
        stream.WriteByte((byte)(original ^ 0xFF));
    }

    // ------------------------------------------------------------ torn tails

    [Fact]
    public async Task A_tail_truncated_mid_value_is_discarded_and_earlier_records_survive()
    {
        using var dir = new TempDirectory();
        await SeedAsync(dir);

        Truncate(dir.DbPath, 4);

        await using var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        Assert.False(engine.Recovery.Clean);
        Assert.True(engine.Recovery.BytesTruncated > 0);
        Assert.Equal(9, engine.Recovery.LiveKeys);
        Assert.Equal("value-0", TestData.Text(await engine.GetAsync(TestData.Key("key:000"))));
        Assert.Null(await engine.GetAsync(TestData.Key("key:009")));
    }

    [Fact]
    public async Task A_tail_truncated_mid_header_is_discarded()
    {
        using var dir = new TempDirectory();
        await SeedAsync(dir);

        // Leave fewer bytes than a full record header, which cannot be anything but a torn write.
        long valueLength = TestData.Value("value-9").Length;
        Truncate(dir.DbPath, valueLength + 8);

        await using var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        Assert.True(engine.Recovery.BytesTruncated > 0);
        Assert.Equal(9, engine.Recovery.LiveKeys);
    }

    [Fact]
    public async Task A_truncated_database_is_writable_again_after_recovery()
    {
        using var dir = new TempDirectory();
        await SeedAsync(dir);
        Truncate(dir.DbPath, 4);

        await using (var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath)))
        {
            await engine.PutAsync(TestData.Key("fresh"), TestData.Value("after-recovery"));
        }

        await using var reopened = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        Assert.True(reopened.Recovery.Clean);
        Assert.Equal("after-recovery", TestData.Text(await reopened.GetAsync(TestData.Key("fresh"))));
        Assert.Equal("value-0", TestData.Text(await reopened.GetAsync(TestData.Key("key:000"))));
    }

    [Fact]
    public async Task Tail_truncation_can_be_refused()
    {
        using var dir = new TempDirectory();
        await SeedAsync(dir);
        Truncate(dir.DbPath, 4);

        var strict = Options(dir.DbPath) with { TruncateCorruptTail = false };

        await Assert.ThrowsAsync<CorruptRecordException>(
            async () => await BitcaskEngine.OpenAsync(strict));
    }

    // ------------------------------------------------------------ real corruption

    /// <summary>
    /// Bit rot in the middle of the log must not be silently papered over. An interrupted append
    /// can only ever damage the final record, so a bad checksum with valid records after it
    /// means something else went wrong — and truncating there would throw away good data.
    /// </summary>
    [Fact]
    public async Task Corruption_in_the_middle_of_the_log_is_reported_not_truncated()
    {
        using var dir = new TempDirectory();
        await SeedAsync(dir, count: 20);

        // Corrupt the value bytes of the very first record.
        FlipByte(dir.DbPath, RecordFormat.FileHeaderSize + RecordFormat.HeaderSize + 2);

        var exception = await Assert.ThrowsAsync<CorruptRecordException>(
            async () => await BitcaskEngine.OpenAsync(Options(dir.DbPath)));

        Assert.Contains("not at the end of the log", exception.Message);
    }

    [Fact]
    public async Task A_corrupt_file_header_is_rejected()
    {
        using var dir = new TempDirectory();
        await SeedAsync(dir);

        FlipByte(dir.DbPath, 1);

        var exception = await Assert.ThrowsAsync<CorruptRecordException>(
            async () => await BitcaskEngine.OpenAsync(Options(dir.DbPath)));

        Assert.Contains("magic", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// A flipped bit inside a length field previously reached <c>new byte[keyLen]</c>
    /// unexamined, so a damaged header could ask for a multi-gigabyte allocation. It must now
    /// produce a diagnosable error instead.
    /// </summary>
    [Fact]
    public async Task An_implausible_length_field_is_rejected_without_allocating()
    {
        using var dir = new TempDirectory();
        await SeedAsync(dir, count: 1);

        // Set the key-length field of the only record to a huge value.
        using (var stream = new FileStream(dir.DbPath, FileMode.Open, FileAccess.ReadWrite))
        {
            stream.Position = RecordFormat.FileHeaderSize + 4;
            stream.Write(BitConverter.GetBytes(int.MaxValue));
        }

        await using var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath));

        // Treated as tail damage: the record is unparseable, so nothing after it can be trusted.
        Assert.Equal(0, engine.Recovery.LiveKeys);
        Assert.True(engine.Recovery.BytesTruncated > 0);
    }

    [Fact]
    public async Task A_negative_length_field_is_rejected()
    {
        using var dir = new TempDirectory();
        await SeedAsync(dir, count: 1);

        using (var stream = new FileStream(dir.DbPath, FileMode.Open, FileAccess.ReadWrite))
        {
            stream.Position = RecordFormat.FileHeaderSize + 8;
            stream.Write(BitConverter.GetBytes(-12345));
        }

        var strict = Options(dir.DbPath) with { TruncateCorruptTail = false };

        await Assert.ThrowsAsync<CorruptRecordException>(
            async () => await BitcaskEngine.OpenAsync(strict));
    }

    /// <summary>
    /// Flipping a single bit anywhere in the file must never produce a wrong answer: the engine
    /// may refuse to open, may lose the tail, or may be unaffected, but it must not hand back
    /// data that was not written.
    /// </summary>
    [Fact]
    public async Task No_single_bit_flip_ever_yields_a_wrong_value()
    {
        using var dir = new TempDirectory();
        var expected = new Dictionary<string, string>();

        await using (var engine = await BitcaskEngine.OpenAsync(Options(dir.DbPath)))
        {
            for (int i = 0; i < 8; i++)
            {
                string key = $"key:{i:D3}";
                string value = $"value-{i}";
                await engine.PutAsync(TestData.Key(key), TestData.Value(value));
                expected[key] = value;
            }
        }

        byte[] pristine = await File.ReadAllBytesAsync(dir.DbPath);

        for (int offset = 0; offset < pristine.Length; offset++)
        {
            string victim = dir.File($"flip-{offset}.kc");
            byte[] mutated = (byte[])pristine.Clone();
            mutated[offset] ^= 0x01;
            await File.WriteAllBytesAsync(victim, mutated);

            try
            {
                await using var engine = await BitcaskEngine.OpenAsync(Options(victim));

                foreach (var (key, value) in expected)
                {
                    string? actual;
                    try
                    {
                        actual = TestData.Text(await engine.GetAsync(TestData.Key(key)));
                    }
                    catch (CorruptRecordException)
                    {
                        // Detected on read. Acceptable: it refused rather than lied.
                        continue;
                    }

                    // The only acceptable answers are the true value, or absent (the record was
                    // in the discarded tail). Anything else means corruption went undetected.
                    Assert.True(
                        actual is null || actual == value,
                        $"bit flip at offset {offset} made {key} read back as '{actual}' "
                            + $"instead of '{value}' or null");
                }
            }
            catch (CorruptRecordException)
            {
                // Refused to open. Acceptable.
            }
            finally
            {
                File.Delete(victim);
            }
        }
    }
}
