using KestrelCache.Lsm;
using Xunit;
using Xunit.Abstractions;

namespace KestrelCache.Tests;

/// <summary>
/// Tests for the SSTable layer on its own: block framing, prefix compression, the Bloom filter,
/// and the two-level iterator. Exercised directly rather than only through the engine, so a
/// failure points at the format instead of at everything above it.
/// </summary>
public sealed class SsTableTests(ITestOutputHelper output)
{
    private static DatabaseOptions Options(string path, int blockSize = 4096) => new()
    {
        Path = path,
        BlockSizeBytes = blockSize,
        BloomBitsPerKey = 10,
    };

    private static async Task<(SsTableMeta Meta, string Path)> WriteTableAsync(
        TempDirectory dir,
        IEnumerable<(string Key, string? Value)> entries,
        DatabaseOptions? options = null,
        ulong startSequence = 1)
    {
        string path = dir.File("000001.sst");
        options ??= Options(dir.Path);

        await using var writer = new SsTableWriter(path, options);
        ulong sequence = startSequence;

        foreach (var (key, value) in entries)
        {
            byte[] internalKey = InternalKey.Encode(
                ByteKey.From(key),
                sequence++,
                value is null ? ValueKind.Deletion : ValueKind.Value);

            await writer.AddAsync(internalKey, value is null ? [] : ByteKey.From(value));
        }

        var meta = await writer.FinishAsync(fileNumber: 1);
        return (meta, path);
    }

    [Fact]
    public async Task A_table_round_trips_every_entry()
    {
        using var dir = new TempDirectory();

        var entries = Enumerable.Range(0, 1_000)
            .Select(i => ($"key:{i:D6}", (string?)$"value-{i}"))
            .ToArray();

        var (_, path) = await WriteTableAsync(dir, entries);

        using var reader = SsTableReader.Open(path, fileNumber: 1, cache: null);

        for (int i = 0; i < entries.Length; i++)
        {
            var result = reader.TryGet(ByteKey.From(entries[i].Item1), ulong.MaxValue, out byte[]? value);
            Assert.Equal(LookupResult.Found, result);
            Assert.Equal(entries[i].Item2, ByteKey.ToString(value!));
        }
    }

    [Fact]
    public async Task Absent_keys_are_reported_as_not_found()
    {
        using var dir = new TempDirectory();

        var entries = Enumerable.Range(0, 200)
            .Select(i => ($"key:{i:D6}", (string?)$"v{i}"))
            .ToArray();

        var (_, path) = await WriteTableAsync(dir, entries);
        using var reader = SsTableReader.Open(path, fileNumber: 1, cache: null);

        foreach (string missing in (string[])["aaa", "key:999999", "zzz", "key:000000x"])
        {
            Assert.Equal(
                LookupResult.NotFound,
                reader.TryGet(ByteKey.From(missing), ulong.MaxValue, out _));
        }
    }

    [Fact]
    public async Task Tombstones_are_reported_as_deleted()
    {
        using var dir = new TempDirectory();

        var (_, path) = await WriteTableAsync(dir,
        [
            ("alive", "yes"),
            ("dead", null),
        ]);

        using var reader = SsTableReader.Open(path, fileNumber: 1, cache: null);

        Assert.Equal(LookupResult.Found, reader.TryGet(ByteKey.From("alive"), ulong.MaxValue, out _));
        Assert.Equal(LookupResult.Deleted, reader.TryGet(ByteKey.From("dead"), ulong.MaxValue, out _));
    }

    [Fact]
    public async Task Metadata_records_the_key_and_sequence_range()
    {
        using var dir = new TempDirectory();

        var entries = Enumerable.Range(0, 50)
            .Select(i => ($"key:{i:D4}", (string?)$"v{i}"))
            .ToArray();

        var (meta, _) = await WriteTableAsync(dir, entries, startSequence: 100);

        Assert.Equal("key:0000", ByteKey.ToString(meta.SmallestUserKey));
        Assert.Equal("key:0049", ByteKey.ToString(meta.LargestUserKey));
        Assert.Equal(100UL, meta.SmallestSequence);
        Assert.Equal(149UL, meta.LargestSequence);
        Assert.Equal(50, meta.EntryCount);
        Assert.True(meta.FileSizeBytes > 0);
    }

    // ------------------------------------------------------------ snapshots

    [Fact]
    public async Task A_snapshot_sequence_hides_newer_versions_of_a_key()
    {
        using var dir = new TempDirectory();
        string path = dir.File("000001.sst");

        await using (var writer = new SsTableWriter(path, Options(dir.Path)))
        {
            // Three versions of one key. They must be written newest-first, which is the order
            // the internal key comparator imposes.
            await writer.AddAsync(
                InternalKey.Encode(ByteKey.From("k"), 30, ValueKind.Value), ByteKey.From("v30"));
            await writer.AddAsync(
                InternalKey.Encode(ByteKey.From("k"), 20, ValueKind.Value), ByteKey.From("v20"));
            await writer.AddAsync(
                InternalKey.Encode(ByteKey.From("k"), 10, ValueKind.Value), ByteKey.From("v10"));
            await writer.FinishAsync(1);
        }

        using var reader = SsTableReader.Open(path, fileNumber: 1, cache: null);

        Assert.Equal(LookupResult.Found, reader.TryGet(ByteKey.From("k"), 35, out byte[]? latest));
        Assert.Equal("v30", ByteKey.ToString(latest!));

        Assert.Equal(LookupResult.Found, reader.TryGet(ByteKey.From("k"), 25, out byte[]? mid));
        Assert.Equal("v20", ByteKey.ToString(mid!));

        Assert.Equal(LookupResult.Found, reader.TryGet(ByteKey.From("k"), 10, out byte[]? old));
        Assert.Equal("v10", ByteKey.ToString(old!));

        // Before any version existed.
        Assert.Equal(LookupResult.NotFound, reader.TryGet(ByteKey.From("k"), 5, out _));
    }

    // ------------------------------------------------------------ iteration

    [Fact]
    public async Task The_iterator_yields_every_entry_in_key_order()
    {
        using var dir = new TempDirectory();

        // A small block size forces many blocks, so the two-level iterator's block-to-block
        // transition is actually exercised rather than everything fitting in one block.
        var options = Options(dir.Path, blockSize: 256);

        var entries = Enumerable.Range(0, 2_000)
            .Select(i => ($"key:{i:D6}", (string?)$"value-{i}"))
            .ToArray();

        var (_, path) = await WriteTableAsync(dir, entries, options);
        using var reader = SsTableReader.Open(path, fileNumber: 1, cache: null);

        using var iterator = reader.CreateIterator();
        iterator.SeekToFirst();

        var seen = new List<string>();
        while (iterator.IsValid)
        {
            seen.Add(ByteKey.ToString(InternalKey.UserKey(iterator.Key)));
            if (!iterator.MoveNext()) break;
        }

        Assert.Equal(entries.Length, seen.Count);
        Assert.Equal(entries.Select(e => e.Item1), seen);
    }

    [Fact]
    public async Task The_iterator_can_seek_into_the_middle_of_a_table()
    {
        using var dir = new TempDirectory();
        var options = Options(dir.Path, blockSize: 256);

        var entries = Enumerable.Range(0, 1_000)
            .Select(i => ($"key:{i:D6}", (string?)$"v{i}"))
            .ToArray();

        var (_, path) = await WriteTableAsync(dir, entries, options);
        using var reader = SsTableReader.Open(path, fileNumber: 1, cache: null);

        using var iterator = reader.CreateIterator();
        iterator.Seek(InternalKey.Encode(ByteKey.From("key:000500"), InternalKey.MaxSequence, ValueKind.Value));

        Assert.True(iterator.IsValid);
        Assert.Equal("key:000500", ByteKey.ToString(InternalKey.UserKey(iterator.Key)));
    }

    // ------------------------------------------------------------ prefix compression

    /// <summary>
    /// Keys sharing a long prefix should cost far less than their nominal size, which is the
    /// whole justification for the block format's complexity.
    /// </summary>
    [Fact]
    public async Task Prefix_compression_shrinks_tables_with_structured_keys()
    {
        using var dir = new TempDirectory();
        const int count = 20_000;

        // Keys with a 28-byte common prefix, which is what real keys tend to look like.
        var shared = Enumerable.Range(0, count)
            .Select(i => ($"tenant:acme/user/profile/{i:D8}", (string?)"x"))
            .ToArray();

        var options = Options(dir.Path) with { CompressBlocks = false };
        var (sharedMeta, _) = await WriteTableAsync(dir, shared, options);

        long nominal = shared.Sum(e => (long)e.Item1.Length + 8 + 1);
        double ratio = (double)sharedMeta.FileSizeBytes / nominal;

        output.WriteLine(
            $"{count:N0} prefixed keys: {nominal:N0} B nominal -> "
                + $"{sharedMeta.FileSizeBytes:N0} B on disk ({ratio:P0}), compression off");

        Assert.True(
            ratio < 0.6,
            $"expected prefix compression to save over 40% on structured keys, got {ratio:P0}");
    }

    [Fact]
    public async Task Block_compression_shrinks_repetitive_values()
    {
        using var dir = new TempDirectory();

        var entries = Enumerable.Range(0, 5_000)
            .Select(i => ($"key:{i:D6}", (string?)new string('a', 200)))
            .ToArray();

        var uncompressed = await WriteTableAsync(
            dir, entries, Options(dir.Path) with { CompressBlocks = false });
        long uncompressedSize = uncompressed.Meta.FileSizeBytes;
        File.Delete(uncompressed.Path);

        var compressed = await WriteTableAsync(
            dir, entries, Options(dir.Path) with { CompressBlocks = true });

        output.WriteLine(
            $"5,000 highly repetitive values: {uncompressedSize:N0} B -> "
                + $"{compressed.Meta.FileSizeBytes:N0} B "
                + $"({(double)compressed.Meta.FileSizeBytes / uncompressedSize:P0})");

        Assert.True(compressed.Meta.FileSizeBytes < uncompressedSize / 2);

        // And it must still read back correctly.
        using var reader = SsTableReader.Open(compressed.Path, fileNumber: 1, cache: null);
        Assert.Equal(
            LookupResult.Found,
            reader.TryGet(ByteKey.From("key:002500"), ulong.MaxValue, out byte[]? value));
        Assert.Equal(new string('a', 200), ByteKey.ToString(value!));
    }

    // ------------------------------------------------------------ bloom filter

    /// <summary>
    /// The measured false-positive rate should track the theoretical one for the configured
    /// bits per key. A filter that is simply broken open would show 100%, and one that is broken
    /// closed would start losing data, so pinning the rate pins the implementation.
    /// </summary>
    [Theory]
    [InlineData(4)]
    [InlineData(10)]
    [InlineData(16)]
    public void The_bloom_filter_false_positive_rate_matches_theory(int bitsPerKey)
    {
        const int keyCount = 20_000;
        const int probeCount = 50_000;

        var keys = Enumerable.Range(0, keyCount)
            .Select(i => ByteKey.From($"present:{i:D8}"))
            .ToArray();

        byte[] filter = BloomFilter.Build(keys, bitsPerKey);

        foreach (byte[] key in keys)
        {
            Assert.True(
                BloomFilter.MayContain(filter, key),
                "a Bloom filter must never produce a false negative");
        }

        int falsePositives = 0;
        for (int i = 0; i < probeCount; i++)
        {
            if (BloomFilter.MayContain(filter, ByteKey.From($"absent:{i:D8}")))
            {
                falsePositives++;
            }
        }

        double measured = (double)falsePositives / probeCount;
        double expected = BloomFilter.ExpectedFalsePositiveRate(bitsPerKey);

        output.WriteLine(
            $"{bitsPerKey} bits/key, k={BloomFilter.OptimalProbeCount(bitsPerKey)}: "
                + $"measured {measured:P2}, theory {expected:P2}, "
                + $"filter {filter.Length:N0} B for {keyCount:N0} keys");

        // Generous bounds: the point is that the rate is in the right ballpark, not that this
        // particular hash matches the idealised analysis to three decimal places.
        Assert.True(
            measured < Math.Max(0.02, expected * 3),
            $"measured false-positive rate {measured:P2} is far above the predicted {expected:P2}");
    }

    /// <summary>
    /// Regression test for the key shape that the original Bloom hash handled badly.
    /// </summary>
    /// <remarks>
    /// Dense zero-padded integers differing only in their final digits -- auto-increment ids,
    /// timestamps, sequence numbers -- are among the most common real key shapes, and they were
    /// the ones the LevelDB-style hash was worst at: it is affine in the key's trailing bytes, so
    /// an arithmetic sequence of keys produced an arithmetic sequence of hashes that collapsed
    /// onto far too few distinct values. The measured false-positive rate was 14% against a
    /// predicted 0.8%, and raising the filter to 16 bits per key barely moved it, which is the
    /// signature of collisions rather than of an undersized filter.
    ///
    /// The two separate shapes below matter: the original implementation passed comfortably on
    /// distinct-prefix keys, which is why the first version of this test suite did not catch the
    /// problem at all. A cross-engine benchmark did.
    /// </remarks>
    [Theory]
    [InlineData(10, 0.02)]
    [InlineData(16, 0.005)]
    public void The_bloom_filter_handles_dense_numeric_keys(int bitsPerKey, double maximumRate)
    {
        const int keyCount = 100_000;

        // Even keys present, odd keys probed: adjacent in value, nearly identical as bytes.
        var keys = Enumerable.Range(0, keyCount)
            .Select(i => ByteKey.From($"key:{i * 2:D12}"))
            .ToArray();

        byte[] filter = BloomFilter.Build(keys, bitsPerKey);

        foreach (byte[] key in keys)
        {
            Assert.True(BloomFilter.MayContain(filter, key), "false negatives are never acceptable");
        }

        int falsePositives = 0;
        for (int i = 0; i < keyCount; i++)
        {
            if (BloomFilter.MayContain(filter, ByteKey.From($"key:{(i * 2) + 1:D12}")))
            {
                falsePositives++;
            }
        }

        double measured = (double)falsePositives / keyCount;
        output.WriteLine(
            $"dense numeric keys at {bitsPerKey} bits/key: measured {measured:P2}, "
                + $"theory {BloomFilter.ExpectedFalsePositiveRate(bitsPerKey):P2}");

        Assert.True(
            measured < maximumRate,
            $"false-positive rate {measured:P2} exceeds {maximumRate:P2} for dense numeric keys; "
                + "the filter hash is colliding on arithmetically-spaced keys");
    }

    /// <summary>
    /// Raising bits per key must actually lower the false-positive rate. A hash that collides
    /// gives a rate that barely budges, so this is the assertion that would have caught the
    /// original problem directly.
    /// </summary>
    [Fact]
    public void More_bits_per_key_lowers_the_false_positive_rate()
    {
        const int keyCount = 50_000;

        var keys = Enumerable.Range(0, keyCount)
            .Select(i => ByteKey.From($"key:{i * 2:D12}"))
            .ToArray();

        double Measure(int bitsPerKey)
        {
            byte[] filter = BloomFilter.Build(keys, bitsPerKey);
            int hits = 0;
            for (int i = 0; i < keyCount; i++)
            {
                if (BloomFilter.MayContain(filter, ByteKey.From($"key:{(i * 2) + 1:D12}"))) hits++;
            }
            return (double)hits / keyCount;
        }

        double at6 = Measure(6);
        double at10 = Measure(10);
        double at16 = Measure(16);

        output.WriteLine($"6 bits: {at6:P2}, 10 bits: {at10:P2}, 16 bits: {at16:P2}");

        Assert.True(at10 < at6 / 2, $"10 bits ({at10:P2}) should roughly halve 6 bits ({at6:P2})");
        Assert.True(at16 < at10 / 2, $"16 bits ({at16:P2}) should roughly halve 10 bits ({at10:P2})");
    }

    [Fact]
    public async Task The_bloom_filter_rules_out_absent_keys_in_a_real_table()
    {
        using var dir = new TempDirectory();

        var entries = Enumerable.Range(0, 5_000)
            .Select(i => ($"present:{i:D6}", (string?)"v"))
            .ToArray();

        var (_, path) = await WriteTableAsync(dir, entries);
        using var reader = SsTableReader.Open(path, fileNumber: 1, cache: null);

        int ruledOut = 0;
        const int probes = 5_000;

        for (int i = 0; i < probes; i++)
        {
            if (!reader.MayContain(ByteKey.From($"absent:{i:D6}"))) ruledOut++;
        }

        output.WriteLine($"{ruledOut:N0} of {probes:N0} absent keys ruled out without I/O");

        Assert.True(ruledOut > probes * 0.9, $"only {ruledOut} of {probes} were ruled out");

        // And never a present key.
        for (int i = 0; i < 5_000; i++)
        {
            Assert.True(reader.MayContain(ByteKey.From($"present:{i:D6}")));
        }
    }

    // ------------------------------------------------------------ corruption

    [Fact]
    public async Task A_truncated_table_fails_to_open()
    {
        using var dir = new TempDirectory();
        var (_, path) = await WriteTableAsync(dir, [("a", "1"), ("b", "2")]);

        using (var stream = new FileStream(path, FileMode.Open, FileAccess.Write))
        {
            stream.SetLength(stream.Length - 8);
        }

        Assert.Throws<CorruptRecordException>(() => SsTableReader.Open(path, 1, cache: null));
    }

    [Fact]
    public async Task A_corrupt_data_block_is_detected_by_its_checksum()
    {
        using var dir = new TempDirectory();

        var entries = Enumerable.Range(0, 500)
            .Select(i => ($"key:{i:D6}", (string?)$"value-{i}"))
            .ToArray();

        // Compression off so the flipped byte lands in recognisable block content.
        var (_, path) = await WriteTableAsync(
            dir, entries, Options(dir.Path) with { CompressBlocks = false });

        using (var stream = new FileStream(path, FileMode.Open, FileAccess.ReadWrite))
        {
            stream.Position = 32;
            int original = stream.ReadByte();
            stream.Position = 32;
            stream.WriteByte((byte)(original ^ 0xFF));
        }

        using var reader = SsTableReader.Open(path, 1, cache: null);

        // The damaged block is detected whenever it is read. Reading the whole table must
        // surface it rather than returning wrong bytes.
        Assert.Throws<CorruptRecordException>(() =>
        {
            using var iterator = reader.CreateIterator();
            iterator.SeekToFirst();
            while (iterator.IsValid)
            {
                _ = iterator.Value.Length;
                if (!iterator.MoveNext()) break;
            }
        });
    }

    // ------------------------------------------------------------ block cache

    [Fact]
    public async Task The_block_cache_serves_repeated_reads()
    {
        using var dir = new TempDirectory();

        var entries = Enumerable.Range(0, 2_000)
            .Select(i => ($"key:{i:D6}", (string?)$"v{i}"))
            .ToArray();

        var (_, path) = await WriteTableAsync(dir, entries, Options(dir.Path, blockSize: 512));

        var cache = new BlockCache(1 << 20);
        using var reader = SsTableReader.Open(path, fileNumber: 1, cache: cache);

        for (int round = 0; round < 10; round++)
        {
            for (int i = 0; i < 100; i++)
            {
                reader.TryGet(ByteKey.From($"key:{i:D6}"), ulong.MaxValue, out _);
            }
        }

        output.WriteLine(
            $"cache: {cache.Hits:N0} hits, {cache.Misses:N0} misses, "
                + $"{cache.Count} block(s), {cache.SizeBytes:N0} B");

        Assert.True(cache.Hits > cache.Misses, "repeated reads of the same keys should mostly hit");
    }

    [Fact]
    public void The_block_cache_evicts_to_stay_within_its_capacity()
    {
        var cache = new BlockCache(capacityBytes: 4_096);

        for (int i = 0; i < 100; i++)
        {
            cache.Put(fileNumber: 1, offset: i * 1_000, new byte[512]);
        }

        Assert.True(cache.SizeBytes <= 4_096, $"cache holds {cache.SizeBytes} B over its 4,096 B limit");

        // The most recent insert must still be resident; the earliest must not.
        Assert.True(cache.TryGet(1, 99 * 1_000, out _));
        Assert.False(cache.TryGet(1, 0, out _));
    }

    [Fact]
    public void The_block_cache_drops_everything_belonging_to_a_deleted_table()
    {
        var cache = new BlockCache(1 << 20);

        cache.Put(fileNumber: 1, offset: 0, new byte[128]);
        cache.Put(fileNumber: 1, offset: 256, new byte[128]);
        cache.Put(fileNumber: 2, offset: 0, new byte[128]);

        cache.EvictFile(1);

        Assert.False(cache.TryGet(1, 0, out _));
        Assert.False(cache.TryGet(1, 256, out _));
        Assert.True(cache.TryGet(2, 0, out _));
    }
}
