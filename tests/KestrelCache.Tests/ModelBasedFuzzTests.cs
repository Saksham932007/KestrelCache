using System.Text;
using KestrelCache.Bitcask;
using KestrelCache.Lsm;
using Xunit;
using Xunit.Abstractions;

namespace KestrelCache.Tests;

/// <summary>
/// Differential testing of both engines against a reference model.
/// </summary>
/// <remarks>
/// <para>
/// The idea is simple and disproportionately effective: a key-value store is supposed to behave
/// exactly like a <see cref="Dictionary{TKey,TValue}"/> that happens to survive restarts. So
/// drive a long random sequence of operations into both, and after every one assert that they
/// agree. Any divergence is a bug, and the seed reproduces it exactly.
/// </para>
/// <para>
/// This is how a replay bug surfaces without anyone having to suspect it. The generator
/// eventually emits a delete, then a put, then a reopen; the model says the new key is there,
/// the engine says it is not, and the assertion names the operation index and the seed.
/// Hand-written example tests only find the bugs their author already thought of. This finds
/// the ones they did not — it caught two during the writing of this project: an off-by-one in
/// tombstone replay, and a compaction that stripped the wrong framing from the records it kept.
/// </para>
/// <para>
/// Both engines are driven through the same <see cref="IStorageEngine"/> contract, so a
/// behavioural difference between them shows up here rather than in production.
/// </para>
/// </remarks>
public sealed class ModelBasedFuzzTests(ITestOutputHelper output)
{
    /// <summary>
    /// Fixed seeds rather than a random one per run: a test that fails only on CI and cannot be
    /// reproduced locally is close to useless. New seeds get added when they find something.
    /// </summary>
    public static TheoryData<EngineKind, int> Cases
    {
        get
        {
            var data = new TheoryData<EngineKind, int>();
            foreach (int seed in (int[])[1, 2, 3, 7, 11, 42, 1337, 20240601, int.MaxValue / 7])
            {
                data.Add(EngineKind.Bitcask, seed);
                data.Add(EngineKind.Lsm, seed);
            }
            return data;
        }
    }

    private enum Operation
    {
        Put,
        Delete,
        Get,
        Batch,
        Reopen,
        Compact,
        Scan,
    }

    private static DatabaseOptions OptionsFor(EngineKind engine, TempDirectory dir) =>
        engine == EngineKind.Bitcask
            ? new DatabaseOptions
            {
                Path = dir.DbPath,
                SyncPolicy = SyncPolicy.None,
                // Compaction stays under the test's control so a failure reproduces from the seed.
                AutoCompactStaleRatio = 0,
            }
            : new DatabaseOptions
            {
                Path = dir.Path,
                SyncPolicy = SyncPolicy.None,
                EnableBackgroundCompaction = false,
                // Small enough that a few thousand operations produce real levels, real
                // compactions and repeated recovery rather than staying in one memtable.
                MemtableSizeBytes = 8 * 1024,
                TargetFileSizeBytes = 16 * 1024,
                BaseLevelSizeBytes = 32 * 1024,
                BlockSizeBytes = 512,
                Level0CompactionTrigger = 3,
            };

    private static async ValueTask<IStorageEngine> OpenAsync(EngineKind engine, DatabaseOptions options) =>
        engine == EngineKind.Bitcask
            ? await BitcaskEngine.OpenAsync(options)
            : await LsmEngine.OpenAsync(options);

    private static bool RecoveryWasClean(IStorageEngine engine) => engine switch
    {
        BitcaskEngine bitcask => bitcask.Recovery.Clean,
        LsmEngine lsm => lsm.Recovery.Clean,
        _ => true,
    };

    private static string RecoveryDescription(IStorageEngine engine) => engine switch
    {
        BitcaskEngine bitcask => bitcask.Recovery.TruncationReason ?? "clean",
        LsmEngine lsm => lsm.Recovery.TruncationReason ?? "clean",
        _ => "n/a",
    };

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task The_engine_agrees_with_a_dictionary_under_random_operations(
        EngineKind engineKind,
        int seed)
    {
        const int operations = 3_000;
        const int keyspace = 120;

        using var dir = new TempDirectory($"fuzz-{engineKind}-{seed}");
        var random = new Random(seed);
        var model = new Dictionary<string, string>(StringComparer.Ordinal);
        var options = OptionsFor(engineKind, dir);

        var engine = await OpenAsync(engineKind, options);
        int reopens = 0;
        int compactions = 0;
        int scans = 0;

        try
        {
            for (int step = 0; step < operations; step++)
            {
                switch (PickOperation(random, engine is IScannableStorageEngine))
                {
                    case Operation.Put:
                    {
                        string key = RandomKey(random, keyspace);
                        string value = RandomValue(random);
                        await engine.PutAsync(TestData.Key(key), TestData.Value(value));
                        model[key] = value;
                        break;
                    }

                    case Operation.Delete:
                    {
                        string key = RandomKey(random, keyspace);
                        await engine.DeleteAsync(TestData.Key(key));
                        model.Remove(key);
                        break;
                    }

                    case Operation.Get:
                    {
                        string key = RandomKey(random, keyspace);
                        string? actual = TestData.Text(await engine.GetAsync(TestData.Key(key)));
                        Assert.Equal(model.GetValueOrDefault(key), actual);
                        break;
                    }

                    case Operation.Batch:
                    {
                        var batch = new WriteBatch();
                        var staged = new List<(string Key, string? Value)>();
                        int size = random.Next(1, 12);

                        for (int i = 0; i < size; i++)
                        {
                            string key = RandomKey(random, keyspace);
                            if (random.Next(4) == 0)
                            {
                                batch.Delete(key);
                                staged.Add((key, null));
                            }
                            else
                            {
                                string value = RandomValue(random);
                                batch.Put(key, value);
                                staged.Add((key, value));
                            }
                        }

                        await engine.WriteAsync(batch);

                        foreach (var (key, value) in staged)
                        {
                            if (value is null) model.Remove(key);
                            else model[key] = value;
                        }
                        break;
                    }

                    case Operation.Reopen:
                    {
                        // The interesting operation. Everything the model holds must still be
                        // there after the index, or the whole tree, is rebuilt from disk alone.
                        await engine.DisposeAsync();
                        engine = await OpenAsync(engineKind, options);
                        reopens++;

                        Assert.True(
                            RecoveryWasClean(engine),
                            $"{engineKind} seed {seed}, step {step}: recovery was not clean "
                                + $"({RecoveryDescription(engine)})");
                        break;
                    }

                    case Operation.Compact:
                    {
                        await engine.CompactAsync();
                        compactions++;
                        break;
                    }

                    case Operation.Scan:
                    {
                        await AssertScanMatchesModelAsync(
                            (IScannableStorageEngine)engine, model, random, engineKind, seed, step);
                        scans++;
                        break;
                    }
                }

                // A full comparison every so often catches drift that a single-key Get would
                // miss, without making the whole run quadratic.
                if (step % 250 == 0)
                {
                    await AssertFullAgreementAsync(engine, model, engineKind, seed, step);
                }
            }

            await AssertFullAgreementAsync(engine, model, engineKind, seed, operations);

            output.WriteLine(
                $"{engineKind} seed {seed}: {operations} ops, {model.Count} live keys, "
                    + $"{reopens} reopens, {compactions} compactions, {scans} scans");
        }
        finally
        {
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// The same comparison, but every operation is a write and the database is reopened only at
    /// the very end — the case where a replay bug has had the most chances to accumulate drift.
    /// </summary>
    [Theory]
    [MemberData(nameof(Cases))]
    public async Task The_whole_keyspace_survives_a_single_replay_of_a_long_history(
        EngineKind engineKind,
        int seed)
    {
        using var dir = new TempDirectory($"replay-{engineKind}-{seed}");
        var random = new Random(seed);
        var model = new Dictionary<string, string>(StringComparer.Ordinal);
        var options = OptionsFor(engineKind, dir);

        await using (var engine = await OpenAsync(engineKind, options))
        {
            for (int i = 0; i < 4_000; i++)
            {
                string key = RandomKey(random, 300);
                if (random.Next(3) == 0)
                {
                    await engine.DeleteAsync(TestData.Key(key));
                    model.Remove(key);
                }
                else
                {
                    string value = RandomValue(random);
                    await engine.PutAsync(TestData.Key(key), TestData.Value(value));
                    model[key] = value;
                }
            }
        }

        await using var reopened = await OpenAsync(engineKind, options);

        Assert.True(RecoveryWasClean(reopened), RecoveryDescription(reopened));
        await AssertFullAgreementAsync(reopened, model, engineKind, seed, -1);
    }

    private static async Task AssertFullAgreementAsync(
        IStorageEngine engine,
        Dictionary<string, string> model,
        EngineKind engineKind,
        int seed,
        int step)
    {
        foreach (var (key, expected) in model)
        {
            string? actual = TestData.Text(await engine.GetAsync(TestData.Key(key)));
            Assert.True(
                expected == actual,
                $"{engineKind} seed {seed}, step {step}: key '{key}' should be '{expected}' but "
                    + $"was '{actual ?? "<missing>"}'");
        }

        if (engine is not IScannableStorageEngine scannable) return;

        // A scan must agree with the model too, and in sorted order. This catches a whole class
        // of bug that point lookups cannot: a key present in a table the merge iterator forgets
        // to include still reads back correctly via Get.
        var scanned = new List<string>();
        await foreach (var (key, value) in scannable.ScanAsync())
        {
            string text = TestData.Text(key)!;
            scanned.Add(text);
            Assert.True(
                model.TryGetValue(text, out string? expected) && expected == TestData.Text(value),
                $"{engineKind} seed {seed}, step {step}: scan produced '{text}' = "
                    + $"'{TestData.Text(value)}', model has "
                    + $"'{model.GetValueOrDefault(text) ?? "<missing>"}'");
        }

        Assert.Equal(model.Count, scanned.Count);

        var expectedOrder = model.Keys
            .Select(ByteKey.From)
            .OrderBy(k => k, ByteKeyComparer.Instance)
            .Select(k => ByteKey.ToString(k))
            .ToList();

        Assert.Equal(expectedOrder, scanned);
    }

    private static async Task AssertScanMatchesModelAsync(
        IScannableStorageEngine engine,
        Dictionary<string, string> model,
        Random random,
        EngineKind engineKind,
        int seed,
        int step)
    {
        // A random sub-range, so bound handling is exercised and not just the full scan.
        string lower = RandomKey(random, 120);
        string upper = RandomKey(random, 120);

        byte[] a = ByteKey.From(lower);
        byte[] b = ByteKey.From(upper);
        if (ByteKeyComparer.Instance.Compare(a, b) > 0) (a, b) = (b, a);

        var expected = model
            .Where(pair =>
            {
                byte[] key = ByteKey.From(pair.Key);
                return ByteKeyComparer.Instance.Compare(key, a) >= 0
                    && ByteKeyComparer.Instance.Compare(key, b) < 0;
            })
            .OrderBy(pair => ByteKey.From(pair.Key), ByteKeyComparer.Instance)
            .ToList();

        var actual = new List<KeyValuePair<string, string>>();
        await foreach (var (key, value) in engine.ScanAsync(a, b))
        {
            actual.Add(new KeyValuePair<string, string>(
                TestData.Text(key)!, TestData.Text(value)!));
        }

        Assert.True(
            expected.Count == actual.Count,
            $"{engineKind} seed {seed}, step {step}: range scan ['{lower}', '{upper}') returned "
                + $"{actual.Count} key(s), model says {expected.Count}");

        for (int i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i].Key, actual[i].Key);
            Assert.Equal(expected[i].Value, actual[i].Value);
        }
    }

    private static Operation PickOperation(Random random, bool scannable) => random.Next(100) switch
    {
        < 38 => Operation.Put,
        < 56 => Operation.Delete,
        < 80 => Operation.Get,
        < 90 => Operation.Batch,
        < 95 => scannable ? Operation.Scan : Operation.Get,
        < 99 => Operation.Reopen,
        _ => Operation.Compact,
    };

    private static string RandomKey(Random random, int keyspace)
    {
        // A mix of plain and awkward keys: multi-byte UTF-8, embedded separators, and keys that
        // are prefixes of one another, since all three have broken real storage engines.
        int n = random.Next(keyspace);
        return (n % 7) switch
        {
            0 => $"user:{n}",
            1 => $"user:{n}:profile",
            2 => $"ключ:{n}",
            3 => $"キー:{n}",
            4 => $"k{new string('x', n % 40)}",
            5 => $"a/b/c/{n}",
            _ => n.ToString(),
        };
    }

    private static string RandomValue(Random random)
    {
        int length = random.Next(100) switch
        {
            < 60 => random.Next(0, 32),
            < 90 => random.Next(32, 512),
            _ => random.Next(512, 8192),
        };

        var builder = new StringBuilder(length);
        for (int i = 0; i < length; i++)
        {
            builder.Append((char)random.Next('a', 'z' + 1));
        }
        return builder.ToString();
    }
}
