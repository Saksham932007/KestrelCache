using System.Text;
using KestrelCache.Bitcask;
using Xunit;
using Xunit.Abstractions;

namespace KestrelCache.Tests;

/// <summary>
/// Differential testing against a reference model.
/// </summary>
/// <remarks>
/// <para>
/// The idea is simple and disproportionately effective: a key-value store is supposed to behave
/// exactly like a <see cref="Dictionary{TKey,TValue}"/> that happens to survive restarts. So
/// drive a long random sequence of operations into both, and after every single one assert that
/// they agree. Any divergence is a bug, and the seed reproduces it exactly.
/// </para>
/// <para>
/// This is how the tombstone replay off-by-one surfaces without anyone having to suspect it:
/// the generator eventually emits a delete followed by a put followed by a reopen, the model
/// says the new key is there, the engine says it is not, and the assertion names the operation
/// index and the seed. Hand-written example tests only find the bugs you already thought of;
/// this finds the ones you did not.
/// </para>
/// </remarks>
public sealed class ModelBasedFuzzTests(ITestOutputHelper output)
{
    /// <summary>
    /// Fixed seeds rather than a random one per run: a test that fails only on CI and cannot be
    /// reproduced locally is close to useless. New seeds get added when they find something.
    /// </summary>
    public static TheoryData<int> Seeds =>
        new() { 1, 2, 3, 7, 11, 42, 1337, 20240601, int.MaxValue / 7 };

    private enum Operation
    {
        Put,
        Delete,
        Get,
        Batch,
        Reopen,
        Compact,
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task The_engine_agrees_with_a_dictionary_under_random_operations(int seed)
    {
        const int operations = 3_000;
        const int keyspace = 120;

        using var dir = new TempDirectory($"fuzz-{seed}");
        var random = new Random(seed);
        var model = new Dictionary<string, string>(StringComparer.Ordinal);

        var options = new DatabaseOptions
        {
            Path = dir.DbPath,
            SyncPolicy = SyncPolicy.None,
            // Keep compaction under the test's control so a failure is reproducible from the seed.
            AutoCompactStaleRatio = 0,
        };

        var engine = await BitcaskEngine.OpenAsync(options);
        int reopens = 0;
        int compactions = 0;

        try
        {
            for (int step = 0; step < operations; step++)
            {
                Operation op = PickOperation(random);

                switch (op)
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
                        // there after the index is rebuilt from the log alone.
                        await engine.DisposeAsync();
                        engine = await BitcaskEngine.OpenAsync(options);
                        reopens++;

                        Assert.True(
                            engine.Recovery.Clean,
                            $"seed {seed}, step {step}: recovery was not clean "
                                + $"({engine.Recovery.TruncationReason})");
                        break;
                    }

                    case Operation.Compact:
                    {
                        await engine.CompactAsync();
                        compactions++;
                        break;
                    }
                }

                // A cheap full comparison every so often catches drift that a single-key Get
                // would miss, without making the whole run quadratic.
                if (step % 250 == 0)
                {
                    await AssertFullAgreementAsync(engine, model, seed, step);
                }
            }

            await AssertFullAgreementAsync(engine, model, seed, operations);
            output.WriteLine(
                $"seed {seed}: {operations} ops, {model.Count} live keys, "
                    + $"{reopens} reopens, {compactions} compactions");
        }
        finally
        {
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// The same comparison, but every operation is a write and the database is reopened at the
    /// very end — the case where a replay bug has had the most chances to accumulate drift.
    /// </summary>
    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task The_whole_keyspace_survives_a_single_replay_of_a_long_log(int seed)
    {
        using var dir = new TempDirectory($"replay-{seed}");
        var random = new Random(seed);
        var model = new Dictionary<string, string>(StringComparer.Ordinal);

        var options = new DatabaseOptions
        {
            Path = dir.DbPath,
            SyncPolicy = SyncPolicy.None,
            AutoCompactStaleRatio = 0,
        };

        await using (var engine = await BitcaskEngine.OpenAsync(options))
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

        await using var reopened = await BitcaskEngine.OpenAsync(options);

        Assert.True(reopened.Recovery.Clean);
        await AssertFullAgreementAsync(reopened, model, seed, -1);
    }

    private static async Task AssertFullAgreementAsync(
        BitcaskEngine engine,
        Dictionary<string, string> model,
        int seed,
        int step)
    {
        Assert.Equal(model.Count, engine.GetStats().KeyCount);

        foreach (var (key, expected) in model)
        {
            string? actual = TestData.Text(await engine.GetAsync(TestData.Key(key)));
            Assert.True(
                expected == actual,
                $"seed {seed}, step {step}: key '{key}' should be '{expected}' but was "
                    + $"'{actual ?? "<missing>"}'");
        }
    }

    private static Operation PickOperation(Random random) => random.Next(100) switch
    {
        < 40 => Operation.Put,
        < 58 => Operation.Delete,
        < 85 => Operation.Get,
        < 95 => Operation.Batch,
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
