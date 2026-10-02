using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Order;
using KestrelCache.Bitcask;

namespace KestrelCache.Benchmarks;

/// <summary>
/// Microbenchmarks of the hot paths, with allocation counts.
/// </summary>
/// <remarks>
/// These complement <see cref="LatencySuite"/> rather than duplicating it. The latency suite
/// measures what a caller experiences, syscalls and all; these isolate the in-process work so
/// that a regression in, say, record encoding is visible instead of being lost in the noise of
/// a 1 ms fsync. <see cref="MemoryDiagnoserAttribute"/> is on because allocation per operation
/// is the thing that silently turns a fast engine into a GC-bound one under load.
/// </remarks>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class MicroBenchmarks
{
    private byte[] _key = null!;
    private byte[] _value = null!;
    private byte[] _encoded = null!;
    private string _root = null!;
    private BitcaskEngine _engine = null!;
    private byte[][] _lookupKeys = null!;
    private int _cursor;

    /// <summary>Value sizes spanning the common cases: a flag, a small JSON document, a blob.</summary>
    [Params(16, 128, 4096)]
    public int ValueSize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _key = KestrelCache.ByteKey.From("user:0000001234");
        _value = new byte[ValueSize];
        new Random(1).NextBytes(_value);

        _encoded = new byte[RecordFormat.RecordSize(_key.Length, _value.Length)];
        RecordFormat.Encode(_encoded, _key, _value, isTombstone: false);

        _root = Path.Combine(Path.GetTempPath(), $"kestrel-micro-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_root);

        _engine = BitcaskEngine.OpenAsync(new DatabaseOptions
        {
            Path = Path.Combine(_root, "micro.kc"),
            SyncPolicy = SyncPolicy.None,
            AutoCompactStaleRatio = 0,
        }).GetAwaiter().GetResult();

        const int keyCount = 10_000;
        _lookupKeys = new byte[keyCount][];
        var batch = new WriteBatch();

        for (int i = 0; i < keyCount; i++)
        {
            byte[] key = KestrelCache.ByteKey.From($"key:{i:D12}");
            _lookupKeys[i] = key;
            batch.Put(key, _value);
            if (batch.Count >= 1_000)
            {
                _engine.WriteAsync(batch).GetAwaiter().GetResult();
                batch.Clear();
            }
        }
        if (batch.Count > 0) _engine.WriteAsync(batch).GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _engine.DisposeAsync().AsTask().GetAwaiter().GetResult();
        if (Directory.Exists(_root)) Directory.Delete(_root, recursive: true);
    }

    // ------------------------------------------------------------ framing

    [Benchmark(Description = "Encode one record (CRC + framing)"), BenchmarkCategory("framing")]
    public int EncodeRecord()
    {
        var buffer = new byte[_encoded.Length];
        return RecordFormat.Encode(buffer, _key, _value, isTombstone: false);
    }

    [Benchmark(Description = "Parse header + verify CRC"), BenchmarkCategory("framing")]
    public bool DecodeAndVerifyRecord()
    {
        RecordFormat.TryParseHeader(_encoded, int.MaxValue, int.MaxValue, out var header, out _);
        return RecordFormat.VerifyChecksum(_encoded, header);
    }

    // ------------------------------------------------------------ engine paths

    [Benchmark(Description = "Get, warm page cache"), BenchmarkCategory("engine")]
    public async Task<byte[]?> Get()
    {
        _cursor = (_cursor + 1) % _lookupKeys.Length;
        return await _engine.GetAsync(_lookupKeys[_cursor]);
    }

    [Benchmark(Description = "Get, missing key (index only)"), BenchmarkCategory("engine")]
    public async Task<byte[]?> GetMissing() =>
        await _engine.GetAsync(KestrelCache.ByteKey.From("definitely:absent"));

    [Benchmark(Description = "Put, no fsync"), BenchmarkCategory("engine")]
    public async Task Put()
    {
        _cursor = (_cursor + 1) % _lookupKeys.Length;
        await _engine.PutAsync(_lookupKeys[_cursor], _value);
    }

    // ------------------------------------------------------------ key space

    [Benchmark(Description = "Key hash (FNV-1a)"), BenchmarkCategory("keys")]
    public int HashKey() => KestrelCache.ByteKeyComparer.Instance.GetHashCode(_key);

    [Benchmark(Description = "Key compare (bytewise)"), BenchmarkCategory("keys")]
    public int CompareKeys() =>
        KestrelCache.ByteKeyComparer.Instance.Compare(_lookupKeys[0], _lookupKeys[1]);
}
