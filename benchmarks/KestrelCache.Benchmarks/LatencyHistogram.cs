using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace KestrelCache.Benchmarks;

/// <summary>
/// Records the duration of every single operation and reports the distribution.
/// </summary>
/// <remarks>
/// <para>
/// Throughput alone — "180,000 ops/sec" — is the number that looks best on a README and tells
/// you least. It is a mean, and a mean hides exactly the behaviour that decides whether a
/// storage engine is usable: the request that waited behind an fsync, the read that landed
/// during a compaction, the write that triggered a memtable flush. A system can average 50 µs
/// and still stall for 200 ms at p99.9, and the user who hits that stall is the one who
/// complains.
/// </para>
/// <para>
/// So every operation's latency is recorded individually and the percentiles are reported from
/// the raw sample. p99.9 matters more than it sounds: at a thousand operations per page load,
/// one request in a thousand being slow means roughly every page is slow.
/// </para>
/// </remarks>
public sealed class LatencyHistogram(string name, int capacity = 1 << 20)
{
    private readonly List<long> _ticks = new(capacity);

    /// <summary>What was measured.</summary>
    public string Name { get; } = name;

    /// <summary>Samples recorded.</summary>
    public int Count => _ticks.Count;

    /// <summary>Wall-clock duration of the whole measured run.</summary>
    public TimeSpan Elapsed { get; set; }

    /// <summary>The raw samples, so per-thread histograms can be merged before percentiles are taken.</summary>
    public IReadOnlyList<long> RawSamples => _ticks;

    /// <summary>Records one operation's duration in <see cref="Stopwatch"/> ticks.</summary>
    public void Record(long stopwatchTicks) => _ticks.Add(stopwatchTicks);

    /// <summary>Operations per second over the measured wall-clock window.</summary>
    public double Throughput =>
        Elapsed.TotalSeconds <= 0 ? 0 : Count / Elapsed.TotalSeconds;

    /// <summary>The requested percentile in microseconds. <paramref name="percentile"/> is in [0, 100].</summary>
    public double PercentileMicroseconds(double percentile)
    {
        if (_ticks.Count == 0) return 0;

        var sorted = Sorted();
        // Nearest-rank: the smallest sample at or above the requested rank. No interpolation,
        // because an interpolated p99.9 reports a latency no request actually experienced.
        int rank = (int)Math.Ceiling(percentile / 100.0 * sorted.Length) - 1;
        rank = Math.Clamp(rank, 0, sorted.Length - 1);
        return ToMicroseconds(sorted[rank]);
    }

    /// <summary>Mean latency in microseconds.</summary>
    public double MeanMicroseconds =>
        _ticks.Count == 0 ? 0 : ToMicroseconds((long)_ticks.Average());

    private long[]? _sortedCache;

    private long[] Sorted()
    {
        if (_sortedCache is null || _sortedCache.Length != _ticks.Count)
        {
            _sortedCache = [.. _ticks];
            Array.Sort(_sortedCache);
        }
        return _sortedCache;
    }

    private static double ToMicroseconds(long ticks) =>
        ticks * 1_000_000.0 / Stopwatch.Frequency;

    /// <summary>A fixed-width table row for console output.</summary>
    public string ToRow() => string.Format(
        CultureInfo.InvariantCulture,
        "{0,-34} {1,10:N0} {2,10:N0} {3,9:F1} {4,9:F1} {5,9:F1} {6,10:F1} {7,10:F1}",
        Name,
        Count,
        Throughput,
        MeanMicroseconds,
        PercentileMicroseconds(50),
        PercentileMicroseconds(95),
        PercentileMicroseconds(99),
        PercentileMicroseconds(99.9));

    /// <summary>The header matching <see cref="ToRow"/>.</summary>
    public static string Header =>
        string.Format(
            CultureInfo.InvariantCulture,
            "{0,-34} {1,10} {2,10} {3,9} {4,9} {5,9} {6,10} {7,10}",
            "scenario",
            "ops",
            "ops/sec",
            "mean us",
            "p50 us",
            "p95 us",
            "p99 us",
            "p99.9 us")
        + Environment.NewLine
        + new string('-', 115);

    /// <summary>A Markdown table row, for pasting measured numbers into the README.</summary>
    public string ToMarkdownRow() => string.Format(
        CultureInfo.InvariantCulture,
        "| {0} | {1:N0} | {2:N0} | {3:F1} | {4:F1} | {5:F1} | {6:F1} | {7:F1} |",
        Name,
        Count,
        Throughput,
        MeanMicroseconds,
        PercentileMicroseconds(50),
        PercentileMicroseconds(95),
        PercentileMicroseconds(99),
        PercentileMicroseconds(99.9));

    /// <summary>The Markdown table header matching <see cref="ToMarkdownRow"/>.</summary>
    public static string MarkdownHeader =>
        "| scenario | ops | ops/sec | mean us | p50 us | p95 us | p99 us | p99.9 us |"
        + Environment.NewLine
        + "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |";

    /// <summary>A short human summary.</summary>
    public override string ToString()
    {
        var builder = new StringBuilder();
        builder.Append(Name).Append(": ");
        builder.Append(CultureInfo.InvariantCulture, $"{Throughput:N0} ops/s, ");
        builder.Append(CultureInfo.InvariantCulture, $"p50 {PercentileMicroseconds(50):F1} us, ");
        builder.Append(CultureInfo.InvariantCulture, $"p99 {PercentileMicroseconds(99):F1} us, ");
        builder.Append(CultureInfo.InvariantCulture, $"p99.9 {PercentileMicroseconds(99.9):F1} us");
        return builder.ToString();
    }
}
