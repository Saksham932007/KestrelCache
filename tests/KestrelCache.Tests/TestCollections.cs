using Xunit;

namespace KestrelCache.Tests;

/// <summary>
/// The collection for every test whose correctness depends on wall-clock timing.
/// </summary>
/// <remarks>
/// <para>
/// A single collection with parallelisation disabled, which in xUnit means its tests run one at a
/// time <i>and</i> not alongside any other collection. That second part is the one that matters
/// here.
/// </para>
/// <para>
/// These tests measure elapsed time: an election timeout expiring, a background fsync loop
/// getting scheduled, an fsync costing more than a memory write. The rest of the suite — fuzzers,
/// compaction, bit-flip sweeps, crash harnesses — is CPU-bound and saturates every core. Run
/// together on a four-thread machine, the timing tests get starved and report absurdities: 300
/// unsynced writes taking 7.1 seconds when they take 5 milliseconds idle, or a background loop
/// that never ran at all.
/// </para>
/// <para>
/// Loosening each threshold until it stopped failing was the wrong instinct. The thresholds were
/// right; the scheduling was wrong. Isolating them keeps the assertions tight enough to mean
/// something — the fsync-cost test in particular only has value if it is allowed to actually
/// measure an fsync.
/// </para>
/// </remarks>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class TimingSensitiveCollection
{
    /// <summary>The collection name, referenced by <see cref="CollectionAttribute"/>.</summary>
    public const string Name = "timing-sensitive";
}
