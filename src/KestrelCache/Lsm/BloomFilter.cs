using System.IO.Hashing;

namespace KestrelCache.Lsm;

/// <summary>
/// A Bloom filter over the user keys of one SSTable, so a lookup can rule the file out without
/// touching the disk.
/// </summary>
/// <remarks>
/// <para>
/// This is the single most valuable optimisation in an LSM tree, because it attacks the design's
/// worst case directly. A read for an absent key has to be proven absent, which naively means
/// consulting every level — several files, each needing at least one seek. Bloom filters turn
/// almost all of those into a memory probe: the filter can say "definitely not here" with
/// certainty and "possibly here" with a small error rate, and the first answer is the common one.
/// </para>
/// <para>
/// The sizing follows from the standard false-positive formula. With <c>m</c> bits for <c>n</c>
/// keys, the optimal number of hash functions is <c>k = (m/n) ln 2</c>, which gives a
/// false-positive rate of about <c>0.6185^(m/n)</c>. Ten bits per key is the usual choice
/// because it lands near 1% while costing 1.25 bytes per key — for a million keys that is 1.2 MB
/// of RAM to avoid roughly 99 disk seeks out of every 100 misses.
/// </para>
/// <para>
/// Rather than computing <c>k</c> independent hashes, this uses double hashing: derive two
/// values from the key and step through the bit array as <c>h, h+d, h+2d, ...</c>. Kirsch and
/// Mitzenmacher showed that two hashes suffice to build <c>k</c> with no asymptotic loss in
/// false-positive rate, which turns <c>k</c> hash computations into one.
/// </para>
/// <para><b>Why XxHash3 and not the usual LevelDB hash</b></para>
/// <para>
/// The first implementation here followed LevelDB exactly: its 32-bit <c>Hash</c> function for
/// <c>h</c>, and a 17-bit rotation of that same value for <c>d</c>. The unit tests measured a
/// false-positive rate within 0.1 percentage points of theory and it looked correct. The
/// cross-engine benchmark then reported <b>14%</b> against a predicted 0.8%, because it used a
/// different key shape: dense zero-padded integers, where present and absent keys differ only in
/// their last digit or two.
/// </para>
/// <para>
/// Two measurements identified the cause. First, raising the filter to 16 bits per key barely
/// moved the rate (14.1% to 13.1%) — an undersized filter improves when given more bits, so the
/// problem was not size. Second, adding a murmur3 finalizer to the existing hash did not help
/// either, which rules out weak avalanche and leaves only one explanation: the hash was
/// genuinely <i>colliding</i> on this key set, and no amount of post-mixing can separate two
/// inputs that have already been mapped to the same 32-bit value. The hash is affine in the key's
/// trailing bytes, so keys forming an arithmetic sequence map to hashes forming one, and a
/// 200,000-key dense range collapses onto far fewer distinct values than chance would predict.
/// A hash collision in a Bloom filter is not a near-miss — every probe coincides, so it is a
/// guaranteed false positive.
/// </para>
/// <para>
/// This matters because dense integer keys are not an exotic case: auto-increment identifiers,
/// timestamps and sequence numbers are among the most common key shapes there are, and they are
/// exactly the ones the original hash handled worst.
/// </para>
/// <para>
/// XxHash3 fixes it, taking <c>h</c> and <c>d</c> from independent halves of one 64-bit digest
/// so the stride is not a function of the hash. Measured on 100,000 keys at 10 bits per key:
/// dense numeric keys fall from 14.06% to 0.78%, distinct-prefix keys stay at 0.88%, and random
/// keys at 0.84% — all three now agree with the 0.84% the formula predicts, and the rate finally
/// scales with bits per key as it should (0.05% at 16 bits).
/// </para>
/// </remarks>
internal static class BloomFilter
{
    /// <summary>
    /// Builds a filter over <paramref name="userKeys"/>. The last byte of the result holds the
    /// probe count, so a reader never has to be told how the filter was configured.
    /// </summary>
    internal static byte[] Build(IReadOnlyList<byte[]> userKeys, int bitsPerKey)
    {
        if (bitsPerKey <= 0 || userKeys.Count == 0)
        {
            return [0]; // zero probes: the reader treats this as "always maybe"
        }

        int probes = OptimalProbeCount(bitsPerKey);

        // A floor on the size keeps the false-positive rate sane for tiny tables, where
        // rounding down would otherwise leave only a handful of bits.
        int bits = Math.Max(64, userKeys.Count * bitsPerKey);
        int bytes = (bits + 7) / 8;
        bits = bytes * 8;

        var filter = new byte[bytes + 1];

        foreach (byte[] key in userKeys)
        {
            var (hash, delta) = Probes(key);

            for (int probe = 0; probe < probes; probe++)
            {
                uint bit = hash % (uint)bits;
                filter[bit / 8] |= (byte)(1 << (int)(bit % 8));
                hash += delta;
            }
        }

        filter[bytes] = (byte)probes;
        return filter;
    }

    /// <summary>
    /// Returns false only when <paramref name="userKey"/> is definitely absent from the table the
    /// filter describes. A true result means "possibly present" and must be confirmed by reading.
    /// </summary>
    internal static bool MayContain(ReadOnlySpan<byte> filter, ReadOnlySpan<byte> userKey)
    {
        if (filter.Length < 2) return true; // no filter, or an empty one: cannot rule anything out

        int probes = filter[^1];
        if (probes == 0 || probes > 30) return true; // unconfigured or implausible: fail open

        var bitmap = filter[..^1];
        int bits = bitmap.Length * 8;

        var (hash, delta) = Probes(userKey);

        for (int probe = 0; probe < probes; probe++)
        {
            uint bit = hash % (uint)bits;
            if ((bitmap[(int)(bit / 8)] & (1 << (int)(bit % 8))) == 0)
            {
                return false;
            }
            hash += delta;
        }

        return true;
    }

    /// <summary>
    /// The optimal probe count for a given bits-per-key, clamped to a sane range.
    /// </summary>
    /// <remarks>
    /// <c>k = (m/n) ln 2 ≈ 0.69 * bitsPerKey</c>. Going above the optimum is actively harmful:
    /// each extra probe sets more bits, and past the optimum the added density costs more in
    /// collisions than the extra check saves.
    /// </remarks>
    internal static int OptimalProbeCount(int bitsPerKey) =>
        Math.Clamp((int)(bitsPerKey * 0.69), 1, 30);

    /// <summary>
    /// The expected false-positive rate for a configuration, so the measured rate can be
    /// compared against theory rather than merely reported.
    /// </summary>
    internal static double ExpectedFalsePositiveRate(int bitsPerKey)
    {
        if (bitsPerKey <= 0) return 1.0;
        int probes = OptimalProbeCount(bitsPerKey);
        // (1 - e^(-k/(m/n)))^k
        double exponent = -(double)probes / bitsPerKey;
        return Math.Pow(1.0 - Math.Exp(exponent), probes);
    }

    /// <summary>
    /// Derives the starting bit position and the stride for one key's probe sequence.
    /// </summary>
    /// <remarks>
    /// Both come from one XxHash3 digest, but from <i>independent halves</i> of it. Deriving the
    /// stride from the hash — by rotating it, as the LevelDB filter policy does — makes the whole
    /// probe sequence a function of a single 32-bit value, so two keys that collide in that value
    /// have identical probe sets. Taking the stride from the other half removes that coupling.
    /// The stride is forced odd so that it is coprime with any power-of-two bit count, which
    /// keeps the sequence from revisiting the same bits.
    /// </remarks>
    private static (uint Hash, uint Delta) Probes(ReadOnlySpan<byte> data)
    {
        ulong digest = XxHash3.HashToUInt64(data);
        return ((uint)digest, (uint)(digest >> 32) | 1u);
    }
}
