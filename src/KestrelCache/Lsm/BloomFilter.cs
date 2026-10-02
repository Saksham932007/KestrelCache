using System.Numerics;

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
/// Rather than computing <c>k</c> independent hashes, this uses the double-hashing scheme from
/// the LevelDB implementation: one 32-bit hash, then a rotation of it as a stride, stepping
/// through the bit array. Kirsch and Mitzenmacher showed that two hashes suffice to build
/// <c>k</c> with no asymptotic loss in false-positive rate, which turns <c>k</c> hash
/// computations into one.
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
            uint hash = Hash(key);
            uint delta = BitOperations.RotateRight(hash, 17);

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

        uint hash = Hash(userKey);
        uint delta = BitOperations.RotateRight(hash, 17);

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
    /// A 32-bit hash with good avalanche behaviour, matching the one LevelDB's filter policy
    /// uses so the sizing analysis carries over unchanged.
    /// </summary>
    private static uint Hash(ReadOnlySpan<byte> data)
    {
        const uint seed = 0xbc9f1d34;
        const uint m = 0xc6a4a793;

        uint hash = seed ^ (uint)(data.Length * m);
        int index = 0;

        while (data.Length - index >= 4)
        {
            uint word = (uint)(data[index]
                | (data[index + 1] << 8)
                | (data[index + 2] << 16)
                | (data[index + 3] << 24));
            hash += word;
            hash *= m;
            hash ^= hash >> 16;
            index += 4;
        }

        switch (data.Length - index)
        {
            case 3:
                hash += (uint)data[index + 2] << 16;
                goto case 2;
            case 2:
                hash += (uint)data[index + 1] << 8;
                goto case 1;
            case 1:
                hash += data[index];
                hash *= m;
                hash ^= hash >> 24;
                break;
        }

        return hash;
    }
}
