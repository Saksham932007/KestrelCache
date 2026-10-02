using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text;

namespace KestrelCache;

/// <summary>
/// Bytewise lexicographic ordering and equality for keys, matching <c>memcmp</c> semantics.
/// </summary>
/// <remarks>
/// Ordering matters: the LSM engine stores keys sorted by this comparer so that range scans,
/// SSTable sparse indexes and merge iterators all agree on a single total order. Using
/// bytewise order (rather than, say, a culture-aware string comparison) is the same choice
/// RocksDB and LevelDB make — it is stable, locale-independent and cheap to compute.
/// </remarks>
public sealed class ByteKeyComparer : IComparer<byte[]>, IEqualityComparer<byte[]>
{
    public static readonly ByteKeyComparer Instance = new();

    private ByteKeyComparer() { }

    public int Compare(byte[]? x, byte[]? y)
    {
        if (ReferenceEquals(x, y)) return 0;
        if (x is null) return -1;
        if (y is null) return 1;
        return x.AsSpan().SequenceCompareTo(y.AsSpan());
    }

    public bool Equals(byte[]? x, byte[]? y)
    {
        if (ReferenceEquals(x, y)) return true;
        if (x is null || y is null) return false;
        return x.AsSpan().SequenceEqual(y.AsSpan());
    }

    public int GetHashCode([DisallowNull] byte[] obj)
    {
        // FNV-1a. Cheap, well-distributed for short keys, and stable across processes
        // (unlike string.GetHashCode, which is randomised per-process).
        unchecked
        {
            const uint offsetBasis = 2166136261;
            const uint prime = 16777619;
            uint hash = offsetBasis;
            foreach (byte b in obj)
            {
                hash ^= b;
                hash *= prime;
            }
            return (int)hash;
        }
    }
}

/// <summary>Helpers for moving between strings and the byte-array key space.</summary>
public static class ByteKey
{
    /// <summary>UTF-8 encodes <paramref name="s"/> into the key space.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte[] From(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>Decodes a key or value back to a UTF-8 string.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string ToString(ReadOnlySpan<byte> bytes) => Encoding.UTF8.GetString(bytes);

    /// <summary>
    /// Returns the shortest key that sorts immediately after every key with the given prefix,
    /// i.e. the exclusive upper bound for a prefix scan. Returns <c>null</c> when the prefix is
    /// all <c>0xFF</c> bytes, meaning the scan is unbounded above.
    /// </summary>
    public static byte[]? PrefixUpperBound(ReadOnlySpan<byte> prefix)
    {
        for (int i = prefix.Length - 1; i >= 0; i--)
        {
            if (prefix[i] != 0xFF)
            {
                var bound = prefix[..(i + 1)].ToArray();
                bound[i]++;
                return bound;
            }
        }
        return null;
    }
}
