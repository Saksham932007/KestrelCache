using System.Buffers.Binary;

namespace KestrelCache.Lsm;

/// <summary>What a stored entry means.</summary>
internal enum ValueKind : byte
{
    /// <summary>The key was deleted at this sequence number.</summary>
    Deletion = 0,

    /// <summary>The key holds a value at this sequence number.</summary>
    Value = 1,
}

/// <summary>
/// The key an LSM tree actually stores: a user key paired with the sequence number of the write
/// that produced it.
/// </summary>
/// <remarks>
/// <para>
/// An LSM tree never overwrites anything. A second write to the same user key does not replace
/// the first; it is appended with a higher sequence number, and the older entry survives in some
/// deeper level until compaction happens to drop it. So "the value of key K" is not a location
/// in the tree — it is the entry for K with the highest sequence number, and every read has to
/// be a search for that maximum.
/// </para>
/// <para>
/// Encoding the sequence number into the key, and sorting equal user keys with the <i>newest</i>
/// first, is what makes that search cheap: the first entry a scan encounters for a user key is
/// already the answer, and everything following it is stale and can be skipped without
/// comparison. It is also exactly what makes snapshots free. A reader that ignores every entry
/// whose sequence exceeds some number S sees the database precisely as it was when S was
/// assigned, with no locks and no copying, because the older versions it needs were never
/// destroyed in the first place.
/// </para>
/// <para>
/// Layout: the user key bytes, followed by an 8-byte little-endian tag holding
/// <c>(sequence &lt;&lt; 8) | kind</c>. Packing both into one integer keeps the common case —
/// comparing two entries for the same user key — a single 64-bit comparison. 56 bits of
/// sequence allows 7.2e16 writes, which at a million writes a second is about two millennia.
/// </para>
/// </remarks>
internal static class InternalKey
{
    /// <summary>Bytes of tag appended to every user key.</summary>
    internal const int TagSize = 8;

    /// <summary>Largest representable sequence number.</summary>
    internal const ulong MaxSequence = (1UL << 56) - 1;

    /// <summary>
    /// Caps a caller-supplied snapshot at the largest representable sequence, so that
    /// <see cref="ulong.MaxValue"/> can be passed to mean "the newest version of everything"
    /// without tripping the 56-bit invariant.
    /// </summary>
    internal static ulong ClampSnapshot(ulong snapshotSequence) =>
        Math.Min(snapshotSequence, MaxSequence);

    internal static ulong PackTag(ulong sequence, ValueKind kind)
    {
        if (sequence > MaxSequence)
        {
            throw new ArgumentOutOfRangeException(
                nameof(sequence), sequence, "Sequence number exceeds the 56-bit limit.");
        }
        return (sequence << 8) | (byte)kind;
    }

    /// <summary>Total encoded length of an internal key for a user key of the given length.</summary>
    internal static int SizeOf(int userKeyLength) => userKeyLength + TagSize;

    /// <summary>Encodes an internal key into <paramref name="destination"/>.</summary>
    internal static int Encode(
        Span<byte> destination,
        ReadOnlySpan<byte> userKey,
        ulong sequence,
        ValueKind kind)
    {
        userKey.CopyTo(destination);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[userKey.Length..], PackTag(sequence, kind));
        return userKey.Length + TagSize;
    }

    /// <summary>Allocates and encodes an internal key.</summary>
    internal static byte[] Encode(ReadOnlySpan<byte> userKey, ulong sequence, ValueKind kind)
    {
        var buffer = new byte[SizeOf(userKey.Length)];
        Encode(buffer, userKey, sequence, kind);
        return buffer;
    }

    /// <summary>The user key portion.</summary>
    internal static ReadOnlySpan<byte> UserKey(ReadOnlySpan<byte> internalKey) =>
        internalKey[..^TagSize];

    /// <summary>The packed tag.</summary>
    internal static ulong Tag(ReadOnlySpan<byte> internalKey) =>
        BinaryPrimitives.ReadUInt64LittleEndian(internalKey[^TagSize..]);

    /// <summary>The sequence number of the write that produced this entry.</summary>
    internal static ulong Sequence(ReadOnlySpan<byte> internalKey) => Tag(internalKey) >> 8;

    /// <summary>Whether this entry is a value or a tombstone.</summary>
    internal static ValueKind Kind(ReadOnlySpan<byte> internalKey) => (ValueKind)(Tag(internalKey) & 0xFF);

    /// <summary>True when the encoded span is long enough to be an internal key at all.</summary>
    internal static bool IsWellFormed(ReadOnlySpan<byte> internalKey) => internalKey.Length > TagSize;

    /// <summary>
    /// Ordering for internal keys: user key ascending, then sequence number <i>descending</i>.
    /// </summary>
    /// <remarks>
    /// The descending half is the load-bearing part. Because the newest entry for a user key
    /// sorts first, every lookup and every merge finds the current value immediately and can
    /// skip the rest of that key's history without inspecting it. Sorting ascending instead
    /// would force each read to scan every version of the key to find the latest.
    /// </remarks>
    internal sealed class Comparer : IComparer<byte[]>
    {
        internal static readonly Comparer Instance = new();

        private Comparer() { }

        public int Compare(byte[]? x, byte[]? y)
        {
            if (ReferenceEquals(x, y)) return 0;
            if (x is null) return -1;
            if (y is null) return 1;
            return Compare(x.AsSpan(), y.AsSpan());
        }

        internal static int Compare(ReadOnlySpan<byte> x, ReadOnlySpan<byte> y)
        {
            int byUserKey = UserKey(x).SequenceCompareTo(UserKey(y));
            if (byUserKey != 0) return byUserKey;

            ulong tagX = Tag(x);
            ulong tagY = Tag(y);

            // Reversed on purpose: a larger tag means a newer write, and newer sorts first.
            return tagX > tagY ? -1 : tagX < tagY ? 1 : 0;
        }
    }
}
