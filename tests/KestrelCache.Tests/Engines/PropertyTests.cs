using FsCheck;
using FsCheck.Xunit;
using KestrelCache.Bitcask;

namespace KestrelCache.Tests;

/// <summary>
/// FsCheck properties for the pure, algebraic parts of the engine — the pieces whose
/// correctness is a universally quantified statement rather than a worked example.
/// </summary>
/// <remarks>
/// Example-based tests pin down the cases the author thought of. These pin down invariants, and
/// FsCheck shrinks any counterexample to a minimal one, which turns "it failed on this
/// 4KB blob" into "it failed on the empty array".
/// </remarks>
public sealed class PropertyTests
{
    // ------------------------------------------------------------ record framing

    [Property(MaxTest = 500)]
    public Property Encoding_then_decoding_a_record_recovers_the_key_and_value()
    {
        return Prop.ForAll(
            NonEmptyBytes(),
            Arb.Default.Array<byte>().Filter(static v => v is not null),
            (byte[] key, byte[] value) =>
            {
                int size = RecordFormat.RecordSize(key.Length, value.Length);
                var buffer = new byte[size];
                RecordFormat.Encode(buffer, key, value, isTombstone: false);

                bool parsed = RecordFormat.TryParseHeader(
                    buffer, int.MaxValue, int.MaxValue, out var header, out _);

                return parsed
                    && RecordFormat.VerifyChecksum(buffer, header)
                    && !header.IsTombstone
                    && RecordFormat.KeyOf(buffer, header).SequenceEqual(key)
                    && RecordFormat.ValueOf(buffer, header).SequenceEqual(value);
            });
    }

    [Property(MaxTest = 500)]
    public Property A_tombstone_round_trips_as_a_tombstone()
    {
        return Prop.ForAll(NonEmptyBytes(), (byte[] key) =>
        {
            int size = RecordFormat.RecordSize(key.Length, 0);
            var buffer = new byte[size];
            RecordFormat.Encode(buffer, key, [], isTombstone: true);

            bool parsed = RecordFormat.TryParseHeader(
                buffer, int.MaxValue, int.MaxValue, out var header, out _);

            return parsed
                && RecordFormat.VerifyChecksum(buffer, header)
                && header.IsTombstone
                && header.ValueLength == 0
                && RecordFormat.KeyOf(buffer, header).SequenceEqual(key);
        });
    }

    /// <summary>
    /// The checksum must cover the framing, not merely the payload. Corrupting any single byte
    /// of the record — length fields and flags included — has to be detected.
    /// </summary>
    [Property(MaxTest = 400)]
    public Property Corrupting_any_byte_of_a_record_breaks_its_checksum()
    {
        return Prop.ForAll(
            NonEmptyBytes(),
            Arb.Default.Array<byte>().Filter(static v => v is not null),
            Arb.Default.Int32(),
            (byte[] key, byte[] value, int corruptionSite) =>
            {
                int size = RecordFormat.RecordSize(key.Length, value.Length);
                var buffer = new byte[size];
                RecordFormat.Encode(buffer, key, value, isTombstone: false);

                // Skip the checksum field itself: changing it is the same experiment seen from
                // the other side, and is covered by the fields it protects.
                int index = 4 + (Math.Abs(corruptionSite % Math.Max(1, size - 4)));
                if (index >= size) return true.ToProperty();

                buffer[index] ^= 0xFF;

                bool parsed = RecordFormat.TryParseHeader(
                    buffer, int.MaxValue, int.MaxValue, out var header, out _);

                // Either the header no longer parses, or it parses and the checksum rejects it.
                // Silently accepting the mutated record is the only failure.
                bool detected = !parsed
                    || header.RecordSize > size
                    || !RecordFormat.VerifyChecksum(buffer, header);

                return detected.ToProperty();
            });
    }

    // ------------------------------------------------------------ key ordering

    [Property(MaxTest = 500)]
    public Property Key_comparison_is_antisymmetric()
    {
        return Prop.ForAll(
            Arb.Default.Array<byte>().Filter(static v => v is not null),
            Arb.Default.Array<byte>().Filter(static v => v is not null),
            (byte[] a, byte[] b) =>
            {
                int forward = Math.Sign(ByteKeyComparer.Instance.Compare(a, b));
                int backward = Math.Sign(ByteKeyComparer.Instance.Compare(b, a));
                return forward == -backward;
            });
    }

    [Property(MaxTest = 300)]
    public Property Key_comparison_is_transitive()
    {
        var bytes = Arb.Default.Array<byte>().Filter(static v => v is not null);
        return Prop.ForAll(bytes, bytes, bytes, (byte[] a, byte[] b, byte[] c) =>
        {
            var sorted = new[] { a, b, c };
            Array.Sort(sorted, ByteKeyComparer.Instance);

            return ByteKeyComparer.Instance.Compare(sorted[0], sorted[1]) <= 0
                && ByteKeyComparer.Instance.Compare(sorted[1], sorted[2]) <= 0
                && ByteKeyComparer.Instance.Compare(sorted[0], sorted[2]) <= 0;
        });
    }

    [Property(MaxTest = 500)]
    public Property Equal_keys_hash_equally()
    {
        return Prop.ForAll(
            Arb.Default.Array<byte>().Filter(static v => v is not null),
            (byte[] a) =>
            {
                byte[] clone = (byte[])a.Clone();
                return ByteKeyComparer.Instance.Equals(a, clone)
                    && ByteKeyComparer.Instance.GetHashCode(a)
                        == ByteKeyComparer.Instance.GetHashCode(clone);
            });
    }

    /// <summary>
    /// A prefix always sorts at or before every key it is a prefix of. This is the property that
    /// makes prefix scans expressible as range scans.
    /// </summary>
    [Property(MaxTest = 400)]
    public Property A_prefix_sorts_before_everything_it_prefixes()
    {
        return Prop.ForAll(
            NonEmptyBytes(),
            Arb.Default.Array<byte>().Filter(static v => v is not null),
            (byte[] prefix, byte[] suffix) =>
            {
                byte[] longer = [.. prefix, .. suffix];
                return ByteKeyComparer.Instance.Compare(prefix, longer) <= 0;
            });
    }

    /// <summary>
    /// <see cref="ByteKey.PrefixUpperBound"/> must be a genuine exclusive upper bound: every key
    /// carrying the prefix sorts strictly below it, and the bound itself does not carry it.
    /// </summary>
    [Property(MaxTest = 400)]
    public Property The_prefix_upper_bound_excludes_exactly_the_keys_with_that_prefix()
    {
        return Prop.ForAll(
            NonEmptyBytes(),
            Arb.Default.Array<byte>().Filter(static v => v is not null),
            (byte[] prefix, byte[] suffix) =>
            {
                byte[]? bound = ByteKey.PrefixUpperBound(prefix);

                // An all-0xFF prefix has no finite upper bound, which the API signals with null.
                if (bound is null) return prefix.All(static b => b == 0xFF).ToProperty();

                byte[] withPrefix = [.. prefix, .. suffix];

                bool inRange = ByteKeyComparer.Instance.Compare(prefix, withPrefix) <= 0
                    && ByteKeyComparer.Instance.Compare(withPrefix, bound) < 0;

                bool boundItselfExcluded = !bound.AsSpan().StartsWith(prefix);

                return (inRange && boundItselfExcluded).ToProperty();
            });
    }

    private static Arbitrary<byte[]> NonEmptyBytes() =>
        Arb.Default.Array<byte>()
            .Filter(static value => value is { Length: > 0 });
}
