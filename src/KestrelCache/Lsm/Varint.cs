using System.Buffers.Binary;

namespace KestrelCache.Lsm;

/// <summary>
/// LEB128-style variable-length integer encoding, as used inside SSTable blocks.
/// </summary>
/// <remarks>
/// Block entries are dominated by three small integers — a shared-prefix length, a key-suffix
/// length and a value length — repeated once per key. Spending four bytes each on values that
/// are almost always below 128 would add twelve bytes of overhead per entry, which on short
/// records is a double-digit percentage of the file. A varint spends one byte for anything under
/// 128 and grows only when it has to.
/// </remarks>
internal static class Varint
{
    /// <summary>Largest number of bytes a 32-bit varint can occupy.</summary>
    internal const int MaxSize32 = 5;

    /// <summary>Largest number of bytes a 64-bit varint can occupy.</summary>
    internal const int MaxSize64 = 10;

    /// <summary>Writes <paramref name="value"/> and returns the bytes used.</summary>
    internal static int Write(Span<byte> destination, uint value)
    {
        int written = 0;
        while (value >= 0x80)
        {
            destination[written++] = (byte)(value | 0x80);
            value >>= 7;
        }
        destination[written++] = (byte)value;
        return written;
    }

    /// <summary>Writes a 64-bit <paramref name="value"/> and returns the bytes used.</summary>
    internal static int Write(Span<byte> destination, ulong value)
    {
        int written = 0;
        while (value >= 0x80)
        {
            destination[written++] = (byte)(value | 0x80);
            value >>= 7;
        }
        destination[written++] = (byte)value;
        return written;
    }

    /// <summary>Bytes <paramref name="value"/> will occupy when written.</summary>
    internal static int SizeOf(uint value)
    {
        int size = 1;
        while (value >= 0x80)
        {
            value >>= 7;
            size++;
        }
        return size;
    }

    /// <summary>Bytes a 64-bit <paramref name="value"/> will occupy when written.</summary>
    internal static int SizeOf(ulong value)
    {
        int size = 1;
        while (value >= 0x80)
        {
            value >>= 7;
            size++;
        }
        return size;
    }

    /// <summary>
    /// Reads a 32-bit varint, advancing <paramref name="offset"/>. Returns false on a truncated
    /// or over-long encoding rather than throwing, because this runs against bytes read from
    /// disk and a malformed block must be reportable, not fatal.
    /// </summary>
    internal static bool TryRead(ReadOnlySpan<byte> source, ref int offset, out uint value)
    {
        value = 0;
        int shift = 0;

        while (offset < source.Length && shift <= 28)
        {
            byte b = source[offset++];
            value |= (uint)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return true;
            shift += 7;
        }

        value = 0;
        return false;
    }

    /// <summary>Reads a 64-bit varint, advancing <paramref name="offset"/>.</summary>
    internal static bool TryRead(ReadOnlySpan<byte> source, ref int offset, out ulong value)
    {
        value = 0;
        int shift = 0;

        while (offset < source.Length && shift <= 63)
        {
            byte b = source[offset++];
            value |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return true;
            shift += 7;
        }

        value = 0;
        return false;
    }

    /// <summary>Writes a fixed 32-bit little-endian value, used where a fixed width is required.</summary>
    internal static void WriteFixed32(Span<byte> destination, uint value) =>
        BinaryPrimitives.WriteUInt32LittleEndian(destination, value);

    /// <summary>Reads a fixed 32-bit little-endian value.</summary>
    internal static uint ReadFixed32(ReadOnlySpan<byte> source) =>
        BinaryPrimitives.ReadUInt32LittleEndian(source);
}
