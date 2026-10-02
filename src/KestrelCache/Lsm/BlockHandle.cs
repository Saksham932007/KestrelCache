using System.Buffers.Binary;
using System.IO.Hashing;

namespace KestrelCache.Lsm;

/// <summary>A pointer to a block within an SSTable: where it starts and how long it is.</summary>
/// <param name="Offset">Byte offset of the block's first byte.</param>
/// <param name="Size">Length of the block's content, excluding its trailer.</param>
internal readonly record struct BlockHandle(long Offset, int Size)
{
    /// <summary>
    /// Bytes appended after every block: a compression-type byte and a CRC-32 over the block
    /// together with that byte.
    /// </summary>
    /// <remarks>
    /// Checksumming per block rather than per file is what makes corruption survivable and
    /// localisable. A single file-wide checksum could only ever say "something in this 64 MB
    /// file is wrong"; a per-block one says which 4 KiB is wrong, and lets every other block
    /// still be served.
    /// </remarks>
    internal const int TrailerSize = 5;

    /// <summary>Maximum encoded size of a handle in varint form.</summary>
    internal const int MaxEncodedSize = Varint.MaxSize64 + Varint.MaxSize32;

    /// <summary>Encodes the handle as two varints, for storage as an index-block value.</summary>
    internal int Encode(Span<byte> destination)
    {
        int written = Varint.Write(destination, (ulong)Offset);
        written += Varint.Write(destination[written..], (uint)Size);
        return written;
    }

    /// <summary>Encodes into a fresh array.</summary>
    internal byte[] Encode()
    {
        Span<byte> scratch = stackalloc byte[MaxEncodedSize];
        int written = Encode(scratch);
        return scratch[..written].ToArray();
    }

    /// <summary>Decodes a handle previously written by <see cref="Encode(Span{byte})"/>.</summary>
    internal static BlockHandle Decode(ReadOnlySpan<byte> source, long diagnosticOffset = 0)
    {
        int cursor = 0;
        if (!Varint.TryRead(source, ref cursor, out ulong offset)
            || !Varint.TryRead(source, ref cursor, out uint size))
        {
            throw new CorruptRecordException("Malformed block handle.", diagnosticOffset);
        }

        return new BlockHandle((long)offset, (int)size);
    }

    /// <summary>Writes the trailer for a stored block.</summary>
    internal static void WriteTrailer(Span<byte> destination, ReadOnlySpan<byte> stored, CompressionKind kind)
    {
        destination[0] = (byte)kind;

        var crc = new Crc32();
        crc.Append(stored);
        crc.Append(destination[..1]);
        BinaryPrimitives.WriteUInt32LittleEndian(destination[1..], crc.GetCurrentHashAsUInt32());
    }

    /// <summary>
    /// Validates a block's trailer and returns the compression type it declares.
    /// </summary>
    internal static CompressionKind ValidateTrailer(
        ReadOnlySpan<byte> stored,
        ReadOnlySpan<byte> trailer,
        long offset,
        string? file)
    {
        var crc = new Crc32();
        crc.Append(stored);
        crc.Append(trailer[..1]);

        uint expected = BinaryPrimitives.ReadUInt32LittleEndian(trailer[1..]);
        uint actual = crc.GetCurrentHashAsUInt32();

        if (expected != actual)
        {
            throw new CorruptRecordException(
                $"Block checksum mismatch: stored {expected:X8}, computed {actual:X8}.", offset, file);
        }

        return (CompressionKind)trailer[0];
    }
}
