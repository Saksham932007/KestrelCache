using System.Buffers.Binary;

namespace KestrelCache.Lsm;

/// <summary>
/// The fixed-size trailer at the very end of an SSTable, which is the only part of the file
/// whose location is known in advance.
/// </summary>
/// <remarks>
/// <para>
/// An SSTable is written in a single forward pass, so neither the filter block nor the index
/// block has a known offset until every data block has already been written. The footer resolves
/// the bootstrapping problem: it sits at a fixed distance from the end of the file, so a reader
/// opens the table by seeking to <c>length - 40</c>, and from there learns where everything else
/// lives. Writing it last also makes it a commit record — a table whose footer is missing or
/// whose magic does not match was never finished, which is exactly what a crash during a flush
/// leaves behind.
/// </para>
/// <code>
/// uint64 filterOffset
/// uint64 filterSize
/// uint64 indexOffset
/// uint64 indexSize
/// uint64 magic
/// </code>
/// </remarks>
internal readonly record struct SsTableFooter(BlockHandle Filter, BlockHandle Index)
{
    /// <summary>Encoded size of the footer.</summary>
    internal const int Size = 40;

    /// <summary>
    /// Identifies a finished KestrelCache SSTable. An arbitrary constant, chosen so that a
    /// truncated or unrelated file fails to open rather than being misread as a table.
    /// </summary>
    internal const ulong Magic = 0x4B43_5353_5441_424CUL; // "KCSSTABL"

    internal void Encode(Span<byte> destination)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(destination, (ulong)Filter.Offset);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[8..], (ulong)Filter.Size);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[16..], (ulong)Index.Offset);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[24..], (ulong)Index.Size);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[32..], Magic);
    }

    internal static SsTableFooter Decode(ReadOnlySpan<byte> source, string? file, long fileLength)
    {
        if (source.Length < Size)
        {
            throw new CorruptRecordException("SSTable is shorter than its footer.", 0, file);
        }

        ulong magic = BinaryPrimitives.ReadUInt64LittleEndian(source[32..]);
        if (magic != Magic)
        {
            throw new CorruptRecordException(
                "SSTable footer magic does not match; the table was never finished or is not a "
                    + "KestrelCache table.",
                fileLength - Size,
                file);
        }

        var filter = new BlockHandle(
            (long)BinaryPrimitives.ReadUInt64LittleEndian(source),
            (int)BinaryPrimitives.ReadUInt64LittleEndian(source[8..]));

        var index = new BlockHandle(
            (long)BinaryPrimitives.ReadUInt64LittleEndian(source[16..]),
            (int)BinaryPrimitives.ReadUInt64LittleEndian(source[24..]));

        foreach (var handle in (BlockHandle[])[filter, index])
        {
            if (handle.Offset < 0
                || handle.Size < 0
                || handle.Offset + handle.Size + BlockHandle.TrailerSize > fileLength)
            {
                throw new CorruptRecordException(
                    $"Footer points at a block at {handle.Offset} of {handle.Size} bytes, "
                        + $"outside a {fileLength}-byte file.",
                    fileLength - Size,
                    file);
            }
        }

        return new SsTableFooter(filter, index);
    }
}
