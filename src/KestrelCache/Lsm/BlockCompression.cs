using System.IO.Compression;

namespace KestrelCache.Lsm;

/// <summary>How a block's bytes are stored on disk.</summary>
internal enum CompressionKind : byte
{
    /// <summary>Stored verbatim.</summary>
    None = 0,

    /// <summary>Deflate (RFC 1951).</summary>
    Deflate = 1,
}

/// <summary>
/// Per-block compression.
/// </summary>
/// <remarks>
/// <para>
/// Compression is applied per block rather than per file, and that choice is the whole point.
/// A whole-file stream would have to be decompressed from the beginning to reach any given key,
/// which destroys the random access the block index exists to provide. Compressing each 4 KiB
/// block independently keeps a point lookup at one block read plus one decompression.
/// </para>
/// <para>
/// A block is stored compressed only if doing so saves at least an eighth of its size. Below
/// that the saving does not pay for decompressing on every read, and a block of
/// already-incompressible data (random bytes, or anything pre-compressed) can come out of a
/// compressor slightly larger than it went in.
/// </para>
/// <para>
/// Deflate is used because it is in the base class library. Production engines reach for LZ4 or
/// Zstandard instead, which are several times faster at comparable ratios for this access
/// pattern; the format carries a type byte per block precisely so another codec can be added
/// without breaking existing files.
/// </para>
/// </remarks>
internal static class BlockCompression
{
    /// <summary>Minimum fraction of a block that must be saved for compression to be worth it.</summary>
    private const double MinimumSaving = 0.125;

    /// <summary>
    /// Compresses <paramref name="block"/> if that is worthwhile, reporting which form was chosen.
    /// </summary>
    internal static ReadOnlyMemory<byte> Compress(
        ReadOnlyMemory<byte> block,
        bool enabled,
        out CompressionKind kind)
    {
        kind = CompressionKind.None;
        if (!enabled || block.Length < 64) return block;

        using var output = new MemoryStream(block.Length);
        using (var deflate = new DeflateStream(output, CompressionLevel.Fastest, leaveOpen: true))
        {
            deflate.Write(block.Span);
        }

        byte[] compressed = output.ToArray();
        if (compressed.Length > block.Length * (1.0 - MinimumSaving))
        {
            return block;
        }

        kind = CompressionKind.Deflate;
        return compressed;
    }

    /// <summary>Restores a block to its uncompressed form.</summary>
    internal static byte[] Decompress(ReadOnlySpan<byte> stored, CompressionKind kind, long offset)
    {
        switch (kind)
        {
            case CompressionKind.None:
                return stored.ToArray();

            case CompressionKind.Deflate:
            {
                using var input = new MemoryStream(stored.ToArray());
                using var deflate = new DeflateStream(input, CompressionMode.Decompress);
                using var output = new MemoryStream(stored.Length * 3);
                deflate.CopyTo(output);
                return output.ToArray();
            }

            default:
                throw new CorruptRecordException(
                    $"Block uses unknown compression type {(byte)kind}.", offset);
        }
    }
}
