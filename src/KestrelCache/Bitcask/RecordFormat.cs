using System.Buffers.Binary;
using System.IO.Hashing;

namespace KestrelCache.Bitcask;

/// <summary>
/// Binary layout of a single append-only log record, and the validation rules that make
/// recovery from a damaged file safe.
/// </summary>
/// <remarks>
/// <para><b>File layout</b></para>
/// <code>
/// +--------------------------------------------------------------+
/// | File header (8 bytes), written once at offset 0              |
/// |   magic         4   "KCB1"                                   |
/// |   formatVersion 2   uint16, currently 1                      |
/// |   reserved      2   zero                                     |
/// +--------------------------------------------------------------+
/// | Record 0 | Record 1 | Record 2 | ...   (append-only)         |
/// +--------------------------------------------------------------+
/// </code>
///
/// <para><b>Record layout</b></para>
/// <code>
/// offset size field
///      0    4 crc32       CRC-32 over every following byte of the record
///      4    4 keyLength   int32, 1 .. MaxKeySize
///      8    4 valueLength int32, 0 .. MaxValueSize
///     12    1 flags       bit 0 = tombstone (this record deletes the key)
///                           bit 1 = more records follow in the same atomic batch
///     13    k key         UTF-8 (or arbitrary) key bytes
///   13+k    v value       value bytes; always empty for a tombstone
/// </code>
///
/// <para><b>Two deliberate changes from the original format</b></para>
/// <para>
/// First, the checksum covers the length fields and the flags byte, not just the key and value.
/// This closes a real hole: the length fields are precisely the bytes that, when corrupted,
/// cause the reader to allocate an absurd buffer or walk off the end of a record. Protecting
/// the payload while leaving its framing unprotected guards the wrong thing.
/// </para>
/// <para>
/// Second, deletion is signalled by a flags bit rather than by a sentinel
/// <c>valueLength == -1</c>. Overloading a length field with a sentinel meant every piece of
/// arithmetic involving that length had to special-case it, and the original recovery loop
/// did not: it computed the next record's offset as <c>position + valueLength</c>, which for a
/// tombstone subtracted one byte and left every subsequent key in the index pointing one byte
/// short of its real record. Making the length always mean the length removes the whole class
/// of bug rather than patching the one instance of it.
/// </para>
/// </remarks>
internal static class RecordFormat
{
    /// <summary>Bytes of fixed header on every record: crc + keyLen + valueLen + flags.</summary>
    internal const int HeaderSize = 13;

    /// <summary>Bytes of fixed header at the start of the file.</summary>
    internal const int FileHeaderSize = 8;

    /// <summary>Current on-disk format version.</summary>
    internal const ushort FormatVersion = 1;

    private const int CrcOffset = 0;
    private const int KeyLenOffset = 4;
    private const int ValueLenOffset = 8;
    private const int FlagsOffset = 12;

    private const byte FlagTombstone = 0b0000_0001;

    /// <summary>
    /// Set on every record of an atomic batch except the last one.
    /// </summary>
    /// <remarks>
    /// This single bit is what makes a multi-key batch atomic across a crash. Recovery buffers
    /// records carrying the flag and applies them only when it reaches the unflagged record that
    /// terminates the batch. If the process dies partway through writing a batch, the surviving
    /// records are all flagged, the terminator never arrives, and recovery discards them — so the
    /// batch is all-or-nothing. A plain single-key write is simply a batch of one: unflagged, and
    /// applied immediately.
    /// </remarks>
    private const byte FlagBatchContinues = 0b0000_0010;

    private const byte AllKnownFlags = FlagTombstone | FlagBatchContinues;

    private static ReadOnlySpan<byte> Magic => "KCB1"u8;

    /// <summary>Total encoded size of a record with the given key and value lengths.</summary>
    internal static int RecordSize(int keyLength, int valueLength) =>
        HeaderSize + keyLength + valueLength;

    /// <summary>Writes the 8-byte file header into <paramref name="destination"/>.</summary>
    internal static void WriteFileHeader(Span<byte> destination)
    {
        if (destination.Length < FileHeaderSize)
            throw new ArgumentException("Destination too small for file header.", nameof(destination));

        Magic.CopyTo(destination);
        BinaryPrimitives.WriteUInt16LittleEndian(destination[4..], FormatVersion);
        destination[6] = 0;
        destination[7] = 0;
    }

    /// <summary>
    /// Validates a file header, throwing <see cref="CorruptRecordException"/> if the magic or
    /// version does not match.
    /// </summary>
    internal static void ValidateFileHeader(ReadOnlySpan<byte> header, string? path)
    {
        if (header.Length < FileHeaderSize)
            throw new CorruptRecordException("File is shorter than its header.", 0, path);

        if (!header[..4].SequenceEqual(Magic))
            throw new CorruptRecordException("Bad magic; not a KestrelCache data file.", 0, path);

        ushort version = BinaryPrimitives.ReadUInt16LittleEndian(header[4..]);
        if (version != FormatVersion)
        {
            throw new CorruptRecordException(
                $"Unsupported format version {version}; this build understands version {FormatVersion}.",
                0,
                path);
        }
    }

    /// <summary>
    /// Encodes one record into <paramref name="destination"/> and returns the number of bytes written.
    /// </summary>
    internal static int Encode(
        Span<byte> destination,
        ReadOnlySpan<byte> key,
        ReadOnlySpan<byte> value,
        bool isTombstone,
        bool continuesBatch = false)
    {
        int size = RecordSize(key.Length, value.Length);
        if (destination.Length < size)
            throw new ArgumentException("Destination too small for record.", nameof(destination));

        BinaryPrimitives.WriteInt32LittleEndian(destination[KeyLenOffset..], key.Length);
        BinaryPrimitives.WriteInt32LittleEndian(destination[ValueLenOffset..], value.Length);
        byte flags = 0;
        if (isTombstone) flags |= FlagTombstone;
        if (continuesBatch) flags |= FlagBatchContinues;
        destination[FlagsOffset] = flags;
        key.CopyTo(destination[HeaderSize..]);
        value.CopyTo(destination[(HeaderSize + key.Length)..]);

        // Checksum everything after the checksum field itself, framing included.
        uint crc = Crc32.HashToUInt32(destination[4..size]);
        BinaryPrimitives.WriteUInt32LittleEndian(destination[CrcOffset..], crc);

        return size;
    }

    /// <summary>Decoded header fields of a record.</summary>
    internal readonly record struct Header(
        uint Crc,
        int KeyLength,
        int ValueLength,
        bool IsTombstone,
        bool ContinuesBatch)
    {
        /// <summary>Total on-disk size of the record this header describes.</summary>
        internal int RecordSize => HeaderSize + KeyLength + ValueLength;
    }

    /// <summary>
    /// Parses and sanity-checks a record header.
    /// </summary>
    /// <remarks>
    /// The bounds checks here are the reason a corrupt file cannot take the process down. A
    /// flipped bit in a length field previously reached <c>new byte[keyLen]</c> unexamined,
    /// so a damaged header could request a multi-gigabyte allocation (or, with the sign bit
    /// set, a negative one). Rejecting implausible lengths before allocating turns an
    /// <c>OutOfMemoryException</c> into a diagnosable <see cref="CorruptRecordException"/>.
    /// </remarks>
    internal static bool TryParseHeader(
        ReadOnlySpan<byte> source,
        int maxKeySize,
        int maxValueSize,
        out Header header,
        out string? error)
    {
        header = default;
        error = null;

        if (source.Length < HeaderSize)
        {
            error = "Truncated record header.";
            return false;
        }

        uint crc = BinaryPrimitives.ReadUInt32LittleEndian(source[CrcOffset..]);
        int keyLength = BinaryPrimitives.ReadInt32LittleEndian(source[KeyLenOffset..]);
        int valueLength = BinaryPrimitives.ReadInt32LittleEndian(source[ValueLenOffset..]);
        byte flags = source[FlagsOffset];

        if (keyLength <= 0 || keyLength > maxKeySize)
        {
            error = $"Implausible key length {keyLength} (limit {maxKeySize}).";
            return false;
        }

        if (valueLength < 0 || valueLength > maxValueSize)
        {
            error = $"Implausible value length {valueLength} (limit {maxValueSize}).";
            return false;
        }

        if ((flags & ~AllKnownFlags) != 0)
        {
            error = $"Unknown record flags 0x{flags:X2}.";
            return false;
        }

        bool isTombstone = (flags & FlagTombstone) != 0;
        if (isTombstone && valueLength != 0)
        {
            error = "Tombstone carries a non-empty value.";
            return false;
        }

        header = new Header(crc, keyLength, valueLength, isTombstone, (flags & FlagBatchContinues) != 0);
        return true;
    }

    /// <summary>
    /// Verifies that a fully-read record matches its stored checksum.
    /// </summary>
    /// <param name="record">The complete record, header included.</param>
    /// <param name="header">The already-parsed header for that record.</param>
    internal static bool VerifyChecksum(ReadOnlySpan<byte> record, in Header header)
    {
        int size = header.RecordSize;
        if (record.Length < size) return false;
        return Crc32.HashToUInt32(record[4..size]) == header.Crc;
    }

    /// <summary>Returns the key bytes inside a complete record.</summary>
    internal static ReadOnlySpan<byte> KeyOf(ReadOnlySpan<byte> record, in Header header) =>
        record.Slice(HeaderSize, header.KeyLength);

    /// <summary>Returns the value bytes inside a complete record.</summary>
    internal static ReadOnlySpan<byte> ValueOf(ReadOnlySpan<byte> record, in Header header) =>
        record.Slice(HeaderSize + header.KeyLength, header.ValueLength);
}
