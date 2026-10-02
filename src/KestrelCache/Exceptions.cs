namespace KestrelCache;

/// <summary>Base type for every error the storage engine raises deliberately.</summary>
public class KestrelCacheException : Exception
{
    public KestrelCacheException(string message) : base(message) { }
    public KestrelCacheException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>
/// A record on disk failed validation: a bad checksum, an impossible length field, or a record
/// that runs past the end of the file.
/// </summary>
/// <remarks>
/// This is deliberately a distinct, catchable type rather than a bare <see cref="IOException"/>.
/// Recovery code needs to tell "this file is damaged at offset N" apart from "the disk is on fire",
/// because the two call for different responses: truncate-and-continue versus fail-fast.
/// </remarks>
public sealed class CorruptRecordException : KestrelCacheException
{
    public CorruptRecordException(string message, long offset, string? file = null)
        : base($"{message} (file: {file ?? "<unknown>"}, offset: {offset})")
    {
        Offset = offset;
        File = file;
    }

    /// <summary>Byte offset in the data file at which the damaged record starts.</summary>
    public long Offset { get; }

    /// <summary>Path of the file holding the damaged record, when known.</summary>
    public string? File { get; }
}

/// <summary>Raised when a key or value exceeds the configured size limits.</summary>
public sealed class KeyOrValueTooLargeException : KestrelCacheException
{
    public KeyOrValueTooLargeException(string message) : base(message) { }
}
