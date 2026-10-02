using System.Buffers;
using System.Buffers.Text;
using System.IO.Pipelines;
using System.Text;

namespace KestrelCache.Server.Resp;

/// <summary>
/// Writes RESP replies into a <see cref="PipeWriter"/>.
/// </summary>
/// <remarks>
/// Everything is written through <see cref="PipeWriter.GetSpan"/> and
/// <see cref="PipeWriter.Advance"/> rather than through intermediate strings or arrays. For a
/// server whose whole job is turning small requests into small replies, the per-reply
/// allocations are the cost that matters: at a few hundred thousand replies a second, one
/// throwaway string each is enough to make garbage collection, not the storage engine, the
/// bottleneck.
/// </remarks>
internal static class RespWriter
{
    private const byte Plus = (byte)'+';
    private const byte Minus = (byte)'-';
    private const byte Colon = (byte)':';
    private const byte Dollar = (byte)'$';
    private const byte Asterisk = (byte)'*';

    private static ReadOnlySpan<byte> Crlf => "\r\n"u8;

    /// <summary>The canonical <c>+OK</c> reply.</summary>
    internal static void WriteOk(PipeWriter writer) => WriteRaw(writer, "+OK\r\n"u8);

    /// <summary>A simple status reply, e.g. <c>+PONG</c>.</summary>
    internal static void WriteSimpleString(PipeWriter writer, ReadOnlySpan<byte> value)
    {
        var span = writer.GetSpan(1 + value.Length + 2);
        span[0] = Plus;
        value.CopyTo(span[1..]);
        Crlf.CopyTo(span[(1 + value.Length)..]);
        writer.Advance(1 + value.Length + 2);
    }

    /// <summary>A simple status reply from a string.</summary>
    internal static void WriteSimpleString(PipeWriter writer, string value) =>
        WriteSimpleString(writer, Encoding.UTF8.GetBytes(value));

    /// <summary>
    /// An error reply. The first word is conventionally an error code such as <c>ERR</c> or
    /// <c>WRONGTYPE</c>, which clients check.
    /// </summary>
    internal static void WriteError(PipeWriter writer, string message)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(message);
        var span = writer.GetSpan(1 + bytes.Length + 2);
        span[0] = Minus;
        bytes.CopyTo(span[1..]);
        Crlf.CopyTo(span[(1 + bytes.Length)..]);
        writer.Advance(1 + bytes.Length + 2);
    }

    /// <summary>An integer reply.</summary>
    internal static void WriteInteger(PipeWriter writer, long value)
    {
        var span = writer.GetSpan(1 + 20 + 2);
        span[0] = Colon;
        Utf8Formatter.TryFormat(value, span[1..], out int written);
        Crlf.CopyTo(span[(1 + written)..]);
        writer.Advance(1 + written + 2);
    }

    /// <summary>A bulk string reply.</summary>
    internal static void WriteBulkString(PipeWriter writer, ReadOnlySpan<byte> value)
    {
        var header = writer.GetSpan(1 + 20 + 2);
        header[0] = Dollar;
        Utf8Formatter.TryFormat(value.Length, header[1..], out int written);
        Crlf.CopyTo(header[(1 + written)..]);
        writer.Advance(1 + written + 2);

        // Written separately so a large value is not forced through one contiguous span.
        writer.Write(value);
        WriteRaw(writer, Crlf);
    }

    /// <summary>A bulk string reply from a string.</summary>
    internal static void WriteBulkString(PipeWriter writer, string value) =>
        WriteBulkString(writer, Encoding.UTF8.GetBytes(value));

    /// <summary>
    /// The null bulk string, which is how RESP2 says "no such key" — distinct from an empty
    /// string, which is a key whose value happens to have zero length.
    /// </summary>
    internal static void WriteNull(PipeWriter writer) => WriteRaw(writer, "$-1\r\n"u8);

    /// <summary>The null array, used by SCAN-style replies when there is nothing to return.</summary>
    internal static void WriteNullArray(PipeWriter writer) => WriteRaw(writer, "*-1\r\n"u8);

    /// <summary>Writes an array header; the caller then writes exactly <paramref name="count"/> elements.</summary>
    internal static void WriteArrayHeader(PipeWriter writer, long count)
    {
        var span = writer.GetSpan(1 + 20 + 2);
        span[0] = Asterisk;
        Utf8Formatter.TryFormat(count, span[1..], out int written);
        Crlf.CopyTo(span[(1 + written)..]);
        writer.Advance(1 + written + 2);
    }

    /// <summary>Writes an array of bulk strings.</summary>
    internal static void WriteBulkStringArray(PipeWriter writer, IReadOnlyList<byte[]?> values)
    {
        WriteArrayHeader(writer, values.Count);
        foreach (byte[]? value in values)
        {
            if (value is null) WriteNull(writer);
            else WriteBulkString(writer, value);
        }
    }

    private static void WriteRaw(PipeWriter writer, ReadOnlySpan<byte> bytes)
    {
        var span = writer.GetSpan(bytes.Length);
        bytes.CopyTo(span);
        writer.Advance(bytes.Length);
    }
}
