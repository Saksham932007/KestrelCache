using System.Buffers;
using System.Text;

namespace KestrelCache.Server.Resp;

/// <summary>Outcome of trying to parse one command out of a buffer.</summary>
internal enum ParseStatus
{
    /// <summary>Not enough bytes have arrived yet; wait for more and try again.</summary>
    Incomplete,

    /// <summary>A complete command was parsed.</summary>
    Complete,

    /// <summary>The bytes are not valid RESP; the connection should be told and closed.</summary>
    Protocol,
}

/// <summary>
/// Parses client requests in the Redis serialization protocol.
/// </summary>
/// <remarks>
/// <para>
/// Speaking RESP rather than inventing a protocol is the single highest-leverage decision in
/// this server. It means <c>redis-cli</c> connects and works, every Redis client library in
/// every language already speaks to it, and — most usefully — <c>redis-benchmark</c> can be
/// pointed at it to produce numbers directly comparable with Redis itself. A bespoke protocol
/// would have required writing all of that.
/// </para>
/// <para>
/// A request is an array of bulk strings:
/// </para>
/// <code>
/// *3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n
/// </code>
/// <para>
/// Inline commands — a bare <c>PING\r\n</c> — are also accepted, because that is what a human
/// typing into <c>telnet</c> or <c>nc</c> sends, and supporting it costs a dozen lines.
/// </para>
/// <para>
/// The parser works over a <see cref="ReadOnlySequence{T}"/> straight from the socket's
/// <c>PipeReader</c>, which is what lets it handle a command split across TCP segments without
/// copying or buffering a whole message itself. Partial input is not an error condition here but
/// the normal case: it returns <see cref="ParseStatus.Incomplete"/>, consumes nothing, and is
/// called again when more bytes land.
/// </para>
/// </remarks>
internal static class RespParser
{
    private const byte Asterisk = (byte)'*';
    private const byte Dollar = (byte)'$';
    private const byte CarriageReturn = (byte)'\r';
    private const byte LineFeed = (byte)'\n';

    /// <summary>
    /// Attempts to parse one command. On <see cref="ParseStatus.Complete"/>,
    /// <paramref name="position"/> is advanced past the command and <paramref name="arguments"/>
    /// holds its parts.
    /// </summary>
    internal static ParseStatus TryParse(
        in ReadOnlySequence<byte> buffer,
        RespLimits limits,
        List<byte[]> arguments,
        out SequencePosition position,
        out string? error)
    {
        arguments.Clear();
        error = null;
        position = buffer.Start;

        var reader = new SequenceReader<byte>(buffer);

        if (!reader.TryPeek(out byte first))
        {
            return ParseStatus.Incomplete;
        }

        ParseStatus status = first == Asterisk
            ? ParseArray(ref reader, limits, arguments, out error)
            : ParseInline(ref reader, limits, arguments, out error);

        if (status == ParseStatus.Complete)
        {
            position = reader.Position;
        }

        return status;
    }

    private static ParseStatus ParseArray(
        ref SequenceReader<byte> reader,
        RespLimits limits,
        List<byte[]> arguments,
        out string? error)
    {
        error = null;

        if (!TryReadLine(ref reader, out ReadOnlySpan<byte> header))
        {
            return ParseStatus.Incomplete;
        }

        // header is "*<count>"
        if (!TryParseInteger(header[1..], out long count))
        {
            error = "invalid multibulk length";
            return ParseStatus.Protocol;
        }

        if (count <= 0)
        {
            // An empty or null array is a well-formed no-op rather than an error; some clients
            // send one while negotiating.
            return ParseStatus.Complete;
        }

        if (count > limits.MaxArguments)
        {
            error = $"invalid multibulk length (limit {limits.MaxArguments})";
            return ParseStatus.Protocol;
        }

        for (long i = 0; i < count; i++)
        {
            if (!TryReadLine(ref reader, out ReadOnlySpan<byte> bulkHeader))
            {
                return ParseStatus.Incomplete;
            }

            if (bulkHeader.Length == 0 || bulkHeader[0] != Dollar)
            {
                error = $"expected '$', got '{(bulkHeader.Length > 0 ? (char)bulkHeader[0] : ' ')}'";
                return ParseStatus.Protocol;
            }

            if (!TryParseInteger(bulkHeader[1..], out long length))
            {
                error = "invalid bulk length";
                return ParseStatus.Protocol;
            }

            if (length < 0 || length > limits.MaxBulkLength)
            {
                error = $"invalid bulk length (limit {limits.MaxBulkLength})";
                return ParseStatus.Protocol;
            }

            // The payload plus its trailing CRLF must all have arrived.
            if (reader.Remaining < length + 2)
            {
                return ParseStatus.Incomplete;
            }

            var payload = new byte[length];
            if (!reader.TryCopyTo(payload))
            {
                return ParseStatus.Incomplete;
            }
            reader.Advance(length);

            if (!reader.TryRead(out byte cr) || !reader.TryRead(out byte lf)
                || cr != CarriageReturn || lf != LineFeed)
            {
                error = "bulk string not terminated by CRLF";
                return ParseStatus.Protocol;
            }

            arguments.Add(payload);
        }

        return ParseStatus.Complete;
    }

    /// <summary>
    /// Parses a whitespace-separated inline command, which is what a human at a <c>telnet</c>
    /// prompt sends.
    /// </summary>
    private static ParseStatus ParseInline(
        ref SequenceReader<byte> reader,
        RespLimits limits,
        List<byte[]> arguments,
        out string? error)
    {
        error = null;

        if (!TryReadLine(ref reader, out ReadOnlySpan<byte> line))
        {
            // Guard against a client that opens a connection and streams bytes without ever
            // sending a newline, which would otherwise grow the read buffer without bound.
            if (reader.Length > limits.MaxInlineLength)
            {
                error = "too big inline request";
                return ParseStatus.Protocol;
            }
            return ParseStatus.Incomplete;
        }

        string text = Encoding.UTF8.GetString(line);
        foreach (string token in text.Split(
            (char[])[' ', '\t'], StringSplitOptions.RemoveEmptyEntries))
        {
            if (arguments.Count >= limits.MaxArguments)
            {
                error = "too many arguments";
                return ParseStatus.Protocol;
            }
            arguments.Add(Encoding.UTF8.GetBytes(token));
        }

        return ParseStatus.Complete;
    }

    /// <summary>Reads up to the next CRLF, leaving the reader positioned after it.</summary>
    private static bool TryReadLine(ref SequenceReader<byte> reader, out ReadOnlySpan<byte> line)
    {
        line = default;

        if (!reader.TryReadTo(out ReadOnlySequence<byte> sequence, CarriageReturn, advancePastDelimiter: true))
        {
            return false;
        }

        if (!reader.TryRead(out byte lf) || lf != LineFeed)
        {
            return false;
        }

        // Copying here is deliberate: the caller only ever needs a short header line, and
        // returning a span into a multi-segment sequence would require the caller to handle
        // segmentation for no benefit.
        line = sequence.IsSingleSegment ? sequence.FirstSpan : sequence.ToArray();
        return true;
    }

    private static bool TryParseInteger(ReadOnlySpan<byte> span, out long value)
    {
        value = 0;
        if (span.Length == 0) return false;

        bool negative = span[0] == (byte)'-';
        int index = negative ? 1 : 0;
        if (index >= span.Length) return false;

        long result = 0;
        for (; index < span.Length; index++)
        {
            byte digit = span[index];
            if (digit is < (byte)'0' or > (byte)'9') return false;

            result = (result * 10) + (digit - '0');
            if (result > int.MaxValue) return false; // bounded well below overflow
        }

        value = negative ? -result : result;
        return true;
    }
}

/// <summary>
/// Caps on what a single request may ask for, so one misbehaving or malicious client cannot
/// make the server allocate without bound.
/// </summary>
internal readonly record struct RespLimits(int MaxArguments, int MaxBulkLength, int MaxInlineLength)
{
    internal static RespLimits Default => new(
        MaxArguments: 1024 * 1024,
        MaxBulkLength: 512 * 1024 * 1024,
        MaxInlineLength: 64 * 1024);
}
