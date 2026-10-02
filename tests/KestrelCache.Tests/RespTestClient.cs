using System.Buffers;
using System.Net.Sockets;
using System.Text;

namespace KestrelCache.Tests;

/// <summary>A RESP reply, in the shape the protocol distinguishes.</summary>
public abstract record RespReply
{
    /// <summary>A status reply, e.g. <c>+OK</c>.</summary>
    public sealed record Status(string Value) : RespReply;

    /// <summary>An error reply, e.g. <c>-ERR ...</c>.</summary>
    public sealed record Error(string Message) : RespReply;

    /// <summary>An integer reply.</summary>
    public sealed record Number(long Value) : RespReply;

    /// <summary>A bulk string reply. <see cref="Value"/> is null for the null bulk string.</summary>
    public sealed record Bulk(byte[]? Value) : RespReply
    {
        /// <summary>The payload decoded as UTF-8, or null.</summary>
        public string? Text => Value is null ? null : Encoding.UTF8.GetString(Value);
    }

    /// <summary>An array reply. <see cref="Items"/> is null for the null array.</summary>
    public sealed record Array(RespReply[]? Items) : RespReply;

    /// <summary>Convenience: this reply's text, whatever its type.</summary>
    public string? AsText => this switch
    {
        Status status => status.Value,
        Error error => error.Message,
        Number number => number.Value.ToString(),
        Bulk bulk => bulk.Text,
        _ => null,
    };
}

/// <summary>
/// A small RESP client, used to test the server over a real TCP socket.
/// </summary>
/// <remarks>
/// Written rather than taking a dependency on a Redis client library, for two reasons. A client
/// library is built to paper over protocol differences — retrying, reconnecting, normalising
/// replies — which is exactly the behaviour that would hide a protocol bug instead of exposing
/// it. And testing against a hand-written client means the tests assert on the bytes the server
/// actually produces, including the distinction between a null bulk string and an empty one,
/// which is the kind of thing a convenience layer flattens.
/// </remarks>
public sealed class RespTestClient : IDisposable
{
    private readonly TcpClient _client;
    private readonly NetworkStream _stream;
    private readonly byte[] _buffer = new byte[64 * 1024];
    private int _length;
    private int _offset;

    private RespTestClient(TcpClient client)
    {
        _client = client;
        _stream = client.GetStream();
    }

    /// <summary>Connects to a server on localhost.</summary>
    public static async Task<RespTestClient> ConnectAsync(int port)
    {
        var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", port);
        client.NoDelay = true;
        return new RespTestClient(client);
    }

    /// <summary>Sends a command and reads its reply.</summary>
    public async Task<RespReply> CommandAsync(params string[] arguments)
    {
        await SendAsync(arguments);
        return await ReadReplyAsync();
    }

    /// <summary>Sends a command without reading the reply, for pipelining tests.</summary>
    public async Task SendAsync(params string[] arguments)
    {
        var request = new StringBuilder();
        request.Append('*').Append(arguments.Length).Append("\r\n");

        foreach (string argument in arguments)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(argument);
            request.Append('$').Append(bytes.Length).Append("\r\n").Append(argument).Append("\r\n");
        }

        byte[] payload = Encoding.UTF8.GetBytes(request.ToString());
        await _stream.WriteAsync(payload);
        await _stream.FlushAsync();
    }

    /// <summary>Sends raw bytes, for protocol-error tests.</summary>
    public async Task SendRawAsync(string text)
    {
        await _stream.WriteAsync(Encoding.UTF8.GetBytes(text));
        await _stream.FlushAsync();
    }

    /// <summary>Convenience: asserts nothing, just returns the reply's text.</summary>
    public async Task<string?> TextAsync(params string[] arguments) =>
        (await CommandAsync(arguments)).AsText;

    /// <summary>Reads one reply.</summary>
    public async Task<RespReply> ReadReplyAsync()
    {
        byte marker = await ReadByteAsync();

        switch ((char)marker)
        {
            case '+':
                return new RespReply.Status(await ReadLineAsync());

            case '-':
                return new RespReply.Error(await ReadLineAsync());

            case ':':
                return new RespReply.Number(long.Parse(await ReadLineAsync()));

            case '$':
            {
                long length = long.Parse(await ReadLineAsync());
                if (length < 0) return new RespReply.Bulk(null);

                var payload = new byte[length];
                for (long i = 0; i < length; i++)
                {
                    payload[i] = await ReadByteAsync();
                }

                // Consume the trailing CRLF.
                await ReadByteAsync();
                await ReadByteAsync();
                return new RespReply.Bulk(payload);
            }

            case '*':
            {
                long count = long.Parse(await ReadLineAsync());
                if (count < 0) return new RespReply.Array(null);

                var items = new RespReply[count];
                for (long i = 0; i < count; i++)
                {
                    items[i] = await ReadReplyAsync();
                }
                return new RespReply.Array(items);
            }

            default:
                throw new InvalidDataException(
                    $"Unexpected RESP type marker '{(char)marker}' (0x{marker:X2}).");
        }
    }

    private async Task<byte> ReadByteAsync()
    {
        if (_offset >= _length)
        {
            _length = await _stream.ReadAsync(_buffer);
            _offset = 0;

            if (_length == 0)
            {
                throw new EndOfStreamException("The server closed the connection.");
            }
        }

        return _buffer[_offset++];
    }

    private async Task<string> ReadLineAsync()
    {
        var bytes = new ArrayBufferWriter<byte>(64);

        while (true)
        {
            byte current = await ReadByteAsync();
            if (current == (byte)'\r')
            {
                await ReadByteAsync(); // the '\n'
                break;
            }

            bytes.GetSpan(1)[0] = current;
            bytes.Advance(1);
        }

        return Encoding.UTF8.GetString(bytes.WrittenSpan);
    }

    public void Dispose()
    {
        _stream.Dispose();
        _client.Dispose();
    }
}
