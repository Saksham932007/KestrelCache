using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace KestrelCache.Raft;

/// <summary>Where a peer can be reached.</summary>
/// <param name="NodeId">The peer's stable identifier.</param>
/// <param name="Host">Hostname or address.</param>
/// <param name="Port">Raft port.</param>
public readonly record struct RaftPeerAddress(string NodeId, string Host, int Port)
{
    /// <summary>Parses <c>id=host:port</c>, the form the CLI accepts.</summary>
    public static RaftPeerAddress Parse(string text)
    {
        int equals = text.IndexOf('=', StringComparison.Ordinal);
        if (equals <= 0)
        {
            throw new FormatException($"Expected 'id=host:port', got '{text}'.");
        }

        string id = text[..equals];
        string endpoint = text[(equals + 1)..];

        int colon = endpoint.LastIndexOf(':');
        if (colon <= 0 || !int.TryParse(endpoint[(colon + 1)..], out int port))
        {
            throw new FormatException($"Expected 'id=host:port', got '{text}'.");
        }

        return new RaftPeerAddress(id, endpoint[..colon], port);
    }

    /// <inheritdoc />
    public override string ToString() => $"{NodeId}={Host}:{Port}";
}

/// <summary>
/// Sends Raft RPCs to peers over TCP.
/// </summary>
/// <remarks>
/// <para>
/// One connection per peer, held open and reused. Opening a connection per RPC would add a
/// three-way handshake to every heartbeat, which at a heartbeat every 50 ms to every peer is
/// both wasteful and a source of latency variance that directly destabilises elections.
/// </para>
/// <para>
/// Each peer's connection is guarded by its own lock, because the protocol here is strictly
/// request-then-response on one socket: two concurrent RPCs sharing a connection would
/// interleave their frames and each read the other's reply. Per-peer rather than global locking
/// keeps a slow or dead peer from blocking traffic to the healthy ones — which matters, because
/// a consensus protocol's whole point is to keep working while some peers are unreachable.
/// </para>
/// <para>
/// A failed RPC is reported as <c>null</c> rather than thrown. In a consensus protocol an
/// unreachable peer is an ordinary condition that the majority requirement exists to handle, not
/// an error; the connection is dropped so the next attempt reconnects.
/// </para>
/// </remarks>
public sealed class TcpRaftTransport : IRaftTransport, IAsyncDisposable
{
    private readonly Dictionary<string, RaftPeerAddress> _addresses;
    private readonly ConcurrentDictionary<string, PeerConnection> _connections = new();
    private readonly TimeSpan _connectTimeout;

    /// <summary>Creates a transport that can reach the given peers.</summary>
    public TcpRaftTransport(IEnumerable<RaftPeerAddress> peers, TimeSpan? connectTimeout = null)
    {
        _addresses = peers.ToDictionary(peer => peer.NodeId);
        _connectTimeout = connectTimeout ?? TimeSpan.FromMilliseconds(500);
    }

    /// <inheritdoc />
    public async Task<RequestVoteResponse?> RequestVoteAsync(
        string peerId,
        RequestVoteRequest request,
        CancellationToken cancellationToken)
    {
        byte[]? reply = await ExchangeAsync(peerId, RaftWire.Encode(request), cancellationToken)
            .ConfigureAwait(false);
        if (reply is null) return null;

        var (kind, reader) = RaftWire.Unframe(reply, peerId);
        if (kind != RaftMessageKind.RequestVoteResponse) return null;

        return RaftWire.DecodeRequestVoteResponse(reader);
    }

    /// <inheritdoc />
    public async Task<AppendEntriesResponse?> AppendEntriesAsync(
        string peerId,
        AppendEntriesRequest request,
        CancellationToken cancellationToken)
    {
        byte[]? reply = await ExchangeAsync(peerId, RaftWire.Encode(request), cancellationToken)
            .ConfigureAwait(false);
        if (reply is null) return null;

        var (kind, reader) = RaftWire.Unframe(reply, peerId);
        if (kind != RaftMessageKind.AppendEntriesResponse) return null;

        return RaftWire.DecodeAppendEntriesResponse(reader);
    }

    private async Task<byte[]?> ExchangeAsync(
        string peerId,
        byte[] frame,
        CancellationToken cancellationToken)
    {
        if (!_addresses.TryGetValue(peerId, out var address)) return null;

        var connection = _connections.GetOrAdd(peerId, _ => new PeerConnection());

        await connection.Gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var stream = await connection
                .EnsureConnectedAsync(address, _connectTimeout, cancellationToken)
                .ConfigureAwait(false);

            if (stream is null) return null;

            await stream.WriteAsync(frame, cancellationToken).ConfigureAwait(false);
            await stream.FlushAsync(cancellationToken).ConfigureAwait(false);

            return await ReadFrameAsync(stream, peerId, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (
            exception is IOException or SocketException or ObjectDisposedException
                or OperationCanceledException or CorruptRecordException)
        {
            // The connection is now of unknown state, so it is discarded rather than reused.
            connection.Reset();
            return null;
        }
        finally
        {
            connection.Gate.Release();
        }
    }

    internal static async Task<byte[]?> ReadFrameAsync(
        Stream stream,
        string? peer,
        CancellationToken cancellationToken)
    {
        byte[] header = new byte[RaftWire.FrameHeaderSize];
        int read = await ReadExactlyAsync(stream, header, cancellationToken).ConfigureAwait(false);
        if (read == 0) return null;

        uint frameLength = BinaryPrimitives.ReadUInt32LittleEndian(header);
        uint expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(4));

        if (frameLength < 4 || frameLength > RaftWire.MaxFrameSize)
        {
            throw new CorruptRecordException($"Implausible frame length {frameLength}.", 0, peer);
        }

        // frameLength counts the checksum field, which has already been read.
        int payloadLength = (int)frameLength - 4;
        byte[] payload = new byte[payloadLength];

        if (await ReadExactlyAsync(stream, payload, cancellationToken).ConfigureAwait(false) == 0)
        {
            return null;
        }

        RaftWire.ValidateChecksum(payload, expectedCrc, peer);
        return payload;
    }

    private static async Task<int> ReadExactlyAsync(
        Stream stream,
        byte[] buffer,
        CancellationToken cancellationToken)
    {
        int read = 0;
        while (read < buffer.Length)
        {
            int n = await stream
                .ReadAsync(buffer.AsMemory(read), cancellationToken)
                .ConfigureAwait(false);

            if (n == 0)
            {
                // A clean close before any bytes is an ordinary disconnect; mid-frame it is a
                // truncated message.
                if (read == 0) return 0;
                throw new EndOfStreamException(
                    $"Connection closed after {read} of {buffer.Length} bytes.");
            }

            read += n;
        }
        return read;
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        foreach (var (_, connection) in _connections)
        {
            connection.Dispose();
        }
        _connections.Clear();
        return ValueTask.CompletedTask;
    }

    private sealed class PeerConnection : IDisposable
    {
        internal SemaphoreSlim Gate { get; } = new(1, 1);

        private TcpClient? _client;
        private NetworkStream? _stream;

        internal async Task<NetworkStream?> EnsureConnectedAsync(
            RaftPeerAddress address,
            TimeSpan connectTimeout,
            CancellationToken cancellationToken)
        {
            if (_stream is not null && _client is { Connected: true })
            {
                return _stream;
            }

            Reset();

            var client = new TcpClient { NoDelay = true };

            try
            {
                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                timeout.CancelAfter(connectTimeout);

                await client.ConnectAsync(address.Host, address.Port, timeout.Token)
                    .ConfigureAwait(false);
            }
            catch (Exception exception) when (
                exception is SocketException or OperationCanceledException)
            {
                client.Dispose();
                return null;
            }

            _client = client;
            _stream = client.GetStream();
            return _stream;
        }

        internal void Reset()
        {
            _stream?.Dispose();
            _client?.Dispose();
            _stream = null;
            _client = null;
        }

        public void Dispose()
        {
            Reset();
            Gate.Dispose();
        }
    }
}
