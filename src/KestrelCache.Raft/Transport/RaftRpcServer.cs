using System.Net;
using System.Net.Sockets;

namespace KestrelCache.Raft;

/// <summary>
/// Accepts inbound Raft RPCs from peers and dispatches them to the local node.
/// </summary>
/// <remarks>
/// A connection per peer, each served by its own long-lived task reading frames in a loop. The
/// peers are few and known in advance, so there is no need for the connection-limit machinery
/// the client-facing server has; what matters here is that a slow or wedged peer connection
/// cannot delay the others, which a task per connection gives for free.
/// </remarks>
public sealed class RaftRpcServer(string bindAddress, int port) : IAsyncDisposable
{
    private readonly CancellationTokenSource _shutdown = new();
    private Socket? _listener;
    private RaftNode? _node;
    private int _disposed;

    /// <summary>Creates a server already bound to its node.</summary>
    public RaftRpcServer(RaftNode node, string bindAddress, int port)
        : this(bindAddress, port) => Attach(node);

    /// <summary>
    /// Associates the node this server dispatches to.
    /// </summary>
    /// <remarks>
    /// Separate from the constructor so that a listener can be bound — and its port therefore
    /// known — before the node exists. That ordering is not merely convenient: binding port 0 is
    /// the only way to get an OS-assigned port, and every node needs its peers' ports before it
    /// can be configured, so something has to come first.
    /// </remarks>
    public void Attach(RaftNode node)
    {
        ArgumentNullException.ThrowIfNull(node);
        if (_node is not null)
        {
            throw new InvalidOperationException("This server is already attached to a node.");
        }
        _node = node;
    }

    private RaftNode Node => _node
        ?? throw new InvalidOperationException(
            "Attach a RaftNode before this server can serve requests.");

    /// <summary>The port actually bound.</summary>
    public int BoundPort { get; private set; }

    /// <summary>Frames served since start.</summary>
    public long FramesServed => Interlocked.Read(ref _framesServed);

    private long _framesServed;

    /// <summary>Binds the listening socket, so the port is known before serving begins.</summary>
    public void Bind()
    {
        var address = bindAddress is "*" or "0.0.0.0" or "" ? IPAddress.Any : IPAddress.Parse(bindAddress);

        var listener = new Socket(
            address.AddressFamily == AddressFamily.InterNetworkV6
                ? AddressFamily.InterNetworkV6
                : AddressFamily.InterNetwork,
            SocketType.Stream,
            ProtocolType.Tcp);

        listener.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        listener.Bind(new IPEndPoint(address, port));
        listener.Listen(backlog: 64);

        _listener = listener;
        BoundPort = ((IPEndPoint)listener.LocalEndPoint!).Port;
    }

    /// <summary>Serves until cancelled.</summary>
    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        if (_listener is null) Bind();

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken, _shutdown.Token);

        while (!linked.IsCancellationRequested)
        {
            Socket peer;
            try
            {
                peer = await _listener!.AcceptAsync(linked.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (SocketException)
            {
                continue;
            }

            _ = Task.Run(() => ServePeerAsync(peer, linked.Token), CancellationToken.None);
        }

        _listener?.Dispose();
        _listener = null;
    }

    private async Task ServePeerAsync(Socket peer, CancellationToken cancellationToken)
    {
        peer.NoDelay = true;

        try
        {
            using (peer)
            await using (var stream = new NetworkStream(peer, ownsSocket: false))
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    byte[]? payload = await TcpRaftTransport
                        .ReadFrameAsync(stream, peer.RemoteEndPoint?.ToString(), cancellationToken)
                        .ConfigureAwait(false);

                    if (payload is null) return; // peer closed

                    byte[] reply = await DispatchAsync(payload, cancellationToken)
                        .ConfigureAwait(false);

                    await stream.WriteAsync(reply, cancellationToken).ConfigureAwait(false);
                    await stream.FlushAsync(cancellationToken).ConfigureAwait(false);

                    Interlocked.Increment(ref _framesServed);
                }
            }
        }
        catch (Exception exception) when (
            exception is IOException or SocketException or OperationCanceledException
                or ObjectDisposedException or EndOfStreamException or CorruptRecordException)
        {
            // A peer disconnecting or sending a damaged frame costs that connection and nothing
            // else; it will reconnect.
        }
    }

    private async Task<byte[]> DispatchAsync(byte[] payload, CancellationToken cancellationToken)
    {
        var (kind, reader) = RaftWire.Unframe(payload, null);

        switch (kind)
        {
            case RaftMessageKind.RequestVoteRequest:
            {
                var request = RaftWire.DecodeRequestVoteRequest(reader);
                var response = await Node.HandleRequestVoteAsync(request, cancellationToken)
                    .ConfigureAwait(false);
                return RaftWire.Encode(response);
            }

            case RaftMessageKind.AppendEntriesRequest:
            {
                var request = RaftWire.DecodeAppendEntriesRequest(reader, null);
                var response = await Node.HandleAppendEntriesAsync(request, cancellationToken)
                    .ConfigureAwait(false);
                return RaftWire.Encode(response);
            }

            case RaftMessageKind.InstallSnapshotRequest:
            {
                var request = RaftWire.DecodeInstallSnapshotRequest(reader, null);
                var response = await Node.HandleInstallSnapshotAsync(request, cancellationToken)
                    .ConfigureAwait(false);
                return RaftWire.Encode(response);
            }

            default:
                throw new CorruptRecordException(
                    $"A peer sent a response frame ({kind}) where a request was expected.", 0);
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// Guarded so that disposing twice is safe. Cancelling and then disposing a
    /// <see cref="CancellationTokenSource"/> leaves a second call to throw
    /// <see cref="ObjectDisposedException"/>, and double disposal is not an exotic case: nested
    /// <c>await using</c> blocks, a teardown path that also disposes its children, and an
    /// explicit close followed by a dispose all produce it. Throwing on teardown turns an
    /// orderly shutdown into a crash.
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        await _shutdown.CancelAsync().ConfigureAwait(false);
        _listener?.Dispose();
        _listener = null;
        _shutdown.Dispose();
    }
}
