using System.Net;
using System.Net.Sockets;
using KestrelCache.Server.Observability;
using KestrelCache.Server.Resp;

namespace KestrelCache.Server;

/// <summary>
/// Accepts client connections and serves each one on its own task.
/// </summary>
/// <remarks>
/// <para>
/// A task per connection rather than an explicit event loop, because .NET's socket
/// implementation is asynchronous underneath: a connection awaiting a read occupies no thread,
/// so ten thousand idle connections cost ten thousand cheap state machines rather than ten
/// thousand threads. Writing a reactor by hand would re-implement what the thread pool and the
/// I/O completion machinery already do, and do it worse.
/// </para>
/// <para>
/// The connection limit exists so that resource exhaustion produces a clear refusal rather than
/// a slow collapse. Past the limit a client gets a RESP error and a closed socket, which a
/// client library reports usefully, instead of the server thrashing and timing everyone out.
/// </para>
/// </remarks>
public sealed class RespServer(ServerOptions options, KestrelDb database) : IAsyncDisposable
{
    private readonly ServerMetrics _metrics = new();
    private readonly CancellationTokenSource _shutdown = new();
    private readonly List<Task> _connections = [];
    private readonly object _connectionsGate = new();

    private Socket? _listener;
    private long _nextConnectionId;
    private int _disposed;

    /// <summary>The port actually bound, which differs from the request when port 0 was asked for.</summary>
    public int BoundPort { get; private set; }

    /// <summary>Counters for the metrics endpoint.</summary>
    internal ServerMetrics Metrics => _metrics;

    /// <summary>Binds the listening socket, so the port is known before serving begins.</summary>
    public void Bind()
    {
        (_listener, int port) = ListenerFactory.Listen(options.BindAddress, options.Port, backlog: 512);
        BoundPort = port;
    }

    /// <summary>Accepts connections until cancelled.</summary>
    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        if (_listener is null) Bind();

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken, _shutdown.Token);

        try
        {
            while (!linked.IsCancellationRequested)
            {
                Socket client;
                try
                {
                    client = await _listener!.AcceptAsync(linked.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (SocketException)
                {
                    // A transient accept failure should not end the server.
                    continue;
                }

                if (options.MaxConnections > 0 && _metrics.ActiveConnections >= options.MaxConnections)
                {
                    _metrics.ConnectionRejected();
                    await RefuseAsync(client).ConfigureAwait(false);
                    continue;
                }

                _metrics.ConnectionAccepted();
                long id = Interlocked.Increment(ref _nextConnectionId);

                var task = Task.Run(
                    async () =>
                    {
                        try
                        {
                            var connection = new RespConnection(
                                client, id, database, options, _metrics);
                            await connection.ServeAsync(linked.Token).ConfigureAwait(false);
                        }
                        finally
                        {
                            _metrics.ConnectionClosed();
                            try
                            {
                                client.Shutdown(SocketShutdown.Both);
                            }
                            catch (SocketException)
                            {
                                // Already gone.
                            }
                            client.Dispose();
                        }
                    },
                    CancellationToken.None);

                Track(task);
            }
        }
        finally
        {
            _listener?.Dispose();
            _listener = null;
        }

        await DrainAsync().ConfigureAwait(false);
    }

    private static async Task RefuseAsync(Socket client)
    {
        try
        {
            byte[] message = System.Text.Encoding.UTF8.GetBytes(
                "-ERR max number of clients reached\r\n");
            await client.SendAsync(message, SocketFlags.None).ConfigureAwait(false);
        }
        catch (SocketException)
        {
            // Nothing to do; the client is being dropped anyway.
        }
        finally
        {
            client.Dispose();
        }
    }

    private void Track(Task task)
    {
        lock (_connectionsGate)
        {
            _connections.Add(task);

            // Completed tasks are pruned opportunistically, so a long-running server does not
            // accumulate a list entry per connection it has ever served.
            if (_connections.Count > 256)
            {
                _connections.RemoveAll(t => t.IsCompleted);
            }
        }
    }

    private async Task DrainAsync()
    {
        Task[] outstanding;
        lock (_connectionsGate)
        {
            outstanding = [.. _connections];
        }

        // Bounded: a client stuck mid-request must not stop the process from exiting.
        await Task.WhenAny(
                Task.WhenAll(outstanding),
                Task.Delay(TimeSpan.FromSeconds(5)))
            .ConfigureAwait(false);
    }

    /// <summary>Stops accepting and waits for in-flight connections to finish.</summary>
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
        await DrainAsync().ConfigureAwait(false);
        _listener?.Dispose();
        _listener = null;
        _shutdown.Dispose();
    }
}
