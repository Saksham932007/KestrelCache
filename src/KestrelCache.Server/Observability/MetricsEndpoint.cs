using System.Net;
using System.Net.Sockets;
using System.Text;

namespace KestrelCache.Server.Observability;

/// <summary>
/// A minimal HTTP/1.1 endpoint serving <c>/metrics</c>, <c>/health</c> and <c>/stats</c>.
/// </summary>
/// <remarks>
/// <para>
/// Hand-rolled rather than hosted on ASP.NET Core, because what is needed is three fixed routes
/// returning text to a scraper. Pulling in the web framework for that would add a large
/// dependency surface and a second concurrency model to a project whose point is the storage
/// engine underneath. The HTTP subset implemented here — request line, discard headers, write a
/// response — is about eighty lines and has no behaviour worth configuring.
/// </para>
/// <para>
/// Pull rather than push: Prometheus scrapes this on an interval rather than being sent samples.
/// That is what makes <c>kestrelcache_up</c> meaningful — a scrape that fails is itself the
/// signal that the process is gone, which a push-based exporter cannot report about itself.
/// </para>
/// </remarks>
internal sealed class MetricsEndpoint(
    int port,
    ServerMetrics metrics,
    KestrelDb database,
    ServerOptions options) : IAsyncDisposable
{
    private readonly CancellationTokenSource _shutdown = new();
    private Socket? _listener;
    private int _disposed;

    /// <summary>The port actually bound.</summary>
    internal int BoundPort { get; private set; }

    internal void Bind()
    {
        (_listener, int bound) = ListenerFactory.Listen("*", port, backlog: 32);
        BoundPort = bound;
    }

    internal async Task RunAsync(CancellationToken cancellationToken)
    {
        if (_listener is null) Bind();

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken, _shutdown.Token);

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
                continue;
            }

            _ = Task.Run(() => ServeAsync(client), CancellationToken.None);
        }

        _listener?.Dispose();
        _listener = null;
    }

    private async Task ServeAsync(Socket client)
    {
        try
        {
            using (client)
            await using (var stream = new NetworkStream(client, ownsSocket: false))
            {
                using var reader = new StreamReader(stream, Encoding.ASCII, false, 1024, leaveOpen: true);

                string? requestLine = await reader.ReadLineAsync().ConfigureAwait(false);
                if (string.IsNullOrEmpty(requestLine)) return;

                // Headers are read and discarded: nothing here varies by header, and leaving
                // them unread would make the client see a truncated exchange.
                while (await reader.ReadLineAsync().ConfigureAwait(false) is { Length: > 0 })
                {
                }

                string[] parts = requestLine.Split(' ');
                string path = parts.Length > 1 ? parts[1] : "/";
                int query = path.IndexOf('?', StringComparison.Ordinal);
                if (query >= 0) path = path[..query];

                var (status, contentType, body) = Route(path);
                await WriteResponseAsync(stream, status, contentType, body).ConfigureAwait(false);
            }
        }
        catch (IOException)
        {
            // Scraper hung up.
        }
        catch (SocketException)
        {
            // Likewise.
        }
    }

    private (string Status, string ContentType, string Body) Route(string path)
    {
        switch (path)
        {
            case "/metrics":
                return (
                    "200 OK",
                    "text/plain; version=0.0.4; charset=utf-8",
                    metrics.Render(database.GetStats(), options, RaftStatsOrNull()));

            case "/health" or "/healthz":
                // A liveness probe must not merely report that the process is running -- it has
                // to touch the thing that could be broken. Reading the engine's counters proves
                // the database object is alive and answering.
                try
                {
                    var stats = database.GetStats();
                    return ("200 OK", "text/plain; charset=utf-8", $"ok engine={stats.Engine}\n");
                }
                catch (Exception exception)
                {
                    return ("503 Service Unavailable", "text/plain; charset=utf-8",
                        $"unhealthy: {exception.Message}\n");
                }

            case "/stats":
            {
                var stats = database.GetStats();
                var text = new StringBuilder();
                text.Append("engine            ").Append(stats.Engine).Append('\n');
                text.Append("keys              ").Append(stats.KeyCount)
                    .Append(stats.KeyCountIsExact ? " (exact)" : " (upper bound)").Append('\n');
                text.Append("disk bytes        ").Append(stats.DiskSizeBytes).Append('\n');
                text.Append("  data files      ").Append(stats.DataFileBytes).Append('\n');
                text.Append("  wal             ").Append(stats.WriteAheadLogBytes).Append('\n');
                text.Append("stale ratio       ")
                    .Append(stats.KeyCountIsExact ? stats.StaleRatio.ToString("P1") : "n/a (needs a merge)")
                    .Append('\n');
                text.Append("reads             ").Append(stats.Reads).Append('\n');
                text.Append("writes            ").Append(stats.Writes).Append('\n');
                text.Append("deletes           ").Append(stats.Deletes).Append('\n');
                text.Append("fsyncs            ").Append(stats.Syncs).Append('\n');
                text.Append("compactions       ").Append(stats.Compactions).Append('\n');
                text.Append("commands served   ").Append(metrics.CommandsTotal).Append('\n');
                text.Append("connections       ").Append(metrics.ActiveConnections).Append('\n');
                if (stats.SsTablesPerLevel.Count > 0)
                {
                    text.Append("sstables/level    [")
                        .Append(string.Join(", ", stats.SsTablesPerLevel)).Append("]\n");
                    text.Append("block cache hits  ").Append(stats.BlockCacheHitRate.ToString("P1"))
                        .Append('\n');
                }

                if (RaftStatsOrNull() is { } raft)
                {
                    text.Append('\n');
                    text.Append("raft role         ").Append(raft.Role).Append('\n');
                    text.Append("raft term         ").Append(raft.Term).Append('\n');
                    text.Append("raft leader       ").Append(raft.LeaderId ?? "none").Append('\n');
                    text.Append("raft voters       ")
                        .Append(raft.Configuration?.ToString() ?? "?").Append('\n');
                    text.Append("raft log          [").Append(raft.FirstLogIndex).Append("..")
                        .Append(raft.LastLogIndex).Append("]\n");
                    text.Append("raft committed    ").Append(raft.CommitIndex).Append('\n');
                    text.Append("raft snapshot     ")
                        .Append(raft.SnapshotIndex > 0
                            ? $"index {raft.SnapshotIndex}, {raft.SnapshotSizeBytes} bytes"
                            : "none")
                        .Append('\n');
                }

                return ("200 OK", "text/plain; charset=utf-8", text.ToString());
            }

            case "/":
                return ("200 OK", "text/plain; charset=utf-8",
                    "KestrelCache\n\n  /metrics  Prometheus exposition\n"
                        + "  /health   liveness probe\n  /stats    human-readable counters\n");

            default:
                return ("404 Not Found", "text/plain; charset=utf-8", "not found\n");
        }
    }

    /// <summary>Consensus counters when this node is clustered, else null.</summary>
    private KestrelCache.Raft.RaftStats? RaftStatsOrNull() =>
        database.Engine is KestrelCache.Raft.ReplicatedEngine replicated
            ? replicated.GetRaftStats()
            : null;

    private static async Task WriteResponseAsync(
        Stream stream,
        string status,
        string contentType,
        string body)
    {
        byte[] payload = Encoding.UTF8.GetBytes(body);

        var header = new StringBuilder();
        header.Append("HTTP/1.1 ").Append(status).Append("\r\n");
        header.Append("Content-Type: ").Append(contentType).Append("\r\n");
        header.Append("Content-Length: ").Append(payload.Length).Append("\r\n");
        header.Append("Connection: close\r\n");
        header.Append("\r\n");

        await stream.WriteAsync(Encoding.ASCII.GetBytes(header.ToString())).ConfigureAwait(false);
        await stream.WriteAsync(payload).ConfigureAwait(false);
        await stream.FlushAsync().ConfigureAwait(false);
    }

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
