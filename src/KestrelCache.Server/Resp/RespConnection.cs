using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Text;
using KestrelCache.Server.Commands;
using KestrelCache.Server.Observability;

namespace KestrelCache.Server.Resp;

/// <summary>
/// Serves one client connection: read bytes, parse commands, dispatch, write replies.
/// </summary>
/// <remarks>
/// <para>
/// The loop is built on <see cref="System.IO.Pipelines"/> rather than on
/// <see cref="NetworkStream"/> and a byte array, and the reason is the problem that dominates
/// every TCP server: a read returns whatever the network happened to deliver, which may be half
/// a command, three commands, or two and a half. Handling that with a plain stream means
/// managing a growable buffer, remembering how much of it is live, compacting it, and growing it
/// when a command straddles the end — the classic source of off-by-one and buffer-reuse bugs.
/// </para>
/// <para>
/// A <see cref="PipeReader"/> makes that the library's problem. It hands over a
/// <see cref="ReadOnlySequence{T}"/> spanning however many buffers the data arrived in, the
/// parser consumes whole commands and reports what it examined, and anything left over is
/// presented again next time with the new bytes appended. No copying, no manual compaction, and
/// the distinction between <c>consumed</c> and <c>examined</c> is what tells the pipe to wait for
/// more data rather than spinning.
/// </para>
/// <para>
/// Replies are written into a <see cref="PipeWriter"/> and flushed once per read batch rather
/// than once per command. That pipelines naturally: a client that sends fifty commands without
/// waiting gets fifty replies in as few packets as possible, which is most of why
/// <c>redis-benchmark -P 50</c> reports an order of magnitude more throughput than <c>-P 1</c>.
/// </para>
/// </remarks>
internal sealed class RespConnection(
    Socket socket,
    long connectionId,
    KestrelDb database,
    ServerOptions options,
    ServerMetrics metrics)
{
    private readonly List<byte[]> _arguments = [];
    private readonly ConnectionState _state = new()
    {
        Id = connectionId,
        Authenticated = string.IsNullOrEmpty(options.RequirePassword),
    };

    internal async Task ServeAsync(CancellationToken cancellationToken)
    {
        // Nagle's algorithm delays small writes hoping to coalesce them, which for a
        // request-response protocol adds latency to every single reply and coalesces nothing,
        // because the next request cannot arrive until this reply does.
        socket.NoDelay = true;

        await using var stream = new NetworkStream(socket, ownsSocket: false);

        var reader = PipeReader.Create(
            stream,
            new StreamPipeReaderOptions(leaveOpen: true, bufferSize: 16 * 1024));
        var writer = PipeWriter.Create(
            stream,
            new StreamPipeWriterOptions(leaveOpen: true, minimumBufferSize: 16 * 1024));

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                ReadResult read = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                var buffer = read.Buffer;

                if (buffer.Length > 0) metrics.BytesRead(buffer.Length);

                bool dispatchedAny = false;

                while (true)
                {
                    var status = RespParser.TryParse(
                        buffer, RespLimits.Default, _arguments, out var position, out string? error);

                    if (status == ParseStatus.Protocol)
                    {
                        metrics.ProtocolError();
                        RespWriter.WriteError(writer, $"ERR Protocol error: {error}");
                        await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                        return;
                    }

                    if (status == ParseStatus.Incomplete) break;

                    buffer = buffer.Slice(position);

                    if (_arguments.Count > 0)
                    {
                        await DispatchAsync(writer, cancellationToken).ConfigureAwait(false);
                        dispatchedAny = true;

                        if (_state.ShouldClose)
                        {
                            await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                            return;
                        }
                    }
                }

                // consumed = what the parser took; examined = everything it looked at. Reporting
                // the whole buffer as examined is what tells the pipe not to wake us again until
                // genuinely new bytes arrive.
                reader.AdvanceTo(buffer.Start, buffer.End);

                if (dispatchedAny)
                {
                    var flush = await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                    if (flush.IsCompleted) return;
                }

                if (read.IsCompleted)
                {
                    // The client half-closed with an incomplete command still buffered.
                    if (buffer.Length > 0)
                    {
                        metrics.ProtocolError();
                    }
                    return;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Server shutting down.
        }
        catch (IOException)
        {
            // Client vanished mid-request. Routine, not noteworthy.
        }
        catch (SocketException)
        {
            // Likewise.
        }
        finally
        {
            await reader.CompleteAsync().ConfigureAwait(false);
            await writer.CompleteAsync().ConfigureAwait(false);
        }
    }

    private async ValueTask DispatchAsync(PipeWriter writer, CancellationToken cancellationToken)
    {
        long started = Stopwatch.GetTimestamp();

        try
        {
            string name = Encoding.UTF8.GetString(_arguments[0]).ToUpperInvariant();
            var spec = CommandTable.Find(name);

            if (spec is null)
            {
                metrics.CommandError();
                RespWriter.WriteError(
                    writer,
                    $"ERR unknown command '{name.ToLowerInvariant()}', with "
                        + $"{_arguments.Count - 1} arg(s)");
                return;
            }

            if (!spec.ArityMatches(_arguments.Count))
            {
                metrics.CommandError();
                RespWriter.WriteError(
                    writer,
                    $"ERR wrong number of arguments for '{name.ToLowerInvariant()}' command");
                return;
            }

            if (spec.RequiresAuth && !_state.Authenticated)
            {
                metrics.CommandError();
                RespWriter.WriteError(writer, "NOAUTH Authentication required.");
                return;
            }

            var context = new CommandContext(
                _arguments, writer, database, options, metrics, _state, cancellationToken);

            await spec.Handler(context).ConfigureAwait(false);
        }
        catch (KestrelCacheException exception)
        {
            // An engine-level failure is the client's business: it means the write did not
            // happen. Reporting it is far better than closing the connection and leaving them to
            // guess.
            metrics.CommandError();
            RespWriter.WriteError(writer, $"ERR {exception.Message.ReplaceLineEndings(" ")}");
        }
        catch (ArgumentException exception)
        {
            metrics.CommandError();
            RespWriter.WriteError(writer, $"ERR {exception.Message.ReplaceLineEndings(" ")}");
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception exception)
        {
            // Anything unexpected is a bug, but it must not take the server down with it. Report
            // it to this one client and keep serving the others.
            metrics.CommandError();
            RespWriter.WriteError(
                writer,
                $"ERR internal error: {exception.GetType().Name}: "
                    + exception.Message.ReplaceLineEndings(" "));
        }
        finally
        {
            metrics.CommandCompleted(Stopwatch.GetTimestamp() - started);
        }
    }
}
