using System.IO.Pipelines;
using System.Text;
using KestrelCache.Server.Observability;

namespace KestrelCache.Server.Commands;

/// <summary>Per-connection state a command handler may read or change.</summary>
internal sealed class ConnectionState
{
    /// <summary>Monotonic connection id, as reported by <c>CLIENT ID</c>.</summary>
    internal long Id { get; init; }

    /// <summary>Name set by <c>CLIENT SETNAME</c>.</summary>
    internal string Name { get; set; } = string.Empty;

    /// <summary>True once <c>AUTH</c> has succeeded, or if no password is configured.</summary>
    internal bool Authenticated { get; set; }

    /// <summary>RESP version negotiated by <c>HELLO</c>. 2 unless a client asks for 3.</summary>
    internal int ProtocolVersion { get; set; } = 2;

    /// <summary>Set by a handler to ask the connection loop to close after replying.</summary>
    internal bool ShouldClose { get; set; }
}

/// <summary>Everything one command invocation needs.</summary>
internal readonly struct CommandContext(
    IReadOnlyList<byte[]> arguments,
    PipeWriter output,
    KestrelDb database,
    ServerOptions options,
    ServerMetrics metrics,
    ConnectionState connection,
    CancellationToken cancellationToken)
{
    /// <summary>The command name followed by its arguments.</summary>
    internal IReadOnlyList<byte[]> Arguments { get; } = arguments;

    /// <summary>Where the reply is written.</summary>
    internal PipeWriter Output { get; } = output;

    /// <summary>The database being served.</summary>
    internal KestrelDb Database { get; } = database;

    /// <summary>Server configuration.</summary>
    internal ServerOptions Options { get; } = options;

    /// <summary>Server counters.</summary>
    internal ServerMetrics Metrics { get; } = metrics;

    /// <summary>State for the connection that issued this command.</summary>
    internal ConnectionState Connection { get; } = connection;

    /// <summary>Cancelled when the server is shutting down or the client has gone.</summary>
    internal CancellationToken CancellationToken { get; } = cancellationToken;

    /// <summary>Arguments excluding the command name.</summary>
    internal int ArgumentCount => Arguments.Count - 1;

    /// <summary>The nth argument after the command name, 1-based.</summary>
    internal byte[] Argument(int index) => Arguments[index];

    /// <summary>The nth argument decoded as UTF-8.</summary>
    internal string Text(int index) => Encoding.UTF8.GetString(Arguments[index]);

    /// <summary>The nth argument upper-cased, for sub-commands.</summary>
    internal string Keyword(int index) => Text(index).ToUpperInvariant();
}
