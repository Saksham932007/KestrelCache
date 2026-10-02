using System.Text;
using KestrelCache.Server.Resp;

namespace KestrelCache.Server.Commands;

/// <summary>One command's handler and its arity rules.</summary>
/// <param name="Name">Upper-case command name.</param>
/// <param name="Arity">
/// Argument count including the name. A negative value means "at least this many", which is the
/// same convention Redis's own COMMAND output uses.
/// </param>
/// <param name="IsWrite">Whether the command mutates the keyspace.</param>
/// <param name="RequiresAuth">Whether AUTH must have succeeded first.</param>
/// <param name="Handler">Writes the reply.</param>
internal sealed record CommandSpec(
    string Name,
    int Arity,
    bool IsWrite,
    bool RequiresAuth,
    Func<CommandContext, ValueTask> Handler)
{
    /// <summary>Checks a command's argument count against its arity rule.</summary>
    internal bool ArityMatches(int argumentCount) =>
        Arity >= 0 ? argumentCount == Arity : argumentCount >= -Arity;
}

/// <summary>
/// The command table: what the server understands, and how each command maps onto the engine.
/// </summary>
/// <remarks>
/// <para>
/// The set is deliberately the subset of Redis that a key-value store can honestly implement,
/// plus the handful of handshake commands real clients insist on. <c>COMMAND DOCS</c>,
/// <c>CLIENT SETINFO</c> and <c>HELLO</c> are not interesting in themselves, but
/// <c>redis-cli</c> sends them on connect and refuses to proceed without an answer — so
/// omitting them would mean the compatibility claim fails at the first thing anyone tries.
/// </para>
/// <para>
/// What is deliberately absent is as telling as what is here. There are no lists, sets, hashes
/// or sorted sets, because the engine stores opaque byte strings and implementing those on top
/// would mean read-modify-write of a serialised blob per element — which works, and is slower
/// and less honest than saying it is not supported. There is no <c>EXPIRE</c>, because nothing
/// in the storage format carries a timestamp and bolting one on would need a new record field
/// and an expiry sweep.
/// </para>
/// </remarks>
internal static class CommandTable
{
    private static readonly Dictionary<string, CommandSpec> Commands = Build();

    /// <summary>Looks a command up by name, case-insensitively.</summary>
    internal static CommandSpec? Find(string name) => Commands.GetValueOrDefault(name);

    /// <summary>Every known command.</summary>
    internal static IReadOnlyCollection<CommandSpec> All => Commands.Values;

    private static Dictionary<string, CommandSpec> Build()
    {
        var table = new Dictionary<string, CommandSpec>(StringComparer.OrdinalIgnoreCase);

        void Add(
            string name,
            int arity,
            Func<CommandContext, ValueTask> handler,
            bool isWrite = false,
            bool requiresAuth = true) =>
            table[name] = new CommandSpec(name, arity, isWrite, requiresAuth, handler);

        // ---- connection and handshake
        Add("PING", -1, KeyspaceCommands.PingAsync, requiresAuth: false);
        Add("ECHO", 2, KeyspaceCommands.EchoAsync, requiresAuth: false);
        Add("QUIT", 1, ServerCommands.QuitAsync, requiresAuth: false);
        Add("AUTH", -2, ServerCommands.AuthAsync, requiresAuth: false);
        Add("HELLO", -1, ServerCommands.HelloAsync, requiresAuth: false);
        Add("SELECT", 2, ServerCommands.SelectAsync);
        Add("CLIENT", -2, ServerCommands.ClientAsync, requiresAuth: false);
        Add("COMMAND", -1, ServerCommands.CommandAsync, requiresAuth: false);
        Add("CONFIG", -2, ServerCommands.ConfigAsync);

        // ---- strings
        Add("GET", 2, KeyspaceCommands.GetAsync);
        Add("SET", -3, KeyspaceCommands.SetAsync, isWrite: true);
        Add("SETNX", 3, KeyspaceCommands.SetNxAsync, isWrite: true);
        Add("GETSET", 3, KeyspaceCommands.GetSetAsync, isWrite: true);
        Add("APPEND", 3, KeyspaceCommands.AppendAsync, isWrite: true);
        Add("STRLEN", 2, KeyspaceCommands.StrLenAsync);
        Add("MGET", -2, KeyspaceCommands.MGetAsync);
        Add("MSET", -3, KeyspaceCommands.MSetAsync, isWrite: true);
        Add("DEL", -2, KeyspaceCommands.DelAsync, isWrite: true);
        Add("UNLINK", -2, KeyspaceCommands.DelAsync, isWrite: true);
        Add("EXISTS", -2, KeyspaceCommands.ExistsAsync);
        Add("TYPE", 2, KeyspaceCommands.TypeAsync);

        // ---- counters
        Add("INCR", 2, KeyspaceCommands.IncrAsync, isWrite: true);
        Add("DECR", 2, KeyspaceCommands.DecrAsync, isWrite: true);
        Add("INCRBY", 3, KeyspaceCommands.IncrByAsync, isWrite: true);
        Add("DECRBY", 3, KeyspaceCommands.DecrByAsync, isWrite: true);

        // ---- iteration (LSM engine only)
        Add("SCAN", -2, KeyspaceCommands.ScanAsync);
        Add("KEYS", 2, KeyspaceCommands.KeysAsync);

        // ---- administration
        Add("DBSIZE", -1, ServerCommands.DbSizeAsync);
        Add("INFO", -1, ServerCommands.InfoAsync);
        Add("FLUSHDB", -1, ServerCommands.FlushDbAsync, isWrite: true);
        Add("FLUSHALL", -1, ServerCommands.FlushDbAsync, isWrite: true);
        Add("COMPACT", 1, ServerCommands.CompactAsync, isWrite: true);
        Add("BGSAVE", -1, ServerCommands.SaveAsync, isWrite: true);
        Add("CLUSTER", -1, ServerCommands.ClusterAsync, requiresAuth: false);
        Add("SAVE", 1, ServerCommands.SaveAsync, isWrite: true);

        return table;
    }

    /// <summary>Writes the standard wrong-arity error.</summary>
    internal static void WriteWrongArity(CommandContext context, string name) =>
        RespWriter.WriteError(
            context.Output, $"ERR wrong number of arguments for '{name.ToLowerInvariant()}' command");

    /// <summary>Decodes an argument as a base-10 integer, Redis-style.</summary>
    internal static bool TryParseInteger(byte[] argument, out long value) =>
        long.TryParse(Encoding.UTF8.GetString(argument), out value);
}
