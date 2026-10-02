using System.Diagnostics;
using System.Text;
using KestrelCache.Server.Resp;

namespace KestrelCache.Server.Commands;

/// <summary>Connection handshake and administrative commands.</summary>
internal static class ServerCommands
{
    private static readonly DateTimeOffset StartedAt = DateTimeOffset.UtcNow;

    internal static ValueTask QuitAsync(CommandContext context)
    {
        RespWriter.WriteOk(context.Output);
        context.Connection.ShouldClose = true;
        return ValueTask.CompletedTask;
    }

    internal static ValueTask AuthAsync(CommandContext context)
    {
        if (string.IsNullOrEmpty(context.Options.RequirePassword))
        {
            RespWriter.WriteError(
                context.Output,
                "ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> "
                    + "<password>?");
            return ValueTask.CompletedTask;
        }

        // AUTH takes either a bare password or a username and password; only the password is
        // checked, since there is a single configured credential.
        string supplied = context.ArgumentCount >= 2 ? context.Text(2) : context.Text(1);

        if (FixedTimeEquals(supplied, context.Options.RequirePassword))
        {
            context.Connection.Authenticated = true;
            RespWriter.WriteOk(context.Output);
        }
        else
        {
            RespWriter.WriteError(context.Output, "WRONGPASS invalid username-password pair");
        }

        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Compares two secrets without leaking which byte differed through timing.
    /// </summary>
    /// <remarks>
    /// An ordinary string comparison returns as soon as it finds a mismatch, so the time it takes
    /// reveals how many leading characters were right — enough, over many attempts, to recover a
    /// password a character at a time. Comparing every byte regardless costs nothing here and
    /// removes the signal.
    /// </remarks>
    private static bool FixedTimeEquals(string supplied, string expected) =>
        System.Security.Cryptography.CryptographicOperations.FixedTimeEquals(
            Encoding.UTF8.GetBytes(supplied),
            Encoding.UTF8.GetBytes(expected));

    /// <summary>
    /// <c>HELLO [protover]</c>, the RESP3 handshake.
    /// </summary>
    /// <remarks>
    /// Modern <c>redis-cli</c> sends this on connect. The reply is deliberately a RESP2 array
    /// even when the client asks for protocol 3, because this server does not implement RESP3's
    /// distinct map, set and double types — and claiming version 3 while speaking version 2
    /// would break clients worse than declining does.
    /// </remarks>
    internal static ValueTask HelloAsync(CommandContext context)
    {
        if (context.ArgumentCount >= 1)
        {
            if (!CommandTable.TryParseInteger(context.Argument(1), out long version)
                || version is < 2 or > 3)
            {
                RespWriter.WriteError(
                    context.Output, "NOPROTO unsupported protocol version");
                return ValueTask.CompletedTask;
            }

            // Answer honestly: 2, whatever was asked for.
            context.Connection.ProtocolVersion = 2;
        }

        RespWriter.WriteArrayHeader(context.Output, 14);
        RespWriter.WriteBulkString(context.Output, "server");
        RespWriter.WriteBulkString(context.Output, "kestrelcache");
        RespWriter.WriteBulkString(context.Output, "version");
        RespWriter.WriteBulkString(context.Output, Version);
        RespWriter.WriteBulkString(context.Output, "proto");
        RespWriter.WriteInteger(context.Output, 2);
        RespWriter.WriteBulkString(context.Output, "id");
        RespWriter.WriteInteger(context.Output, context.Connection.Id);
        RespWriter.WriteBulkString(context.Output, "mode");
        RespWriter.WriteBulkString(context.Output, "standalone");
        RespWriter.WriteBulkString(context.Output, "role");
        RespWriter.WriteBulkString(context.Output, "master");
        RespWriter.WriteBulkString(context.Output, "modules");
        RespWriter.WriteArrayHeader(context.Output, 0);

        return ValueTask.CompletedTask;
    }

    internal static ValueTask SelectAsync(CommandContext context)
    {
        // There is one keyspace, so SELECT 0 succeeds and anything else does not. Accepting
        // SELECT 5 silently would let a client believe it had switched databases.
        if (CommandTable.TryParseInteger(context.Argument(1), out long index) && index == 0)
        {
            RespWriter.WriteOk(context.Output);
        }
        else
        {
            RespWriter.WriteError(context.Output, "ERR DB index is out of range");
        }

        return ValueTask.CompletedTask;
    }

    internal static ValueTask ClientAsync(CommandContext context)
    {
        switch (context.Keyword(1))
        {
            case "ID":
                RespWriter.WriteInteger(context.Output, context.Connection.Id);
                break;

            case "SETNAME" when context.ArgumentCount >= 2:
                context.Connection.Name = context.Text(2);
                RespWriter.WriteOk(context.Output);
                break;

            case "GETNAME":
                if (context.Connection.Name.Length == 0) RespWriter.WriteNull(context.Output);
                else RespWriter.WriteBulkString(context.Output, context.Connection.Name);
                break;

            // redis-cli 7 announces its library name and version on connect and treats an error
            // as fatal, so these have to be accepted even though nothing is done with them.
            case "SETINFO":
                RespWriter.WriteOk(context.Output);
                break;

            case "INFO":
                RespWriter.WriteBulkString(
                    context.Output,
                    $"id={context.Connection.Id} name={context.Connection.Name} "
                        + $"resp={context.Connection.ProtocolVersion}");
                break;

            default:
                RespWriter.WriteError(
                    context.Output,
                    $"ERR Unknown CLIENT subcommand '{context.Text(1)}'");
                break;
        }

        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// <c>COMMAND</c> and its subcommands, enough for clients that probe capabilities on connect.
    /// </summary>
    internal static ValueTask CommandAsync(CommandContext context)
    {
        if (context.ArgumentCount == 0)
        {
            WriteCommandList(context);
            return ValueTask.CompletedTask;
        }

        switch (context.Keyword(1))
        {
            case "COUNT":
                RespWriter.WriteInteger(context.Output, CommandTable.All.Count);
                break;

            // redis-cli asks for DOCS on connect. An empty map is a valid answer and keeps the
            // client from deciding the server is broken.
            case "DOCS":
                RespWriter.WriteArrayHeader(context.Output, 0);
                break;

            case "INFO":
                if (context.ArgumentCount == 1)
                {
                    WriteCommandList(context);
                    break;
                }

                RespWriter.WriteArrayHeader(context.Output, context.ArgumentCount - 1);
                for (int i = 2; i < context.Arguments.Count; i++)
                {
                    var spec = CommandTable.Find(context.Text(i));
                    if (spec is null) RespWriter.WriteNull(context.Output);
                    else WriteCommandSpec(context, spec);
                }
                break;

            default:
                RespWriter.WriteArrayHeader(context.Output, 0);
                break;
        }

        return ValueTask.CompletedTask;
    }

    private static void WriteCommandList(CommandContext context)
    {
        RespWriter.WriteArrayHeader(context.Output, CommandTable.All.Count);
        foreach (var spec in CommandTable.All)
        {
            WriteCommandSpec(context, spec);
        }
    }

    private static void WriteCommandSpec(CommandContext context, CommandSpec spec)
    {
        // The shape Redis uses: name, arity, flags, first key, last key, key step.
        RespWriter.WriteArrayHeader(context.Output, 6);
        RespWriter.WriteBulkString(context.Output, spec.Name.ToLowerInvariant());
        RespWriter.WriteInteger(context.Output, spec.Arity);
        RespWriter.WriteArrayHeader(context.Output, 1);
        RespWriter.WriteSimpleString(context.Output, spec.IsWrite ? "write" : "readonly");
        RespWriter.WriteInteger(context.Output, spec.Arity == 1 ? 0 : 1);
        RespWriter.WriteInteger(context.Output, spec.Arity == 1 ? 0 : 1);
        RespWriter.WriteInteger(context.Output, spec.Arity == 1 ? 0 : 1);
    }

    internal static ValueTask ConfigAsync(CommandContext context)
    {
        switch (context.Keyword(1))
        {
            case "GET":
            {
                // redis-benchmark issues CONFIG GET save and CONFIG GET appendonly before
                // starting. Returning an empty map is accepted and means "not configured".
                var pairs = new List<(string Key, string Value)>();
                string pattern = context.ArgumentCount >= 2 ? context.Text(2) : "*";

                foreach (var (key, value) in ConfigurationOf(context.Options))
                {
                    if (GlobMatcher.Matches(pattern, key)) pairs.Add((key, value));
                }

                RespWriter.WriteArrayHeader(context.Output, pairs.Count * 2);
                foreach (var (key, value) in pairs)
                {
                    RespWriter.WriteBulkString(context.Output, key);
                    RespWriter.WriteBulkString(context.Output, value);
                }
                break;
            }

            case "SET":
                RespWriter.WriteError(
                    context.Output,
                    "ERR CONFIG SET is not supported; configuration is fixed at startup");
                break;

            case "RESETSTAT":
                RespWriter.WriteOk(context.Output);
                break;

            default:
                RespWriter.WriteError(
                    context.Output, $"ERR Unknown CONFIG subcommand '{context.Text(1)}'");
                break;
        }

        return ValueTask.CompletedTask;
    }

    private static IEnumerable<(string Key, string Value)> ConfigurationOf(ServerOptions options)
    {
        yield return ("maxmemory", "0");
        yield return ("maxmemory-policy", "noeviction");
        yield return ("appendonly", "yes");
        yield return ("save", string.Empty);
        yield return ("databases", "1");
        yield return ("engine", options.Engine.ToString().ToLowerInvariant());
        yield return ("sync-policy", options.SyncPolicy.ToString().ToLowerInvariant());
        yield return ("dir", options.DataPath);
    }

    /// <summary>
    /// <c>DBSIZE</c>, and <c>DBSIZE EXACT</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Redis answers DBSIZE in constant time from its hash table's element count. An LSM tree
    /// cannot: it holds every version of every key spread across a memtable and many immutable
    /// files, and it has no way to know how many of those are superseded without merging them.
    /// The number it can produce cheaply is an upper bound that counts stale versions and
    /// tombstones, converging on the truth as compaction proceeds.
    /// </para>
    /// <para>
    /// So the default reply is that estimate, because tools call DBSIZE freely and expect an
    /// integer — RocksDB takes the same line, exposing its key count as
    /// <c>estimate-num-keys</c>. <c>DBSIZE EXACT</c> is available for when the precise figure is
    /// actually wanted, and it scans, which is why it is opt-in: making the default O(keys) would
    /// turn a routine monitoring call into a way to stall the server. <c>INFO engine</c> reports
    /// <c>keys_exact</c> so a caller can tell which kind of answer it got.
    /// </para>
    /// </remarks>
    internal static async ValueTask DbSizeAsync(CommandContext context)
    {
        var stats = context.Database.GetStats();

        bool exactRequested = context.ArgumentCount >= 1
            && context.Keyword(1) is "EXACT";

        if (!exactRequested || stats.KeyCountIsExact)
        {
            RespWriter.WriteInteger(context.Output, stats.KeyCount);
            return;
        }

        if (context.Database.Engine is not IScannableStorageEngine scannable)
        {
            RespWriter.WriteError(
                context.Output,
                $"ERR DBSIZE EXACT needs ordered iteration, which the {stats.Engine} engine "
                    + "cannot provide");
            return;
        }

        long exact = 0;
        await foreach (var _ in scannable
            .ScanAsync(cancellationToken: context.CancellationToken)
            .ConfigureAwait(false))
        {
            exact++;
        }

        RespWriter.WriteInteger(context.Output, exact);
    }

    /// <summary>The <c>INFO</c> report, in the sectioned <c>field:value</c> format clients parse.</summary>
    internal static ValueTask InfoAsync(CommandContext context)
    {
        var stats = context.Database.GetStats();
        var text = new StringBuilder(2048);

        string? section = context.ArgumentCount >= 1 ? context.Keyword(1) : null;
        bool Wanted(string name) =>
            section is null || section == "ALL" || section == "EVERYTHING"
            || section.Equals(name, StringComparison.OrdinalIgnoreCase);

        if (Wanted("server"))
        {
            text.Append("# Server\r\n");
            text.Append("kestrelcache_version:").Append(Version).Append("\r\n");
            text.Append("redis_version:7.0.0-kestrelcache\r\n"); // clients gate features on this
            text.Append("mode:standalone\r\n");
            text.Append("os:").Append(Environment.OSVersion).Append("\r\n");
            text.Append("process_id:").Append(Environment.ProcessId).Append("\r\n");
            text.Append("runtime:").Append(Environment.Version).Append("\r\n");
            text.Append("uptime_in_seconds:")
                .Append((long)(DateTimeOffset.UtcNow - StartedAt).TotalSeconds).Append("\r\n");
            text.Append("tcp_port:").Append(context.Options.Port).Append("\r\n");
            text.Append("\r\n");
        }

        if (Wanted("clients"))
        {
            text.Append("# Clients\r\n");
            text.Append("connected_clients:").Append(context.Metrics.ActiveConnections).Append("\r\n");
            text.Append("\r\n");
        }

        if (Wanted("memory"))
        {
            text.Append("# Memory\r\n");
            text.Append("used_memory:").Append(GC.GetTotalMemory(false)).Append("\r\n");
            text.Append("used_memory_rss:").Append(Environment.WorkingSet).Append("\r\n");
            text.Append("maxmemory:0\r\n");
            text.Append("maxmemory_policy:noeviction\r\n");
            text.Append("\r\n");
        }

        if (Wanted("stats"))
        {
            text.Append("# Stats\r\n");
            text.Append("total_commands_processed:").Append(context.Metrics.CommandsTotal).Append("\r\n");
            text.Append("keyspace_hits:").Append(stats.Reads).Append("\r\n");
            text.Append("\r\n");
        }

        if (Wanted("persistence"))
        {
            text.Append("# Persistence\r\n");
            text.Append("loading:0\r\n");
            text.Append("aof_enabled:1\r\n");
            text.Append("rdb_bgsave_in_progress:0\r\n");
            text.Append("\r\n");
        }

        if (Wanted("engine"))
        {
            text.Append("# Engine\r\n");
            text.Append("engine:").Append(stats.Engine).Append("\r\n");
            text.Append("sync_policy:").Append(context.Options.SyncPolicy).Append("\r\n");
            text.Append("keys:").Append(stats.KeyCount).Append("\r\n");
            text.Append("keys_exact:").Append(stats.KeyCountIsExact ? 1 : 0).Append("\r\n");
            text.Append("disk_bytes:").Append(stats.DiskSizeBytes).Append("\r\n");
            text.Append("data_file_bytes:").Append(stats.DataFileBytes).Append("\r\n");
            text.Append("wal_bytes:").Append(stats.WriteAheadLogBytes).Append("\r\n");
            text.Append("live_bytes:").Append(stats.LiveDataBytes).Append("\r\n");
            text.Append("stale_ratio:").Append(stats.StaleRatio.ToString("F4")).Append("\r\n");
            text.Append("stale_ratio_measurable:").Append(stats.KeyCountIsExact ? 1 : 0).Append("\r\n");
            text.Append("engine_reads:").Append(stats.Reads).Append("\r\n");
            text.Append("engine_writes:").Append(stats.Writes).Append("\r\n");
            text.Append("engine_deletes:").Append(stats.Deletes).Append("\r\n");
            text.Append("fsyncs:").Append(stats.Syncs).Append("\r\n");
            text.Append("compactions:").Append(stats.Compactions).Append("\r\n");
            text.Append("compaction_bytes_written:").Append(stats.CompactionBytesWritten).Append("\r\n");

            if (stats.SsTablesPerLevel.Count > 0)
            {
                text.Append("sstables_per_level:")
                    .Append(string.Join(',', stats.SsTablesPerLevel)).Append("\r\n");
                text.Append("bloom_negatives:").Append(stats.BloomFilterNegatives).Append("\r\n");
                text.Append("bloom_false_positives:").Append(stats.BloomFilterFalsePositives).Append("\r\n");
                text.Append("block_cache_hit_rate:")
                    .Append(stats.BlockCacheHitRate.ToString("F4")).Append("\r\n");
            }

            text.Append("\r\n");
        }

        if (Wanted("replication") && context.Database.Engine is Raft.ReplicatedEngine replicated)
        {
            var raft = replicated.GetRaftStats();
            text.Append("# Replication\r\n");
            text.Append("role:").Append(raft.Role == Raft.RaftRole.Leader ? "master" : "slave")
                .Append("\r\n");
            text.Append("consensus:raft\r\n");
            text.Append("node_id:").Append(raft.NodeId).Append("\r\n");
            text.Append("raft_role:").Append(raft.Role.ToString().ToLowerInvariant()).Append("\r\n");
            text.Append("raft_term:").Append(raft.Term).Append("\r\n");
            text.Append("raft_leader:").Append(raft.LeaderId ?? "none").Append("\r\n");
            text.Append("raft_log_index:").Append(raft.LastLogIndex).Append("\r\n");
            text.Append("raft_commit_index:").Append(raft.CommitIndex).Append("\r\n");
            text.Append("raft_last_applied:").Append(raft.LastApplied).Append("\r\n");
            text.Append("\r\n");
        }

        RespWriter.WriteBulkString(context.Output, text.ToString());
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// <c>FLUSHDB</c>, implemented as a scan-and-delete.
    /// </summary>
    /// <remarks>
    /// Redis drops its hash table and is done. Here every key has to be tombstoned, because
    /// there is no "forget everything" operation that recovery would honour — a crash halfway
    /// through must leave a coherent database, and only writing deletions achieves that. The
    /// space comes back on the next compaction, not immediately.
    /// </remarks>
    internal static async ValueTask FlushDbAsync(CommandContext context)
    {
        if (context.Database.Engine is not IScannableStorageEngine scannable)
        {
            RespWriter.WriteError(
                context.Output,
                $"ERR FLUSHDB needs ordered iteration, which the {context.Database.Engine.Name} "
                    + "engine cannot provide");
            return;
        }

        var batch = new WriteBatch();
        long deleted = 0;

        await foreach (var (key, _) in scannable
            .ScanAsync(cancellationToken: context.CancellationToken)
            .ConfigureAwait(false))
        {
            batch.Delete(key);
            deleted++;

            if (batch.Count >= 1_000)
            {
                await context.Database.WriteAsync(batch, context.CancellationToken).ConfigureAwait(false);
                batch.Clear();
            }
        }

        if (batch.Count > 0)
        {
            await context.Database.WriteAsync(batch, context.CancellationToken).ConfigureAwait(false);
        }

        RespWriter.WriteOk(context.Output);
    }

    internal static async ValueTask CompactAsync(CommandContext context)
    {
        var before = context.Database.GetStats();
        var stopwatch = Stopwatch.StartNew();

        await context.Database.CompactAsync(context.CancellationToken).ConfigureAwait(false);

        stopwatch.Stop();
        var after = context.Database.GetStats();

        RespWriter.WriteSimpleString(
            context.Output,
            $"compacted in {stopwatch.ElapsedMilliseconds} ms: "
                + $"{before.DiskSizeBytes} -> {after.DiskSizeBytes} bytes");
    }

    internal static async ValueTask SaveAsync(CommandContext context)
    {
        // There is no snapshot file to write; the log is already the durable record. The useful
        // interpretation of SAVE here is "make everything acknowledged durable now".
        await context.Database.FlushAsync(context.CancellationToken).ConfigureAwait(false);
        RespWriter.WriteOk(context.Output);
    }

    /// <summary>
    /// <c>CLUSTER INFO</c> and <c>CLUSTER NODES</c>, reporting this node's consensus state.
    /// </summary>
    /// <remarks>
    /// Reuses Redis's command name and field shape where it fits, because operators and tooling
    /// already know them — but the fields describe Raft, not Redis Cluster's hash slots, and
    /// saying so plainly is better than inventing a resemblance that does not hold.
    /// </remarks>
    internal static ValueTask ClusterAsync(CommandContext context)
    {
        if (context.Database.Engine is not Raft.ReplicatedEngine replicated)
        {
            if (context.ArgumentCount >= 1 && context.Keyword(1) == "INFO")
            {
                RespWriter.WriteBulkString(
                    context.Output,
                    "cluster_enabled:0\r\ncluster_state:ok\r\ncluster_known_nodes:1\r\n");
                return ValueTask.CompletedTask;
            }

            RespWriter.WriteError(
                context.Output,
                "ERR This instance is not replicated; start it with --raft-id and --raft-peers");
            return ValueTask.CompletedTask;
        }

        var stats = replicated.GetRaftStats();

        switch (context.ArgumentCount == 0 ? "INFO" : context.Keyword(1))
        {
            case "INFO":
            {
                var text = new StringBuilder();
                text.Append("cluster_enabled:1\r\n");
                text.Append("cluster_state:").Append(stats.LeaderId is null ? "down" : "ok").Append("\r\n");
                text.Append("consensus:raft\r\n");
                text.Append("node_id:").Append(stats.NodeId).Append("\r\n");
                text.Append("role:").Append(stats.Role.ToString().ToLowerInvariant()).Append("\r\n");
                text.Append("term:").Append(stats.Term).Append("\r\n");
                text.Append("leader:").Append(stats.LeaderId ?? "none").Append("\r\n");
                text.Append("log_index:").Append(stats.LastLogIndex).Append("\r\n");
                text.Append("commit_index:").Append(stats.CommitIndex).Append("\r\n");
                text.Append("last_applied:").Append(stats.LastApplied).Append("\r\n");
                text.Append("log_bytes:").Append(stats.LogSizeBytes).Append("\r\n");
                text.Append("elections_started:").Append(stats.ElectionsStarted).Append("\r\n");
                text.Append("elections_won:").Append(stats.ElectionsWon).Append("\r\n");
                RespWriter.WriteBulkString(context.Output, text.ToString());
                break;
            }

            case "NODES":
            {
                var text = new StringBuilder();
                text.Append(stats.NodeId).Append(' ')
                    .Append("myself,").Append(stats.Role.ToString().ToLowerInvariant())
                    .Append(" term=").Append(stats.Term)
                    .Append(" log=").Append(stats.LastLogIndex)
                    .Append(" commit=").Append(stats.CommitIndex)
                    .Append("\r\n");

                foreach (var (peerId, matchIndex) in stats.MatchIndex.OrderBy(p => p.Key))
                {
                    text.Append(peerId).Append(" peer match=").Append(matchIndex).Append("\r\n");
                }

                RespWriter.WriteBulkString(context.Output, text.ToString());
                break;
            }

            case "MYID":
                RespWriter.WriteBulkString(context.Output, stats.NodeId);
                break;

            default:
                RespWriter.WriteError(
                    context.Output, $"ERR Unknown CLUSTER subcommand '{context.Text(1)}'");
                break;
        }

        return ValueTask.CompletedTask;
    }

    /// <summary>Assembly informational version, or a placeholder.</summary>
    internal static string Version { get; } =
        typeof(ServerCommands).Assembly
            .GetCustomAttributes(typeof(System.Reflection.AssemblyInformationalVersionAttribute), false)
            .OfType<System.Reflection.AssemblyInformationalVersionAttribute>()
            .FirstOrDefault()?.InformationalVersion
            .Split('+')[0]
        ?? "0.1.0";
}
