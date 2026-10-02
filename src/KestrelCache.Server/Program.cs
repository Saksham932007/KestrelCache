using KestrelCache;
using KestrelCache.Server;
using KestrelCache.Server.Observability;

if (args.Length > 0 && args[0] is "-h" or "--help" or "help")
{
    Console.WriteLine(
        """
        kestrel-server - a KestrelCache server speaking the Redis protocol

        USAGE
          kestrel-server [options]

        OPTIONS
          --port N          RESP port (default 6380, so it coexists with Redis on 6379)
          --bind ADDR       Address to bind (default 0.0.0.0)
          --data PATH       Database directory or file (default ./kc-data)
          --engine NAME     bitcask | lsm (default lsm)
          --sync POLICY     none | everywrite | interval (default interval)
          --metrics-port N  HTTP port for /metrics, /health, /stats (default 9180; 0 disables)
          --max-clients N   Connection limit (default 10000; 0 unlimited)
          --requirepass P   Require AUTH before serving commands

        CLUSTERING (Raft)
          --raft-id ID      This node's id; enables replication
          --raft-peers LIST Comma-separated id=host:port for every node, this one included
          --raft-port N     Port for peer traffic (default 7380)
          --raft-trace      Log every role transition and election decision

        A THREE-NODE CLUSTER
          Run each on its own ports, then write to whichever node reports itself leader:

            kestrel-server --raft-id n1 --raft-port 7381 --port 6381 --data ./d1 \
              --raft-peers n1=127.0.0.1:7381,n2=127.0.0.1:7382,n3=127.0.0.1:7383
            kestrel-server --raft-id n2 --raft-port 7382 --port 6382 --data ./d2 --raft-peers ...
            kestrel-server --raft-id n3 --raft-port 7383 --port 6383 --data ./d3 --raft-peers ...

            redis-cli -p 6381 cluster info
            redis-cli -p 6381 set replicated yes      # -NOTLEADER names the leader if not here
            redis-cli -p 6382 get replicated          # followers serve reads

        TRY IT
          redis-cli -p 6380 set hello world
          redis-cli -p 6380 get hello
          redis-cli -p 6380 --scan --pattern 'user:*'
          redis-benchmark -p 6380 -t set,get -n 100000 -P 16
          curl -s localhost:9180/metrics | head
        """);
    return 0;
}

var options = ParseOptions(args);

Console.WriteLine($"KestrelCache server {ServerVersion()}");
Console.WriteLine($"  engine        {options.Engine}");
Console.WriteLine($"  data          {Path.GetFullPath(options.DataPath)}");
Console.WriteLine($"  durability    {options.SyncPolicy}");

// A clustered node puts consensus between the protocol layer and the storage engine, but
// presents the same IStorageEngine interface -- so everything above this point is identical in
// both deployments.
ClusterHost? cluster = null;
KestrelDb database;

if (options.IsClustered)
{
    var localEngine = options.Engine == EngineKind.Bitcask
        ? (IStorageEngine)await KestrelCache.Bitcask.BitcaskEngine.OpenAsync(options.ToDatabaseOptions())
        : await KestrelCache.Lsm.LsmEngine.OpenAsync(options.ToDatabaseOptions());

    Action<string>? trace = args.Contains("--raft-trace")
        ? message => Console.WriteLine($"  raft {message}")
        : null;

    cluster = await ClusterHost.StartAsync(options, localEngine, trace);
    database = KestrelDb.Wrap(cluster.Engine);

    Console.WriteLine($"  cluster       {options.RaftNodeId} of "
        + $"[{string.Join(", ", options.RaftPeers)}]");
    Console.WriteLine($"  raft          0.0.0.0:{cluster.RaftPort}");
}
else
{
    database = await KestrelDb.OpenAsync(options.ToDatabaseOptions(), options.Engine);
}

await using (database)
{
    var server = new RespServer(options, database);
    server.Bind();
    Console.WriteLine($"  resp          0.0.0.0:{server.BoundPort}");

    MetricsEndpoint? metricsEndpoint = null;
    if (options.MetricsPort > 0)
    {
        metricsEndpoint = new MetricsEndpoint(
            options.MetricsPort, server.Metrics, database, options);
        metricsEndpoint.Bind();
        Console.WriteLine($"  metrics       http://0.0.0.0:{metricsEndpoint.BoundPort}/metrics");
    }

    using var shutdown = new CancellationTokenSource();

    // Cancellation can be requested from a signal handler that may still be registered while the
    // token source is being torn down, so every request goes through this guard. Without it, a
    // signal arriving during shutdown turns a clean exit into an unhandled
    // ObjectDisposedException and a non-zero exit code -- which in a container reads as a crash
    // loop rather than a successful stop.
    void RequestShutdown(string reason)
    {
        try
        {
            if (shutdown.IsCancellationRequested) return;
            Console.WriteLine($"shutting down ({reason})");
            shutdown.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Already shutting down.
        }
    }

    // SIGINT and SIGTERM are handled identically. SIGTERM is what Docker, Kubernetes and systemd
    // send, so treating only Ctrl+C as a shutdown request would mean every container stop killed
    // the process outright and skipped the final fsync.
    Console.CancelKeyPress += (_, eventArgs) =>
    {
        eventArgs.Cancel = true; // exit deliberately rather than being killed
        Console.WriteLine();
        RequestShutdown("SIGINT");
    };

    using var sigterm = System.Runtime.InteropServices.PosixSignalRegistration.Create(
        System.Runtime.InteropServices.PosixSignal.SIGTERM,
        signal =>
        {
            signal.Cancel = true;
            RequestShutdown("SIGTERM");
        });

    using var sighup = System.Runtime.InteropServices.PosixSignalRegistration.Create(
        System.Runtime.InteropServices.PosixSignal.SIGHUP,
        signal =>
        {
            signal.Cancel = true;
            RequestShutdown("SIGHUP");
        });

    Console.WriteLine();
    Console.WriteLine("ready");

    var serving = server.RunAsync(shutdown.Token);
    var metricsServing = metricsEndpoint?.RunAsync(shutdown.Token) ?? Task.CompletedTask;

    await Task.WhenAll(serving, metricsServing);

    await server.DisposeAsync();
    if (metricsEndpoint is not null) await metricsEndpoint.DisposeAsync();

    // The final flush is the whole reason graceful shutdown matters: everything acknowledged
    // under a bounded-loss sync policy is still in the page cache, and this is its last chance
    // to reach the disk.
    Console.WriteLine("flushing");
    await database.FlushAsync();

    var stats = database.GetStats();
    Console.WriteLine(
        $"served {server.Metrics.CommandsTotal:N0} command(s); "
            + $"{stats.Writes:N0} write(s), {stats.Reads:N0} read(s), {stats.Syncs:N0} fsync(s)");

    if (cluster is not null)
    {
        var raft = cluster.Node.GetStats();
        Console.WriteLine(
            $"raft: {raft.Role} in term {raft.Term}, log {raft.LastLogIndex}, "
                + $"committed {raft.CommitIndex}, applied {raft.LastApplied}");
        await cluster.DisposeAsync();
    }
}

Console.WriteLine("stopped");
return 0;

static ServerOptions ParseOptions(string[] argv)
{
    var options = new ServerOptions();

    for (int i = 0; i < argv.Length; i++)
    {
        if (!argv[i].StartsWith("--", StringComparison.Ordinal)) continue;

        string name = argv[i][2..];
        string? value = i + 1 < argv.Length && !argv[i + 1].StartsWith("--", StringComparison.Ordinal)
            ? argv[++i]
            : null;

        options = name switch
        {
            "port" when value is not null => options with { Port = int.Parse(value) },
            "bind" when value is not null => options with { BindAddress = value },
            "data" when value is not null => options with { DataPath = value },
            "engine" when value is not null => options with
            {
                Engine = Enum.Parse<EngineKind>(value, ignoreCase: true),
            },
            "sync" when value is not null => options with
            {
                SyncPolicy = Enum.Parse<SyncPolicy>(value, ignoreCase: true),
            },
            "metrics-port" when value is not null => options with { MetricsPort = int.Parse(value) },
            "max-clients" when value is not null => options with { MaxConnections = int.Parse(value) },
            "requirepass" when value is not null => options with { RequirePassword = value },
            "raft-id" when value is not null => options with { RaftNodeId = value },
            "raft-peers" when value is not null => options with
            {
                RaftPeers = value.Split(',', StringSplitOptions.RemoveEmptyEntries
                    | StringSplitOptions.TrimEntries),
            },
            "raft-port" when value is not null => options with { RaftPort = int.Parse(value) },
            "raft-election-timeout" when value is not null => options with
            {
                RaftElectionTimeout = TimeSpan.FromMilliseconds(int.Parse(value)),
            },
            "raft-heartbeat" when value is not null => options with
            {
                RaftHeartbeatInterval = TimeSpan.FromMilliseconds(int.Parse(value)),
            },
            _ => options,
        };
    }

    return options;
}

static string ServerVersion() => KestrelCache.Server.Commands.ServerCommands.Version;
