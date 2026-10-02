using KestrelCache.Server;
using Xunit;
using Xunit.Abstractions;

namespace KestrelCache.Tests;

/// <summary>
/// A server bound to an ephemeral port, torn down with the test.
/// </summary>
/// <remarks>
/// Port 0 asks the OS for any free port, which is what lets these tests run in parallel and on
/// a CI machine that may already have something on 6380.
/// </remarks>
public sealed class RespServerFixture : IAsyncDisposable
{
    private readonly TempDirectory _directory;
    private readonly CancellationTokenSource _shutdown = new();
    private readonly Task _serving;
    private readonly KestrelDb _database;
    private readonly RespServer _server;

    public RespServerFixture(EngineKind engine = EngineKind.Lsm, string? password = null)
    {
        _directory = new TempDirectory($"resp-{engine}");

        var options = new ServerOptions
        {
            Port = 0,
            MetricsPort = 0,
            BindAddress = "127.0.0.1",
            DataPath = engine == EngineKind.Bitcask
                ? _directory.File("server.kc")
                : _directory.File("server"),
            Engine = engine,
            SyncPolicy = SyncPolicy.None,
            RequirePassword = password,
        };

        _database = KestrelDb.OpenAsync(options.ToDatabaseOptions(), engine)
            .AsTask().GetAwaiter().GetResult();

        _server = new RespServer(options, _database);
        _server.Bind();
        Port = _server.BoundPort;

        _serving = _server.RunAsync(_shutdown.Token);
    }

    /// <summary>The port the server actually bound.</summary>
    public int Port { get; }

    /// <summary>Opens a client against this server.</summary>
    public Task<RespTestClient> ConnectAsync() => RespTestClient.ConnectAsync(Port);

    public async ValueTask DisposeAsync()
    {
        await _shutdown.CancelAsync();
        await _server.DisposeAsync();

        try
        {
            await _serving;
        }
        catch (OperationCanceledException)
        {
            // Expected.
        }

        await _database.DisposeAsync();
        _shutdown.Dispose();
        _directory.Dispose();
    }
}

public sealed class RespServerTests(ITestOutputHelper output)
{
    // ------------------------------------------------------------ protocol

    [Fact]
    public async Task Ping_returns_pong()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        Assert.Equal(new RespReply.Status("PONG"), await client.CommandAsync("PING"));
    }

    [Fact]
    public async Task Ping_with_an_argument_echoes_it()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        var reply = Assert.IsType<RespReply.Bulk>(await client.CommandAsync("PING", "hello"));
        Assert.Equal("hello", reply.Text);
    }

    /// <summary>
    /// A bare <c>PING\r\n</c> with no RESP framing, which is what a human at a telnet prompt
    /// sends.
    /// </summary>
    [Fact]
    public async Task An_inline_command_is_accepted()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        await client.SendRawAsync("PING\r\n");

        Assert.Equal(new RespReply.Status("PONG"), await client.ReadReplyAsync());
    }

    [Fact]
    public async Task Commands_are_case_insensitive()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        Assert.Equal(new RespReply.Status("OK"), await client.CommandAsync("set", "k", "v"));
        Assert.Equal(new RespReply.Status("OK"), await client.CommandAsync("SeT", "k", "v"));
    }

    /// <summary>
    /// A command arriving in pieces must be buffered and parsed once it is complete, not
    /// rejected. This is the case a naive read-and-parse loop gets wrong.
    /// </summary>
    [Fact]
    public async Task A_command_split_across_packets_is_reassembled()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        await client.SendRawAsync("*3\r\n$3\r\nSE");
        await Task.Delay(50);
        await client.SendRawAsync("T\r\n$5\r\nsplit\r\n$5\r\nva");
        await Task.Delay(50);
        await client.SendRawAsync("lue\r\n");

        Assert.Equal(new RespReply.Status("OK"), await client.ReadReplyAsync());

        var reply = Assert.IsType<RespReply.Bulk>(await client.CommandAsync("GET", "split"));
        Assert.Equal("value", reply.Text);
    }

    /// <summary>
    /// Several commands in one write must all be answered, in order. This is what makes
    /// client-side pipelining work, and it is most of why a pipelined benchmark reports an order
    /// of magnitude more throughput than an unpipelined one.
    /// </summary>
    [Fact]
    public async Task Pipelined_commands_are_answered_in_order()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        var request = new System.Text.StringBuilder();
        for (int i = 0; i < 100; i++)
        {
            request.Append($"*3\r\n$3\r\nSET\r\n$6\r\nkey:{i:D2}\r\n$5\r\nval{i:D2}\r\n");
        }
        await client.SendRawAsync(request.ToString());

        for (int i = 0; i < 100; i++)
        {
            Assert.Equal(new RespReply.Status("OK"), await client.ReadReplyAsync());
        }

        for (int i = 0; i < 100; i++)
        {
            var reply = Assert.IsType<RespReply.Bulk>(await client.CommandAsync("GET", $"key:{i:D2}"));
            Assert.Equal($"val{i:D2}", reply.Text);
        }
    }

    [Fact]
    public async Task Malformed_resp_is_reported_and_the_connection_closed()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        await client.SendRawAsync("*2\r\n$3\r\nGET\r\n+notabulkstring\r\n");

        var reply = Assert.IsType<RespReply.Error>(await client.ReadReplyAsync());
        Assert.Contains("Protocol error", reply.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task An_unknown_command_is_an_error_but_the_connection_survives()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        var reply = Assert.IsType<RespReply.Error>(await client.CommandAsync("NOSUCHTHING"));
        Assert.Contains("unknown command", reply.Message, StringComparison.Ordinal);

        // The connection must still be usable: one bad command is not a protocol violation.
        Assert.Equal(new RespReply.Status("PONG"), await client.CommandAsync("PING"));
    }

    [Fact]
    public async Task Wrong_arity_is_reported()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        var reply = Assert.IsType<RespReply.Error>(await client.CommandAsync("GET"));
        Assert.Contains("wrong number of arguments", reply.Message, StringComparison.Ordinal);
    }

    // ------------------------------------------------------------ strings

    [Fact]
    public async Task Set_and_get_round_trip()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        await client.CommandAsync("SET", "hello", "world");

        var reply = Assert.IsType<RespReply.Bulk>(await client.CommandAsync("GET", "hello"));
        Assert.Equal("world", reply.Text);
    }

    /// <summary>
    /// A missing key is the null bulk string, which is a different reply from an empty one. A
    /// client that cannot tell them apart cannot distinguish "no such key" from "a key whose
    /// value is the empty string".
    /// </summary>
    [Fact]
    public async Task A_missing_key_is_null_and_an_empty_value_is_not()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        var missing = Assert.IsType<RespReply.Bulk>(await client.CommandAsync("GET", "absent"));
        Assert.Null(missing.Value);

        await client.CommandAsync("SET", "empty", string.Empty);
        var empty = Assert.IsType<RespReply.Bulk>(await client.CommandAsync("GET", "empty"));
        Assert.NotNull(empty.Value);
        Assert.Empty(empty.Value);
    }

    [Fact]
    public async Task Set_nx_and_xx_respect_existence()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        Assert.IsType<RespReply.Bulk>(await client.CommandAsync("SET", "k", "first", "XX"));
        Assert.Null(await client.TextAsync("GET", "k"));

        Assert.Equal("OK", (await client.CommandAsync("SET", "k", "first", "NX")).AsText);
        Assert.Equal("first", await client.TextAsync("GET", "k"));

        Assert.IsType<RespReply.Bulk>(await client.CommandAsync("SET", "k", "second", "NX"));
        Assert.Equal("first", await client.TextAsync("GET", "k"));

        Assert.Equal("OK", (await client.CommandAsync("SET", "k", "second", "XX")).AsText);
        Assert.Equal("second", await client.TextAsync("GET", "k"));
    }

    /// <summary>
    /// Unsupported SET options must be refused, not ignored. Silently dropping <c>EX</c> would
    /// return <c>+OK</c> for a key the caller believes will expire and which never will.
    /// </summary>
    [Fact]
    public async Task Unsupported_set_options_are_refused_rather_than_ignored()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        var reply = Assert.IsType<RespReply.Error>(
            await client.CommandAsync("SET", "k", "v", "EX", "10"));
        Assert.Contains("expiry is not supported", reply.Message, StringComparison.Ordinal);

        Assert.Null(await client.TextAsync("GET", "k"));
    }

    [Fact]
    public async Task Del_reports_how_many_keys_existed()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        await client.CommandAsync("MSET", "a", "1", "b", "2");

        Assert.Equal(new RespReply.Number(2), await client.CommandAsync("DEL", "a", "b", "absent"));
        Assert.Null(await client.TextAsync("GET", "a"));
    }

    [Fact]
    public async Task Mget_returns_nulls_for_missing_keys_in_position()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        await client.CommandAsync("MSET", "a", "1", "c", "3");

        var reply = Assert.IsType<RespReply.Array>(
            await client.CommandAsync("MGET", "a", "b", "c"));

        Assert.NotNull(reply.Items);
        Assert.Equal(3, reply.Items.Length);
        Assert.Equal("1", ((RespReply.Bulk)reply.Items[0]).Text);
        Assert.Null(((RespReply.Bulk)reply.Items[1]).Value);
        Assert.Equal("3", ((RespReply.Bulk)reply.Items[2]).Text);
    }

    [Fact]
    public async Task Append_and_strlen_work()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        Assert.Equal(new RespReply.Number(5), await client.CommandAsync("APPEND", "k", "hello"));
        Assert.Equal(new RespReply.Number(10), await client.CommandAsync("APPEND", "k", "world"));
        Assert.Equal(new RespReply.Number(10), await client.CommandAsync("STRLEN", "k"));
        Assert.Equal("helloworld", await client.TextAsync("GET", "k"));
    }

    // ------------------------------------------------------------ counters

    [Fact]
    public async Task Incr_counts_from_zero()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        Assert.Equal(new RespReply.Number(1), await client.CommandAsync("INCR", "n"));
        Assert.Equal(new RespReply.Number(42), await client.CommandAsync("INCRBY", "n", "41"));
        Assert.Equal(new RespReply.Number(41), await client.CommandAsync("DECR", "n"));
        Assert.Equal(new RespReply.Number(1), await client.CommandAsync("DECRBY", "n", "40"));
    }

    [Fact]
    public async Task Incr_on_a_non_numeric_value_is_an_error()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        await client.CommandAsync("SET", "k", "notanumber");

        var reply = Assert.IsType<RespReply.Error>(await client.CommandAsync("INCR", "k"));
        Assert.Contains("not an integer", reply.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Incr_overflow_is_reported_rather_than_wrapping()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        await client.CommandAsync("SET", "n", long.MaxValue.ToString());

        var reply = Assert.IsType<RespReply.Error>(await client.CommandAsync("INCR", "n"));
        Assert.Contains("overflow", reply.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The reason the counter commands take a per-key lock: concurrent increments must all land.
    /// Without the lock this loses increments, and the loss is silent.
    /// </summary>
    [Fact]
    public async Task Concurrent_increments_are_not_lost()
    {
        await using var fixture = new RespServerFixture();

        const int clients = 8;
        const int perClient = 250;

        await Task.WhenAll(Enumerable.Range(0, clients).Select(async _ =>
        {
            using var client = await fixture.ConnectAsync();
            for (int i = 0; i < perClient; i++)
            {
                await client.CommandAsync("INCR", "shared");
            }
        }));

        using var verifier = await fixture.ConnectAsync();
        Assert.Equal(
            (clients * perClient).ToString(),
            await verifier.TextAsync("GET", "shared"));
    }

    // ------------------------------------------------------------ iteration

    [Fact]
    public async Task Keys_matches_a_glob()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        await client.CommandAsync("MSET",
            "user:1", "a", "user:2", "b", "user:10", "c",
            "usera", "d", "order:1", "e");

        var reply = Assert.IsType<RespReply.Array>(await client.CommandAsync("KEYS", "user:*"));

        Assert.NotNull(reply.Items);
        Assert.Equal(
            ["user:1", "user:10", "user:2"],
            reply.Items.Select(item => ((RespReply.Bulk)item).Text));
    }

    [Fact]
    public async Task Keys_supports_question_marks_and_classes()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        await client.CommandAsync("MSET", "ka", "1", "kb", "2", "kc", "3", "kz", "4", "kaa", "5");

        var single = Assert.IsType<RespReply.Array>(await client.CommandAsync("KEYS", "k?"));
        Assert.Equal(4, single.Items!.Length);

        var klass = Assert.IsType<RespReply.Array>(await client.CommandAsync("KEYS", "k[a-c]"));
        Assert.Equal(
            ["ka", "kb", "kc"],
            klass.Items!.Select(item => ((RespReply.Bulk)item).Text));
    }

    /// <summary>
    /// A full SCAN must terminate and return every key exactly once.
    /// </summary>
    /// <remarks>
    /// Because the keyspace is sorted, the cursor can be the last key returned, which gives this
    /// SCAN a guarantee Redis's hash-table cursor cannot: no duplicates, and no missed keys for
    /// anything present throughout the iteration.
    /// </remarks>
    [Fact]
    public async Task Scan_visits_every_key_exactly_once()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        const int total = 500;
        for (int i = 0; i < total; i += 50)
        {
            var arguments = new List<string> { "MSET" };
            for (int j = i; j < i + 50; j++)
            {
                arguments.Add($"key:{j:D4}");
                arguments.Add($"v{j}");
            }
            await client.CommandAsync([.. arguments]);
        }

        var seen = new List<string>();
        string cursor = "0";
        int iterations = 0;

        do
        {
            var reply = Assert.IsType<RespReply.Array>(
                await client.CommandAsync("SCAN", cursor, "COUNT", "37"));

            Assert.NotNull(reply.Items);
            Assert.Equal(2, reply.Items.Length);

            cursor = ((RespReply.Bulk)reply.Items[0]).Text!;
            var page = Assert.IsType<RespReply.Array>(reply.Items[1]);

            foreach (var item in page.Items!)
            {
                seen.Add(((RespReply.Bulk)item).Text!);
            }

            Assert.True(++iterations < 200, "SCAN did not terminate");
        }
        while (cursor != "0");

        output.WriteLine($"{total} keys in {iterations} SCAN call(s)");

        Assert.Equal(total, seen.Count);
        Assert.Equal(total, seen.Distinct().Count());
        Assert.Equal(seen.OrderBy(k => k, StringComparer.Ordinal), seen);
    }

    [Fact]
    public async Task Scan_honours_a_match_pattern()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        await client.CommandAsync("MSET",
            "user:1", "a", "user:2", "b", "order:1", "c", "order:2", "d");

        var seen = new List<string>();
        string cursor = "0";

        do
        {
            var reply = Assert.IsType<RespReply.Array>(
                await client.CommandAsync("SCAN", cursor, "MATCH", "user:*", "COUNT", "2"));

            cursor = ((RespReply.Bulk)reply.Items![0]).Text!;
            foreach (var item in ((RespReply.Array)reply.Items[1]).Items!)
            {
                seen.Add(((RespReply.Bulk)item).Text!);
            }
        }
        while (cursor != "0");

        Assert.Equal(["user:1", "user:2"], seen);
    }

    /// <summary>
    /// The Bitcask engine has no ordered iteration, so these commands must refuse clearly rather
    /// than return something wrong.
    /// </summary>
    [Fact]
    public async Task Scan_and_keys_refuse_clearly_on_the_bitcask_engine()
    {
        await using var fixture = new RespServerFixture(EngineKind.Bitcask);
        using var client = await fixture.ConnectAsync();

        await client.CommandAsync("SET", "k", "v");

        foreach (string[] command in (string[][])[["SCAN", "0"], ["KEYS", "*"]])
        {
            var reply = Assert.IsType<RespReply.Error>(await client.CommandAsync(command));
            Assert.Contains("ordered iteration", reply.Message, StringComparison.Ordinal);
            Assert.Contains("lsm", reply.Message, StringComparison.Ordinal);
        }

        // Point operations still work.
        Assert.Equal("v", await client.TextAsync("GET", "k"));
    }

    // ------------------------------------------------------------ handshake and admin

    [Fact]
    public async Task Hello_reports_protocol_two_even_when_three_is_requested()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        var reply = Assert.IsType<RespReply.Array>(await client.CommandAsync("HELLO", "3"));

        Assert.NotNull(reply.Items);
        int protoIndex = Array.FindIndex(
            reply.Items, item => (item as RespReply.Bulk)?.Text == "proto");
        Assert.True(protoIndex >= 0);

        // Claiming RESP3 while speaking RESP2 would break clients worse than declining does.
        Assert.Equal(new RespReply.Number(2), reply.Items[protoIndex + 1]);
    }

    [Fact]
    public async Task The_handshake_commands_real_clients_send_are_answered()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        // redis-cli sends all of these on connect and treats an error from any of them as fatal.
        Assert.IsNotType<RespReply.Error>(await client.CommandAsync("COMMAND", "DOCS"));
        Assert.IsNotType<RespReply.Error>(
            await client.CommandAsync("CLIENT", "SETINFO", "LIB-NAME", "test"));
        Assert.IsNotType<RespReply.Error>(await client.CommandAsync("CONFIG", "GET", "save"));
        Assert.Equal(new RespReply.Status("OK"), await client.CommandAsync("SELECT", "0"));
    }

    [Fact]
    public async Task Selecting_a_database_other_than_zero_is_refused()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        Assert.IsType<RespReply.Error>(await client.CommandAsync("SELECT", "3"));
    }

    [Fact]
    public async Task Info_reports_engine_sections()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        await client.CommandAsync("SET", "k", "v");

        var reply = Assert.IsType<RespReply.Bulk>(await client.CommandAsync("INFO"));
        string text = reply.Text!;

        output.WriteLine(text);

        Assert.Contains("# Engine", text, StringComparison.Ordinal);
        Assert.Contains("engine:lsm", text, StringComparison.Ordinal);
        Assert.Contains("redis_version:", text, StringComparison.Ordinal);
        Assert.Contains("keys_exact:0", text, StringComparison.Ordinal);
    }

    /// <summary>
    /// DBSIZE returns the engine's cheap estimate by default, and the exact count only when
    /// asked — because on the LSM engine the exact count means a full scan.
    /// </summary>
    [Fact]
    public async Task Dbsize_estimates_by_default_and_is_exact_on_request()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        await client.CommandAsync("MSET", "a", "1", "b", "2", "c", "3");
        await client.CommandAsync("DEL", "b");

        var estimate = Assert.IsType<RespReply.Number>(await client.CommandAsync("DBSIZE"));
        var exact = Assert.IsType<RespReply.Number>(await client.CommandAsync("DBSIZE", "EXACT"));

        output.WriteLine($"estimate {estimate.Value}, exact {exact.Value}");

        Assert.Equal(2, exact.Value);

        // The estimate counts every version and tombstone, so it is an upper bound.
        Assert.True(estimate.Value >= exact.Value);
    }

    [Fact]
    public async Task Dbsize_is_exact_on_the_bitcask_engine()
    {
        await using var fixture = new RespServerFixture(EngineKind.Bitcask);
        using var client = await fixture.ConnectAsync();

        await client.CommandAsync("MSET", "a", "1", "b", "2");
        await client.CommandAsync("DEL", "a");

        Assert.Equal(new RespReply.Number(1), await client.CommandAsync("DBSIZE"));
    }

    [Fact]
    public async Task Flushdb_empties_the_keyspace()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        await client.CommandAsync("MSET", "a", "1", "b", "2", "c", "3");
        Assert.Equal(new RespReply.Status("OK"), await client.CommandAsync("FLUSHDB"));

        Assert.Equal(new RespReply.Number(0), await client.CommandAsync("DBSIZE", "EXACT"));
        Assert.Null(await client.TextAsync("GET", "a"));
    }

    [Fact]
    public async Task Quit_closes_the_connection()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        Assert.Equal(new RespReply.Status("OK"), await client.CommandAsync("QUIT"));
        await Assert.ThrowsAnyAsync<Exception>(async () => await client.CommandAsync("PING"));
    }

    // ------------------------------------------------------------ authentication

    [Fact]
    public async Task Commands_are_refused_until_auth_succeeds()
    {
        await using var fixture = new RespServerFixture(password: "s3cret");
        using var client = await fixture.ConnectAsync();

        var refused = Assert.IsType<RespReply.Error>(await client.CommandAsync("GET", "k"));
        Assert.StartsWith("NOAUTH", refused.Message, StringComparison.Ordinal);

        // PING must work unauthenticated, so health checks do not need the password.
        Assert.Equal(new RespReply.Status("PONG"), await client.CommandAsync("PING"));

        Assert.IsType<RespReply.Error>(await client.CommandAsync("AUTH", "wrong"));
        Assert.IsType<RespReply.Error>(await client.CommandAsync("GET", "k"));

        Assert.Equal(new RespReply.Status("OK"), await client.CommandAsync("AUTH", "s3cret"));
        Assert.IsType<RespReply.Bulk>(await client.CommandAsync("GET", "k"));
    }

    [Fact]
    public async Task Auth_is_refused_when_no_password_is_configured()
    {
        await using var fixture = new RespServerFixture();
        using var client = await fixture.ConnectAsync();

        Assert.IsType<RespReply.Error>(await client.CommandAsync("AUTH", "anything"));
    }

    // ------------------------------------------------------------ concurrency

    [Fact]
    public async Task Many_concurrent_clients_are_served_correctly()
    {
        await using var fixture = new RespServerFixture();

        const int clients = 16;
        const int perClient = 100;

        await Task.WhenAll(Enumerable.Range(0, clients).Select(async id =>
        {
            using var client = await fixture.ConnectAsync();

            for (int i = 0; i < perClient; i++)
            {
                string key = $"c{id}:k{i}";
                Assert.Equal(
                    new RespReply.Status("OK"),
                    await client.CommandAsync("SET", key, $"{id}-{i}"));
            }

            for (int i = 0; i < perClient; i++)
            {
                string key = $"c{id}:k{i}";
                Assert.Equal($"{id}-{i}", await client.TextAsync("GET", key));
            }
        }));

        using var verifier = await fixture.ConnectAsync();
        var exact = Assert.IsType<RespReply.Number>(await verifier.CommandAsync("DBSIZE", "EXACT"));
        Assert.Equal(clients * perClient, exact.Value);
    }

    [Fact]
    public async Task Data_written_through_the_server_survives_a_restart()
    {
        using var directory = new TempDirectory("resp-restart");
        string dataPath = directory.File("server");

        var options = new ServerOptions
        {
            Port = 0,
            MetricsPort = 0,
            BindAddress = "127.0.0.1",
            DataPath = dataPath,
            Engine = EngineKind.Lsm,
            SyncPolicy = SyncPolicy.EveryWrite,
        };

        async Task<int> RunAsync(Func<RespTestClient, Task> body)
        {
            var database = await KestrelDb.OpenAsync(options.ToDatabaseOptions(), options.Engine);
            await using (database)
            {
                var server = new RespServer(options, database);
                server.Bind();

                using var shutdown = new CancellationTokenSource();
                var serving = server.RunAsync(shutdown.Token);

                using (var client = await RespTestClient.ConnectAsync(server.BoundPort))
                {
                    await body(client);
                }

                await shutdown.CancelAsync();
                await server.DisposeAsync();
                try
                {
                    await serving;
                }
                catch (OperationCanceledException)
                {
                }

                return server.BoundPort;
            }
        }

        await RunAsync(async client =>
        {
            await client.CommandAsync("MSET", "a", "1", "b", "2");
            await client.CommandAsync("INCR", "n");
        });

        await RunAsync(async client =>
        {
            Assert.Equal("1", await client.TextAsync("GET", "a"));
            Assert.Equal("2", await client.TextAsync("GET", "b"));
            Assert.Equal("1", await client.TextAsync("GET", "n"));
        });
    }
}
