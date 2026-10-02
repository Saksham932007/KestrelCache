using System.Text;
using KestrelCache.Raft;
using Xunit;
using Xunit.Abstractions;

namespace KestrelCache.Tests;

/// <summary>
/// Tests the real TCP transport and wire format, rather than the in-process network the
/// consensus tests use.
/// </summary>
/// <remarks>
/// The in-memory network is the right tool for testing the protocol, because it can partition
/// and drop messages deterministically. It is the wrong tool for testing the <i>encoding</i>:
/// it passes message objects by reference and never serialises anything, so a broken frame
/// layout or a field written in the wrong order would go completely unnoticed. These tests run
/// a cluster over genuine sockets on ephemeral ports, so every message is framed, checksummed,
/// written, read and decoded.
/// </remarks>
[Collection("raft-tcp")]
public sealed class RaftTransportTests(ITestOutputHelper output)
{
    private sealed record Node(
        string Id,
        RaftNode Raft,
        ReplicatedStore Store,
        RaftRpcServer Rpc,
        TcpRaftTransport Transport,
        Task Serving,
        CancellationTokenSource Shutdown);

    /// <summary>
    /// Builds a cluster whose nodes talk over TCP. Ports are assigned by the OS, so the test can
    /// run in parallel with anything else.
    /// </summary>
    private static async Task<List<Node>> StartClusterAsync(
        TempDirectory directory,
        int size,
        ITestOutputHelper output)
    {
        var ids = Enumerable.Range(1, size).Select(i => $"n{i}").ToArray();

        // Bind every peer listener first, so the addresses are known before any node is told
        // about its peers -- a node cannot be configured with a port that has not been assigned.
        var listeners = new Dictionary<string, RaftRpcServer>();
        var engines = new Dictionary<string, IStorageEngine>();
        var stores = new Dictionary<string, ReplicatedStore>();
        var addresses = new List<RaftPeerAddress>();

        foreach (string id in ids)
        {
            string nodeDirectory = directory.File(id);
            Directory.CreateDirectory(nodeDirectory);

            engines[id] = await Lsm.LsmEngine.OpenAsync(new DatabaseOptions
            {
                Path = Path.Combine(nodeDirectory, "data"),
                SyncPolicy = SyncPolicy.None,
                EnableBackgroundCompaction = false,
            });

            stores[id] = new ReplicatedStore(engines[id]);

            // Bound before the node exists, so the OS-assigned port is known in time to
            // configure every node's peer list.
            var rpc = new RaftRpcServer("127.0.0.1", port: 0);
            rpc.Bind();
            listeners[id] = rpc;

            addresses.Add(new RaftPeerAddress(id, "127.0.0.1", rpc.BoundPort));
        }

        output.WriteLine("peers: " + string.Join(", ", addresses));

        var nodes = new List<Node>();

        foreach (string id in ids)
        {
            var transport = new TcpRaftTransport(addresses);
            var options = new RaftOptions
            {
                NodeId = id,
                Peers = ids,
                DataDirectory = Path.Combine(directory.File(id), "raft"),
                ElectionTimeout = TimeSpan.FromMilliseconds(250),
                HeartbeatInterval = TimeSpan.FromMilliseconds(40),
                RpcTimeout = TimeSpan.FromMilliseconds(400),
                RandomSeed = id.GetHashCode(StringComparison.Ordinal),
            };

            var raft = new RaftNode(options, transport, stores[id]);
            stores[id].Attach(raft);
            listeners[id].Attach(raft);

            var shutdown = new CancellationTokenSource();
            var serving = listeners[id].RunAsync(shutdown.Token);
            await raft.StartAsync();

            nodes.Add(new Node(
                id, raft, stores[id], listeners[id], transport, serving, shutdown));
        }

        return nodes;
    }

    private static async Task StopClusterAsync(List<Node> nodes)
    {
        foreach (var node in nodes)
        {
            await node.Shutdown.CancelAsync();
            await node.Rpc.DisposeAsync();
            await node.Raft.DisposeAsync();
            await node.Store.DisposeAsync();
            await node.Transport.DisposeAsync();
            node.Shutdown.Dispose();
        }
    }

    private static async Task<Node?> WaitForLeaderAsync(
        List<Node> nodes,
        TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;

        while (DateTime.UtcNow < deadline)
        {
            var leaders = nodes.Where(n => n.Raft.IsLeader).ToList();
            if (leaders.Count == 1) return leaders[0];
            await Task.Delay(25);
        }

        return nodes.FirstOrDefault(n => n.Raft.IsLeader);
    }

    [Fact]
    public async Task A_cluster_over_real_sockets_elects_a_leader_and_replicates()
    {
        using var directory = new TempDirectory("raft-tcp");
        var nodes = await StartClusterAsync(directory, 3, output);

        try
        {
            var leader = await WaitForLeaderAsync(nodes, TimeSpan.FromSeconds(15));
            Assert.NotNull(leader);
            output.WriteLine($"{leader.Id} leads in term {leader.Raft.CurrentTerm}");

            await leader.Store.WriteAsync(new WriteBatch()
                .Put("over-the-wire", "yes")
                .Put("another", "value"));

            // Every node must converge, which means every frame was encoded, transmitted and
            // decoded correctly.
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
            while (DateTime.UtcNow < deadline
                && nodes.Any(n => n.Raft.LastApplied < leader.Raft.LastLogIndex))
            {
                await Task.Delay(25);
            }

            foreach (var node in nodes)
            {
                byte[]? value = await node.Store.GetAsync(Encoding.UTF8.GetBytes("over-the-wire"));
                Assert.Equal(
                    "yes",
                    value is null ? null : Encoding.UTF8.GetString(value));
            }

            output.WriteLine(
                $"frames served: {string.Join(", ", nodes.Select(n => $"{n.Id}={n.Rpc.FramesServed}"))}");

            Assert.True(
                nodes.Sum(n => n.Rpc.FramesServed) > 0,
                "no frames crossed the wire, so the transport was not exercised");
        }
        finally
        {
            await StopClusterAsync(nodes);
        }
    }

    [Fact]
    public async Task Large_values_survive_the_wire_format()
    {
        using var directory = new TempDirectory("raft-tcp-large");
        var nodes = await StartClusterAsync(directory, 3, output);

        try
        {
            var leader = await WaitForLeaderAsync(nodes, TimeSpan.FromSeconds(15));
            Assert.NotNull(leader);

            // A value far larger than any single TCP segment, so the frame must be reassembled
            // from several reads on the receiving side.
            byte[] large = new byte[512 * 1024];
            new Random(42).NextBytes(large);

            await leader.Store.PutAsync(Encoding.UTF8.GetBytes("large"), large);

            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(20);
            while (DateTime.UtcNow < deadline
                && nodes.Any(n => n.Raft.LastApplied < leader.Raft.LastLogIndex))
            {
                await Task.Delay(25);
            }

            foreach (var node in nodes)
            {
                byte[]? value = await node.Store.GetAsync(Encoding.UTF8.GetBytes("large"));
                Assert.NotNull(value);
                Assert.Equal(large, value);
            }
        }
        finally
        {
            await StopClusterAsync(nodes);
        }
    }

    [Fact]
    public async Task An_unreachable_peer_does_not_stop_the_cluster()
    {
        using var directory = new TempDirectory("raft-tcp-dead");
        var nodes = await StartClusterAsync(directory, 3, output);

        try
        {
            var leader = await WaitForLeaderAsync(nodes, TimeSpan.FromSeconds(15));
            Assert.NotNull(leader);

            // Close one follower's listener outright: connections to it now fail at connect,
            // which is the condition the majority requirement exists to tolerate.
            var victim = nodes.First(n => n.Id != leader.Id);
            await victim.Shutdown.CancelAsync();
            await victim.Rpc.DisposeAsync();
            output.WriteLine($"took {victim.Id}'s listener down");

            await leader.Store.WriteAsync(new WriteBatch().Put("quorum", "sufficient"));

            var survivors = nodes.Where(n => n.Id != victim.Id).ToList();

            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
            while (DateTime.UtcNow < deadline
                && survivors.Any(n => n.Raft.LastApplied < leader.Raft.LastLogIndex))
            {
                await Task.Delay(25);
            }

            foreach (var node in survivors)
            {
                byte[]? value = await node.Store.GetAsync(Encoding.UTF8.GetBytes("quorum"));
                Assert.Equal("sufficient", value is null ? null : Encoding.UTF8.GetString(value));
            }
        }
        finally
        {
            await StopClusterAsync(nodes);
        }
    }

    /// <summary>
    /// Regression test: tearing a server down twice must not throw. Nested teardown paths and a
    /// close-then-dispose sequence both produce it, and an exception on shutdown turns an orderly
    /// stop into a crash.
    /// </summary>
    [Fact]
    public async Task Disposing_the_rpc_server_twice_is_safe()
    {
        var server = new RaftRpcServer("127.0.0.1", port: 0);
        server.Bind();

        await server.DisposeAsync();
        await server.DisposeAsync();
    }

    [Fact]
    public void A_peer_address_round_trips_through_its_text_form()
    {
        var address = RaftPeerAddress.Parse("n1=10.0.0.5:7381");

        Assert.Equal("n1", address.NodeId);
        Assert.Equal("10.0.0.5", address.Host);
        Assert.Equal(7381, address.Port);
        Assert.Equal("n1=10.0.0.5:7381", address.ToString());
    }

    [Theory]
    [InlineData("no-equals")]
    [InlineData("n1=hostonly")]
    [InlineData("n1=host:notaport")]
    [InlineData("=127.0.0.1:1")]
    public void A_malformed_peer_address_is_rejected(string text) =>
        Assert.Throws<FormatException>(() => RaftPeerAddress.Parse(text));
}

/// <summary>TCP cluster tests bind real sockets, so they run one at a time.</summary>
[CollectionDefinition("raft-tcp", DisableParallelization = true)]
public sealed class RaftTcpCollection;
