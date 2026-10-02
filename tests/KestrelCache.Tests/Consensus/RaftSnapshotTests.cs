using System.Text;
using KestrelCache.Raft;
using Xunit;
using Xunit.Abstractions;

namespace KestrelCache.Tests;

/// <summary>
/// Tests for log snapshotting: taking one, truncating behind it, recovering from it, and
/// shipping it to a follower that has fallen past the end of the leader's log.
/// </summary>
/// <remarks>
/// Snapshotting is what stops the Raft log growing without bound, and it introduces the one
/// situation ordinary replication cannot handle: a follower needing entries the leader has
/// already discarded. Both halves need testing — the truncation, and the state transfer that
/// covers the gap it opens.
/// </remarks>
[Collection("raft-cluster")]
public sealed class RaftSnapshotTests(ITestOutputHelper output)
{
    private static byte[] Key(string s) => Encoding.UTF8.GetBytes(s);

    private static string? Text(byte[]? value) =>
        value is null ? null : Encoding.UTF8.GetString(value);

    private static WriteBatch Put(string key, string value) => new WriteBatch().Put(key, value);

    /// <summary>
    /// A low snapshot threshold, so a few dozen writes is enough to trigger one. The production
    /// default is deliberately generous; tests need the opposite.
    /// </summary>
    private static RaftOptions SnapshotEagerly(RaftOptions options) => options with
    {
        SnapshotThresholdEntries = 25,
        SnapshotThresholdBytes = 0,
    };

    /// <summary>Small chunks, so a transfer is genuinely multi-part.</summary>
    private static RaftOptions SnapshotInSmallChunks(RaftOptions options) => StableLeadership(options) with
    {
        SnapshotThresholdEntries = 0,
        SnapshotThresholdBytes = 0,
        SnapshotChunkBytes = 16 * 1024,
    };

    /// <summary>
    /// Long election timeouts, so leadership does not change during a test that is about
    /// something else.
    /// </summary>
    /// <remarks>
    /// The snapshot-transfer tests arrange for one specific node to be the leader with a
    /// truncated log. Under the suite's usual 60 ms timeouts and a loaded machine, leadership
    /// churns, a node with a complete log takes over, and ordinary replication catches the
    /// follower up — so the test passes without ever exercising the code it was written for.
    /// Making the precondition hold is better than asserting a weaker property.
    /// </remarks>
    private static RaftOptions StableLeadership(RaftOptions options) => options with
    {
        // Long enough that a loaded machine stalling a tick does not unseat a healthy leader,
        // short enough that the first election still happens promptly. Ten seconds -- the
        // obvious overcorrection -- means no election ever happens inside a test's tick budget.
        ElectionTimeout = TimeSpan.FromMilliseconds(250),
        HeartbeatInterval = TimeSpan.FromMilliseconds(25),
    };

    // ------------------------------------------------------------ taking a snapshot

    [Fact]
    public async Task A_snapshot_truncates_the_log_behind_it()
    {
        await using var cluster = await RaftCluster.StartAsync(3);
        var leader = await cluster.WaitForLeaderAsync();

        for (int i = 0; i < 40; i++)
        {
            await leader.Store.WriteAsync(Put($"k:{i:D3}", $"v{i}"));
        }
        await cluster.WaitForAppliedAsync(leader.Node.LastLogIndex, maxTicks: 600);

        var before = leader.Node.GetStats();
        var metadata = await leader.Node.CreateSnapshotAsync();
        var after = leader.Node.GetStats();

        Assert.NotNull(metadata);
        output.WriteLine(
            $"log [{before.FirstLogIndex}..{before.LastLogIndex}] "
                + $"-> [{after.FirstLogIndex}..{after.LastLogIndex}], "
                + $"snapshot at {after.SnapshotIndex}@{after.SnapshotTerm}, "
                + $"{after.SnapshotSizeBytes} byte(s)");

        // The prefix is gone and the snapshot covers it.
        Assert.True(after.FirstLogIndex > before.FirstLogIndex);
        Assert.Equal(metadata.Value.LastIncludedIndex, after.SnapshotIndex);
        Assert.True(after.LogEntryCount < before.LogEntryCount);
        Assert.True(after.SnapshotSizeBytes > 0);

        // And the data is all still readable, which is the only thing a client cares about.
        for (int i = 0; i < 40; i++)
        {
            Assert.Equal($"v{i}", Text(await leader.Store.GetAsync(Key($"k:{i:D3}"))));
        }
    }

    [Fact]
    public async Task A_snapshot_covers_exactly_what_has_been_applied()
    {
        await using var cluster = await RaftCluster.StartAsync(3);
        var leader = await cluster.WaitForLeaderAsync();

        for (int i = 0; i < 20; i++)
        {
            await leader.Store.WriteAsync(Put($"k:{i:D3}", $"v{i}"));
        }
        await cluster.WaitForAppliedAsync(leader.Node.LastLogIndex, maxTicks: 600);

        long applied = leader.Node.LastApplied;
        var metadata = await leader.Node.CreateSnapshotAsync();

        Assert.NotNull(metadata);

        // Never beyond lastApplied: an entry that is committed but not yet applied is not in the
        // state machine, so a snapshot including it would claim to cover state it does not hold.
        Assert.Equal(applied, metadata.Value.LastIncludedIndex);
    }

    [Fact]
    public async Task Snapshotting_twice_with_no_new_writes_does_nothing()
    {
        await using var cluster = await RaftCluster.StartAsync(1);
        var leader = await cluster.WaitForLeaderAsync();

        await leader.Store.WriteAsync(Put("k", "v"));
        await cluster.WaitForAppliedAsync(leader.Node.LastLogIndex);

        Assert.NotNull(await leader.Node.CreateSnapshotAsync());

        // Nothing has been applied since, so there is nothing to fold in.
        Assert.Null(await leader.Node.CreateSnapshotAsync());
    }

    [Fact]
    public async Task Snapshots_happen_automatically_once_the_log_grows()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: SnapshotEagerly);
        var leader = await cluster.WaitForLeaderAsync();

        // Drive enough writes to cross the configured threshold.
        for (int i = 0; i < 120; i++)
        {
            await leader.Store.WriteAsync(Put($"k:{i:D3}", $"v{i}"));
        }

        await cluster.TickUntilAsync(
            () => cluster.Members.Any(m => m.Node.GetStats().SnapshotsTaken > 0),
            maxTicks: 800);

        output.WriteLine(cluster.Describe());

        var withSnapshots = cluster.Members
            .Where(m => m.Node.GetStats().SnapshotsTaken > 0)
            .ToList();

        Assert.NotEmpty(withSnapshots);

        foreach (var member in withSnapshots)
        {
            var stats = member.Node.GetStats();
            output.WriteLine(
                $"{member.NodeId}: {stats.SnapshotsTaken} snapshot(s), log starts at "
                    + $"{stats.FirstLogIndex}");
            Assert.True(stats.SnapshotIndex > 0);
        }
    }

    // ------------------------------------------------------------ recovery

    /// <summary>
    /// The payoff: a restarted node reconstructs its state from the snapshot plus the short log
    /// after it, rather than replaying everything it ever did.
    /// </summary>
    [Fact]
    public async Task A_restarted_node_recovers_from_its_snapshot()
    {
        var ids = new[] { "n1", "n2", "n3" };
        await using var cluster = await RaftCluster.StartAsync(3);
        var leader = await cluster.WaitForLeaderAsync();

        for (int i = 0; i < 60; i++)
        {
            await leader.Store.WriteAsync(Put($"k:{i:D3}", $"v{i}"));
        }
        await cluster.WaitForAppliedAsync(leader.Node.LastLogIndex, maxTicks: 800);

        string victim = cluster.NodeIds.First(id => id != leader.NodeId);
        await cluster[victim].Node.CreateSnapshotAsync();

        var beforeRestart = cluster[victim].Node.GetStats();
        output.WriteLine(
            $"{victim} before restart: snapshot at {beforeRestart.SnapshotIndex}, "
                + $"log holds {beforeRestart.LogEntryCount} entry(ies)");

        // A few more writes, so the log after the snapshot is non-empty and both halves of
        // recovery are exercised.
        for (int i = 60; i < 70; i++)
        {
            await leader.Store.WriteAsync(Put($"k:{i:D3}", $"v{i}"));
        }
        await cluster.WaitForAppliedAsync(leader.Node.LastLogIndex, maxTicks: 600);

        await cluster.StopAsync(victim);
        await cluster.RestartAsync(victim, ids);

        var restarted = cluster[victim];
        output.WriteLine($"recovery: {restarted.Node.Recovery}");

        Assert.True(restarted.Node.Recovery.SnapshotRestored);
        Assert.Equal(beforeRestart.SnapshotIndex, restarted.Node.Recovery.SnapshotIndex);

        // Everything from before the snapshot came back from the snapshot itself.
        for (int i = 0; i < 60; i++)
        {
            Assert.Equal($"v{i}", Text(await restarted.Store.GetAsync(Key($"k:{i:D3}"))));
        }

        // And it catches up on the rest from the log.
        Assert.True(
            await cluster.TickUntilAsync(
                () => restarted.Node.LastApplied >= leader.Node.LastApplied, maxTicks: 800),
            cluster.Describe());

        for (int i = 60; i < 70; i++)
        {
            Assert.Equal($"v{i}", Text(await restarted.Store.GetAsync(Key($"k:{i:D3}"))));
        }
    }

    [Fact]
    public async Task A_snapshot_restore_removes_keys_the_snapshot_does_not_contain()
    {
        var ids = new[] { "n1" };
        await using var cluster = await RaftCluster.StartAsync(1);
        var leader = await cluster.WaitForLeaderAsync();

        await leader.Store.WriteAsync(Put("survivor", "yes"));
        await cluster.WaitForAppliedAsync(leader.Node.LastLogIndex);
        await leader.Node.CreateSnapshotAsync();

        // Write a key, then put it back the way the snapshot has it by deleting it, so the
        // engine holds something the snapshot does not.
        await leader.Store.WriteAsync(Put("extra", "should-not-survive"));
        await cluster.WaitForAppliedAsync(leader.Node.LastLogIndex);

        Assert.Equal("should-not-survive", Text(await leader.Store.GetAsync(Key("extra"))));

        // Restoring the earlier snapshot must remove it. A merge would leave it behind and the
        // node permanently divergent from whatever the leader holds.
        var (_, payload) = new RaftSnapshotStore(
            Path.Combine(cluster[leader.NodeId].DataDirectory, "raft")).OpenForRestore();

        await using (payload)
        {
            await leader.Store.RestoreSnapshotAsync(payload, CancellationToken.None);
        }

        Assert.Equal("yes", Text(await leader.Store.GetAsync(Key("survivor"))));
        Assert.Null(await leader.Store.GetAsync(Key("extra")));
    }

    // ------------------------------------------------------------ install snapshot

    /// <summary>
    /// The gap snapshotting opens: a follower that was down while the leader snapshotted needs
    /// entries the leader no longer has, so it must be sent the state instead of the history.
    /// </summary>
    [Fact]
    public async Task A_follower_that_is_too_far_behind_receives_a_snapshot()
    {
        var ids = new[] { "n1", "n2", "n3" };
        await using var cluster = await RaftCluster.StartAsync(3, configure: StableLeadership);
        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        string victim = cluster.NodeIds.First(id => id != leader.NodeId);
        output.WriteLine($"isolating {victim} while the leader moves on");
        cluster.Network.Isolate(victim);

        var survivors = cluster.NodeIds.Where(id => id != victim).ToList();

        for (int i = 0; i < 50; i++)
        {
            await leader.Store.WriteAsync(Put($"k:{i:D3}", $"v{i}"));
        }
        await cluster.WaitForAppliedAsync(leader.Node.LastLogIndex, survivors, maxTicks: 800);

        // Every survivor snapshots and discards, so whichever of them is leading when the victim
        // reconnects has no choice but to send state. Snapshotting only the current leader would
        // let a leadership change during the test hand the job to a node with a complete log,
        // and the test would pass without exercising the transfer at all.
        foreach (string id in survivors)
        {
            await cluster[id].Node.CreateSnapshotAsync();
        }

        var leaderStats = leader.Node.GetStats();
        output.WriteLine(
            $"survivor logs now start at "
                + $"[{string.Join(", ", survivors.Select(id => cluster[id].Node.GetStats().FirstLogIndex))}]; "
                + $"{victim} is at {cluster[victim].Node.LastLogIndex}");

        Assert.All(
            survivors,
            id => Assert.True(
                cluster[victim].Node.LastLogIndex < cluster[id].Node.GetStats().FirstLogIndex,
                $"{victim} is not behind {id}'s log start, so this test proves nothing"));

        cluster.Network.Heal(victim);

        Assert.True(
            await cluster.TickUntilAsync(
                () => cluster[victim].Node.LastApplied >= leaderStats.SnapshotIndex,
                maxTicks: 1500),
            cluster.Describe());

        var victimStats = cluster[victim].Node.GetStats();
        output.WriteLine(
            $"{victim} installed {victimStats.SnapshotsInstalled} snapshot(s); "
                + $"leader sent {leader.Node.GetStats().SnapshotChunksSent} chunk(s)");

        Assert.True(victimStats.SnapshotsInstalled > 0, "no snapshot was installed");

        for (int i = 0; i < 50; i++)
        {
            Assert.Equal($"v{i}", Text(await cluster[victim].Store.GetAsync(Key($"k:{i:D3}"))));
        }
    }

    [Fact]
    public async Task A_large_snapshot_transfers_in_chunks()
    {
        await using var cluster = await RaftCluster.StartAsync(
            3, configure: SnapshotInSmallChunks);

        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        string victim = cluster.NodeIds.First(id => id != leader.NodeId);
        cluster.Network.Isolate(victim);
        var survivors = cluster.NodeIds.Where(id => id != victim).ToList();

        // Values large enough that the snapshot comfortably exceeds one chunk.
        string payload = new('x', 4096);
        for (int i = 0; i < 60; i++)
        {
            await leader.Store.WriteAsync(Put($"big:{i:D3}", payload));
        }
        await cluster.WaitForAppliedAsync(leader.Node.LastLogIndex, survivors, maxTicks: 1000);

        foreach (string id in survivors)
        {
            await cluster[id].Node.CreateSnapshotAsync();
        }

        long snapshotSize = leader.Node.GetStats().SnapshotSizeBytes;
        output.WriteLine($"snapshot is {snapshotSize:N0} byte(s)");

        cluster.Network.Heal(victim);

        Assert.True(
            await cluster.TickUntilAsync(
                () => cluster[victim].Node.GetStats().SnapshotsInstalled > 0,
                maxTicks: 2000),
            cluster.Describe());

        long chunks = survivors.Sum(id => cluster[id].Node.GetStats().SnapshotChunksSent);
        output.WriteLine($"transferred in {chunks} chunk(s)");

        // A 240 KB snapshot at 16 KB a chunk cannot have gone in one message.
        Assert.True(chunks > 1, $"expected a multi-chunk transfer, got {chunks} chunk(s)");

        for (int i = 0; i < 60; i++)
        {
            Assert.Equal(payload, Text(await cluster[victim].Store.GetAsync(Key($"big:{i:D3}"))));
        }
    }

    // ------------------------------------------------------------ snapshot file

    [Fact]
    public void A_corrupt_snapshot_header_is_refused()
    {
        using var directory = new TempDirectory("snapshot-corrupt");
        var store = new RaftSnapshotStore(directory.Path);

        File.WriteAllBytes(store.Path, [0, 1, 2, 3, 4, 5, 6, 7]);

        // Too short to hold a header at all.
        Assert.Null(store.TryReadMetadata());
    }

    [Fact]
    public async Task A_snapshot_with_a_damaged_payload_is_refused_rather_than_restored()
    {
        using var directory = new TempDirectory("snapshot-damaged");
        var store = new RaftSnapshotStore(directory.Path);

        var machine = new CountingStateMachine();
        await store.WriteAsync(
            new RaftSnapshotMetadata(10, 2, RaftConfiguration.Of("n1", "n2", "n3")),
            machine);

        // Flip a byte well inside the payload.
        byte[] bytes = await File.ReadAllBytesAsync(store.Path);
        bytes[^8] ^= 0xFF;
        await File.WriteAllBytesAsync(store.Path, bytes);

        // The metadata still parses -- its own checksum covers only the header -- but the restore
        // must refuse. Restoring a corrupt image would replace good state with garbage.
        Assert.NotNull(store.TryReadMetadata());
        Assert.Throws<CorruptRecordException>(() => store.OpenForRestore());
    }

    [Fact]
    public async Task A_snapshot_round_trips_its_metadata_and_payload()
    {
        using var directory = new TempDirectory("snapshot-roundtrip");
        var store = new RaftSnapshotStore(directory.Path);

        var configuration = RaftConfiguration.Of("a", "b", "c").BeginTransitionTo(["b", "c", "d"]);
        var machine = new CountingStateMachine { Payload = "hello snapshot" };

        await store.WriteAsync(new RaftSnapshotMetadata(42, 7, configuration), machine);

        var metadata = store.TryReadMetadata();
        Assert.NotNull(metadata);
        Assert.Equal(42, metadata.Value.LastIncludedIndex);
        Assert.Equal(7, metadata.Value.LastIncludedTerm);

        // The configuration travels with the snapshot, joint state included, because the log
        // entries that established it may have been discarded.
        Assert.True(metadata.Value.Configuration.IsJoint);
        Assert.Equal(["b", "c", "d"], metadata.Value.Configuration.Voters);
        Assert.Equal(["a", "b", "c"], metadata.Value.Configuration.OutgoingVoters);

        var (_, payload) = store.OpenForRestore();
        await using (payload)
        {
            var restored = new CountingStateMachine();
            await restored.RestoreSnapshotAsync(payload, CancellationToken.None);
            Assert.Equal("hello snapshot", restored.Payload);
        }
    }

    /// <summary>A trivial state machine, so the snapshot store can be tested on its own.</summary>
    private sealed class CountingStateMachine : IRaftStateMachine
    {
        internal string Payload { get; set; } = "state";

        public ValueTask ApplyAsync(RaftLogEntry entry, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        public async ValueTask CaptureSnapshotAsync(
            Stream destination,
            CancellationToken cancellationToken)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(Payload);
            await destination.WriteAsync(bytes, cancellationToken);
        }

        public async ValueTask RestoreSnapshotAsync(
            Stream source,
            CancellationToken cancellationToken)
        {
            using var memory = new MemoryStream();
            await source.CopyToAsync(memory, cancellationToken);
            Payload = Encoding.UTF8.GetString(memory.ToArray());
        }
    }
}
