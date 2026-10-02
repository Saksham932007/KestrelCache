using System.Text;
using KestrelCache.Raft;
using Xunit;
using Xunit.Abstractions;

namespace KestrelCache.Tests;

/// <summary>
/// Tests for cluster membership changes through joint consensus.
/// </summary>
/// <remarks>
/// The property worth protecting is that no two disjoint majorities can ever exist. Changing
/// configuration one node at a time would break it — three nodes becoming five admits
/// {A,B} as a majority of the old set and {C,D,E} as a majority of the new, so two leaders could
/// be elected in the same term. The joint phase, which demands a majority of both sets, is what
/// rules that out, and these tests exercise it rather than taking it on trust.
/// </remarks>
[Collection("raft-cluster")]
public sealed class RaftMembershipTests(ITestOutputHelper output)
{
    private static byte[] Key(string s) => Encoding.UTF8.GetBytes(s);

    private static string? Text(byte[]? value) =>
        value is null ? null : Encoding.UTF8.GetString(value);

    private static WriteBatch Put(string key, string value) => new WriteBatch().Put(key, value);

    /// <summary>
    /// Leadership stable enough that a multi-phase membership change completes under one leader.
    /// </summary>
    private static RaftOptions StableLeadership(RaftOptions options) => options with
    {
        ElectionTimeout = TimeSpan.FromMilliseconds(250),
        HeartbeatInterval = TimeSpan.FromMilliseconds(25),
    };

    /// <summary>
    /// Drives the cluster while <paramref name="work"/> runs, since a membership change needs
    /// two entries to commit and commitment only advances on a tick.
    /// </summary>
    private static async Task DriveAsync(RaftCluster cluster, Task work, int maxTicks = 2000)
    {
        for (int i = 0; i < maxTicks && !work.IsCompleted; i++)
        {
            await cluster.TickAsync();
            await Task.Delay(1);
        }

        await work;
    }

    // ------------------------------------------------------------ the configuration type

    [Fact]
    public void A_simple_configuration_needs_a_simple_majority()
    {
        var configuration = RaftConfiguration.Of("a", "b", "c");

        Assert.False(configuration.HasQuorum(Set("a")));
        Assert.True(configuration.HasQuorum(Set("a", "b")));
        Assert.True(configuration.HasQuorum(Set("a", "b", "c")));
    }

    /// <summary>
    /// The heart of joint consensus: during a transition a decision needs a majority of the old
    /// configuration <i>and</i> of the new one. Either alone is not enough, which is precisely
    /// what prevents two disjoint majorities.
    /// </summary>
    [Fact]
    public void A_joint_configuration_needs_a_majority_of_both()
    {
        var joint = RaftConfiguration.Of("a", "b", "c").BeginTransitionTo(["c", "d", "e"]);

        Assert.True(joint.IsJoint);

        // A majority of the old set only.
        Assert.False(joint.HasQuorum(Set("a", "b")));

        // A majority of the new set only.
        Assert.False(joint.HasQuorum(Set("d", "e")));

        // Both.
        Assert.True(joint.HasQuorum(Set("a", "b", "c", "d")));
        Assert.True(joint.HasQuorum(Set("b", "c", "d")));
    }

    /// <summary>
    /// The scenario the joint phase exists to prevent, stated as a test: no two sets that each
    /// satisfy the joint rule can be disjoint.
    /// </summary>
    [Fact]
    public void Two_sets_that_both_satisfy_a_joint_configuration_must_overlap()
    {
        var joint = RaftConfiguration.Of("a", "b", "c").BeginTransitionTo(["a", "b", "c", "d", "e"]);

        var everyone = new[] { "a", "b", "c", "d", "e" };
        var satisfying = new List<HashSet<string>>();

        // Enumerate every subset and keep the ones that constitute a decision.
        for (int mask = 0; mask < 1 << 5; mask++)
        {
            var subset = new HashSet<string>(StringComparer.Ordinal);
            for (int bit = 0; bit < 5; bit++)
            {
                if ((mask & (1 << bit)) != 0) subset.Add(everyone[bit]);
            }

            if (joint.HasQuorum(subset)) satisfying.Add(subset);
        }

        output.WriteLine($"{satisfying.Count} subset(s) satisfy the joint rule");
        Assert.NotEmpty(satisfying);

        foreach (var first in satisfying)
        {
            foreach (var second in satisfying)
            {
                Assert.True(
                    first.Overlaps(second),
                    $"[{string.Join(",", first)}] and [{string.Join(",", second)}] are disjoint "
                        + "yet both satisfy the joint rule, which would permit two leaders");
            }
        }
    }

    [Fact]
    public void An_empty_configuration_has_no_majority()
    {
        // A joining server starts with no voters. Treating that as trivially satisfied would let
        // it decide it had won an election before it knew the cluster existed.
        Assert.False(RaftConfiguration.Empty.HasQuorum(Set("a", "b", "c")));
    }

    [Fact]
    public void A_second_change_cannot_start_while_one_is_in_flight()
    {
        var joint = RaftConfiguration.Of("a", "b", "c").BeginTransitionTo(["a", "b"]);

        Assert.Throws<InvalidOperationException>(() => joint.BeginTransitionTo(["a"]));
    }

    [Fact]
    public void A_configuration_round_trips_through_its_encoding()
    {
        var joint = RaftConfiguration.Of("a", "b", "c").BeginTransitionTo(["c", "d"]);

        var decoded = RaftConfigurationCodec.Decode(RaftConfigurationCodec.Encode(joint));

        Assert.Equal(joint.Voters, decoded.Voters);
        Assert.Equal(joint.OutgoingVoters, decoded.OutgoingVoters);
        Assert.True(decoded.IsJoint);
    }

    [Fact]
    public void A_truncated_configuration_is_rejected()
    {
        byte[] encoded = RaftConfigurationCodec.Encode(RaftConfiguration.Of("a", "b"));

        Assert.ThrowsAny<Exception>(
            () => RaftConfigurationCodec.Decode(encoded.AsSpan(0, encoded.Length - 3)));
    }

    // ------------------------------------------------------------ adding a server

    [Fact]
    public async Task A_server_can_be_added_to_a_running_cluster()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: StableLeadership);
        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        await leader.Store.WriteAsync(Put("before", "the-change"));
        await cluster.WaitForAppliedAsync(leader.Node.LastLogIndex, maxTicks: 600);

        // The new server comes up with no configuration of its own, so it cannot campaign and
        // waits to learn the membership from the leader.
        await cluster.AddJoiningNodeAsync("n4");
        Assert.Empty(cluster["n4"].Node.Configuration.Voters);

        await DriveAsync(cluster, leader.Node.AddServerAsync("n4"));

        output.WriteLine(cluster.Describe());

        var configuration = await cluster.WaitForConfigurationAsync(["n1", "n2", "n3", "n4"]);
        Assert.NotNull(configuration);
        Assert.False(configuration.IsJoint);
        Assert.Equal(4, configuration.Voters.Count);

        // And the newcomer catches up on everything written before it joined.
        Assert.True(
            await cluster.TickUntilAsync(
                () => cluster["n4"].Node.LastApplied >= leader.Node.LastApplied, maxTicks: 1200),
            cluster.Describe());

        Assert.Equal("the-change", Text(await cluster["n4"].Store.GetAsync(Key("before"))));
    }

    [Fact]
    public async Task A_newly_added_server_receives_subsequent_writes()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: StableLeadership);
        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        await cluster.AddJoiningNodeAsync("n4");
        await DriveAsync(cluster, leader.Node.AddServerAsync("n4"));
        await cluster.WaitForConfigurationAsync(["n1", "n2", "n3", "n4"]);

        await DriveAsync(cluster, leader.Store.WriteAsync(Put("after", "joining")));

        Assert.True(
            await cluster.WaitForAppliedAsync(leader.Node.LastLogIndex, maxTicks: 1200),
            cluster.Describe());

        foreach (var member in cluster.Members)
        {
            Assert.Equal("joining", Text(await member.Store.GetAsync(Key("after"))));
        }
    }

    /// <summary>
    /// A server added to a cluster whose leader has already discarded its log prefix must be
    /// caught up with a snapshot, because the entries it needs no longer exist.
    /// </summary>
    [Fact]
    public async Task A_server_added_after_a_snapshot_is_caught_up_with_one()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: StableLeadership);
        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        for (int i = 0; i < 40; i++)
        {
            await leader.Store.WriteAsync(Put($"k:{i:D3}", $"v{i}"));
        }
        await cluster.WaitForAppliedAsync(leader.Node.LastLogIndex, maxTicks: 800);

        // Every existing node discards its prefix, so whichever one leads must transfer state.
        foreach (string id in cluster.NodeIds.ToList())
        {
            await cluster[id].Node.CreateSnapshotAsync();
        }

        await cluster.AddJoiningNodeAsync("n4");
        await DriveAsync(cluster, leader.Node.AddServerAsync("n4"));
        await cluster.WaitForConfigurationAsync(["n1", "n2", "n3", "n4"], maxTicks: 1500);

        Assert.True(
            await cluster.TickUntilAsync(
                () => cluster["n4"].Node.GetStats().SnapshotsInstalled > 0, maxTicks: 3000),
            cluster.Describe());

        output.WriteLine(cluster.Describe());

        for (int i = 0; i < 40; i++)
        {
            Assert.Equal($"v{i}", Text(await cluster["n4"].Store.GetAsync(Key($"k:{i:D3}"))));
        }
    }

    // ------------------------------------------------------------ removing a server

    [Fact]
    public async Task A_server_can_be_removed_from_a_running_cluster()
    {
        await using var cluster = await RaftCluster.StartAsync(5, configure: StableLeadership);
        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        string departing = cluster.NodeIds.First(id => id != leader.NodeId);
        output.WriteLine($"removing {departing}, leader is {leader.NodeId}");

        await DriveAsync(cluster, leader.Node.RemoveServerAsync(departing));

        var remaining = cluster.NodeIds.Where(id => id != departing).ToList();

        await cluster.TickUntilAsync(
            () => remaining.All(id => !cluster[id].Node.Configuration.IsJoint
                && !cluster[id].Node.Configuration.ContainsIncoming(departing)),
            maxTicks: 1500);

        output.WriteLine(cluster.Describe());

        foreach (string id in remaining)
        {
            var configuration = cluster[id].Node.Configuration;
            Assert.False(configuration.IsJoint);
            Assert.DoesNotContain(departing, configuration.Voters);
        }

        // The smaller cluster keeps working.
        await DriveAsync(cluster, leader.Store.WriteAsync(Put("after", "removal")));

        Assert.True(
            await cluster.WaitForAppliedAsync(leader.Node.LastLogIndex, remaining, maxTicks: 1200),
            cluster.Describe());
    }

    /// <summary>
    /// A leader that removes itself must step aside: it can no longer count itself toward a
    /// quorum, so continuing to lead would stall every subsequent decision.
    /// </summary>
    [Fact]
    public async Task A_leader_that_removes_itself_steps_down()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: StableLeadership);
        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        output.WriteLine($"{leader.NodeId} is removing itself");
        await DriveAsync(cluster, leader.Node.RemoveServerAsync(leader.NodeId));

        Assert.True(
            await cluster.TickUntilAsync(
                () => leader.Node.Role != RaftRole.Leader, maxTicks: 1200),
            cluster.Describe());

        output.WriteLine(cluster.Describe());
        Assert.NotEqual(RaftRole.Leader, leader.Node.Role);

        // And one of the remaining two takes over.
        var remaining = cluster.NodeIds.Where(id => id != leader.NodeId).ToList();
        var replacement = await cluster.WaitForLeaderAmongAsync(remaining, maxTicks: 1500);

        Assert.NotNull(replacement);
        output.WriteLine($"{replacement.NodeId} now leads");
    }

    [Fact]
    public async Task Removing_the_last_voter_is_refused()
    {
        await using var cluster = await RaftCluster.StartAsync(1, configure: StableLeadership);
        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await leader.Node.RemoveServerAsync(leader.NodeId));
    }

    // ------------------------------------------------------------ safety during a change

    /// <summary>
    /// During the joint phase a decision needs both majorities, so a partition holding a
    /// majority of only one of them must not be able to commit.
    /// </summary>
    /// <remarks>
    /// The configuration matters for this to test anything. Growing {n1,n2,n3} to
    /// {n1,n2,n3,n4,n5} does <i>not</i> work as a demonstration: three of five is a majority, so
    /// the reachable nodes satisfy both rules and the change legitimately commits. Replacing two
    /// nodes — {n1,n2,n3} to {n1,n4,n5} — is the case where the old majority is reachable and the
    /// new one is not, so the joint entry genuinely cannot commit.
    /// </remarks>
    [Fact]
    public async Task A_change_cannot_commit_without_a_majority_of_both_configurations()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: StableLeadership);
        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        // The change has to be driven by a node that stays in the new configuration, so pick the
        // leader's own identity as the one surviving member.
        string stays = leader.NodeId;
        var replaced = cluster.NodeIds.Where(id => id != stays).ToList();

        await cluster.AddJoiningNodeAsync("n4");
        await cluster.AddJoiningNodeAsync("n5");

        // Cut the incoming servers off: the leader can reach a majority of the old configuration
        // but only one of the three voters in the new one.
        cluster.Network.Partition(["n4", "n5"], [.. cluster.NodeIds.Where(id => id is not ("n4" or "n5"))]);

        var target = new[] { stays, "n4", "n5" };
        output.WriteLine($"changing [{string.Join(",", cluster.NodeIds)}] -> [{string.Join(",", target)}]");

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var change = leader.Node.ChangeMembershipAsync(target, timeout.Token);

        for (int i = 0; i < 800 && !change.IsCompleted; i++)
        {
            await cluster.TickAsync();
            await Task.Delay(2);
        }

        output.WriteLine(cluster.Describe());

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await change);

        // Left in the joint configuration, which is the safe place to be stuck: it still demands
        // both majorities, so neither side can decide anything on its own.
        Assert.True(leader.Node.Configuration.IsJoint);
        Assert.Contains("n4", leader.Node.Configuration.Voters);
        Assert.Contains(replaced[0], leader.Node.Configuration.OutgoingVoters!);

        // Healing lets the transition finish. Only the servers that remain voters are checked:
        // a removed server legitimately stops receiving entries once it has acknowledged the one
        // that removed it, so it is left holding the joint configuration and that is correct.
        cluster.Network.HealAll();

        Assert.True(
            await cluster.TickUntilAsync(
                () => target.All(id => !cluster[id].Node.Configuration.IsJoint
                    && cluster[id].Node.Configuration.Voters.Count == target.Length),
                maxTicks: 3000),
            cluster.Describe());

        output.WriteLine("after healing:");
        output.WriteLine(cluster.Describe());

        foreach (string id in target)
        {
            Assert.Equal(
                target.OrderBy(v => v, StringComparer.Ordinal),
                cluster[id].Node.Configuration.Voters.OrderBy(v => v, StringComparer.Ordinal));
        }
    }

    /// <summary>
    /// A configuration takes effect when it is appended, not when it commits. Waiting for
    /// commitment would mean counting quorums under a membership the node had already
    /// superseded.
    /// </summary>
    /// <remarks>
    /// Shown by arranging for the joint entry to be unable to commit — the incoming voters are
    /// unreachable — and then observing that the leader is nonetheless already operating under
    /// the joint configuration.
    /// </remarks>
    [Fact]
    public async Task A_configuration_takes_effect_as_soon_as_it_is_appended()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: StableLeadership);
        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        await cluster.AddJoiningNodeAsync("n4");
        await cluster.AddJoiningNodeAsync("n5");

        cluster.Network.Partition(["n4", "n5"], [.. cluster.NodeIds.Where(id => id is not ("n4" or "n5"))]);

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        var change = leader.Node.ChangeMembershipAsync(
            [leader.NodeId, "n4", "n5"], timeout.Token);

        for (int i = 0; i < 600 && !change.IsCompleted; i++)
        {
            await cluster.TickAsync();
            await Task.Delay(1);
        }

        try
        {
            await change;
        }
        catch (OperationCanceledException)
        {
            // Expected: the joint entry cannot commit while the incoming voters are unreachable.
        }

        var configuration = leader.Node.Configuration;
        output.WriteLine($"leader configuration while uncommitted: {configuration}");

        // Appended, so already in force, even though it has not committed.
        Assert.True(configuration.IsJoint);
        Assert.Contains("n4", configuration.Voters);
        Assert.Contains("n5", configuration.Voters);
    }

    // ------------------------------------------------------------ durability

    [Fact]
    public async Task Membership_survives_a_restart()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: StableLeadership);
        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        await cluster.AddJoiningNodeAsync("n4");
        await DriveAsync(cluster, leader.Node.AddServerAsync("n4"));
        await cluster.WaitForConfigurationAsync(["n1", "n2", "n3", "n4"], maxTicks: 1500);

        string victim = cluster.NodeIds.First(id => id != leader.NodeId);
        var before = cluster[victim].Node.Configuration;
        output.WriteLine($"{victim} before restart: {before}");

        await cluster.StopAsync(victim);

        // Restarted with the *original* three-node bootstrap list, which must be ignored in
        // favour of what the log says. A node that trusted its configuration file over its log
        // would come back up with a stale view of the voters, and a stale view of the voters is
        // how two majorities come to exist at once.
        await cluster.RestartAsync(victim, ["n1", "n2", "n3"]);

        var after = cluster[victim].Node.Configuration;
        output.WriteLine($"{victim} after restart:  {after}");

        Assert.Equal(4, after.Voters.Count);
        Assert.Contains("n4", after.Voters);
    }

    [Fact]
    public async Task Membership_survives_a_snapshot()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: StableLeadership);
        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        await cluster.AddJoiningNodeAsync("n4");
        await DriveAsync(cluster, leader.Node.AddServerAsync("n4"));
        await cluster.WaitForConfigurationAsync(["n1", "n2", "n3", "n4"], maxTicks: 1500);

        string victim = cluster.NodeIds.First(id => id != leader.NodeId);

        // The victim must have applied the configuration entries before snapshotting, or the
        // snapshot would not cover them and the test would prove nothing.
        Assert.True(
            await cluster.TickUntilAsync(
                () => cluster[victim].Node.LastApplied >= leader.Node.LastApplied,
                maxTicks: 1200),
            cluster.Describe());

        // Snapshot, which discards the log entries that established the membership. The
        // configuration must therefore travel in the snapshot itself.
        await cluster[victim].Node.CreateSnapshotAsync();

        Assert.DoesNotContain(
            true,
            cluster[victim].Node.InspectLog().Select(entry => entry.IsConfiguration));

        await cluster.StopAsync(victim);
        await cluster.RestartAsync(victim, ["n1", "n2", "n3"]);

        var recovered = cluster[victim].Node;
        output.WriteLine($"recovery: {recovered.Recovery}");

        Assert.True(recovered.Recovery.SnapshotRestored);
        Assert.Equal(4, recovered.Configuration.Voters.Count);
        Assert.Contains("n4", recovered.Configuration.Voters);
    }

    private static HashSet<string> Set(params string[] values) =>
        new(values, StringComparer.Ordinal);
}
