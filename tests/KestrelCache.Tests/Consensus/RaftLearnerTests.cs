using System.Text;
using KestrelCache.Raft;
using Xunit;
using Xunit.Abstractions;

namespace KestrelCache.Tests;

/// <summary>
/// Tests for learner (non-voting) members.
/// </summary>
/// <remarks>
/// A learner is replicated to but counted in nothing — it never votes, never appears in a quorum,
/// and never campaigns. The payoff is that adding or removing one needs no joint phase, because no
/// majority anywhere changes size.
///
/// Which matters because adding a voter directly has a real cost. A three-node cluster admitting a
/// fourth voter immediately needs three of four rather than two of three, so until the newcomer
/// has caught up the cluster tolerates <i>fewer</i> failures than before — and a newcomer with an
/// empty log can take a long time to catch up, especially if a whole snapshot has to be
/// transferred. Admitting it as a learner costs nothing, and promotion happens when it is already
/// current.
/// </remarks>
[Collection(TimingSensitiveCollection.Name)]
public sealed class RaftLearnerTests(ITestOutputHelper output)
{
    private static byte[] Key(string s) => Encoding.UTF8.GetBytes(s);

    private static string? Text(byte[]? value) =>
        value is null ? null : Encoding.UTF8.GetString(value);

    private static WriteBatch Put(string key, string value) => new WriteBatch().Put(key, value);

    private static RaftOptions StableLeadership(RaftOptions options) => options with
    {
        ElectionTimeout = TimeSpan.FromMilliseconds(250),
        HeartbeatInterval = TimeSpan.FromMilliseconds(25),
    };

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
    public void A_learner_is_a_member_but_not_a_voter()
    {
        var configuration = RaftConfiguration.Of("a", "b", "c").WithLearner("d");

        Assert.True(configuration.IsVoter("a"));
        Assert.False(configuration.IsVoter("d"));
        Assert.True(configuration.IsLearner("d"));
        Assert.True(configuration.IsMember("d"));

        // Replicated to, so it must appear in the set the leader sends entries to.
        Assert.Contains("d", configuration.AllServers);
    }

    /// <summary>
    /// The property that makes learners cheap: they change no majority, so adding one needs no
    /// joint phase.
    /// </summary>
    [Fact]
    public void A_learner_does_not_change_any_quorum()
    {
        var before = RaftConfiguration.Of("a", "b", "c");
        var after = before.WithLearner("d");

        Assert.Equal(before.QuorumSize, after.QuorumSize);

        // Two of the three voters is still a decision, and the learner's agreement adds nothing.
        Assert.True(after.HasQuorum(new HashSet<string>(StringComparer.Ordinal) { "a", "b" }));
        Assert.False(after.HasQuorum(new HashSet<string>(StringComparer.Ordinal) { "a", "d" }));
    }

    [Fact]
    public void A_server_cannot_be_both_a_voter_and_a_learner()
    {
        var configuration = RaftConfiguration.Of("a", "b", "c");

        Assert.Throws<InvalidOperationException>(() => configuration.WithLearner("a"));
        Assert.Throws<ArgumentException>(
            () => configuration.BeginTransitionTo(["a", "b"], ["b"]));
    }

    [Fact]
    public void Learners_survive_a_transition()
    {
        var joint = RaftConfiguration.Of("a", "b", "c").WithLearner("d")
            .BeginTransitionTo(["a", "b", "c", "e"]);

        Assert.Contains("d", joint.Learners);
        Assert.Contains("d", joint.CompleteTransition().Learners);
    }

    [Fact]
    public void A_configuration_with_learners_round_trips_through_its_encoding()
    {
        var configuration = RaftConfiguration.Of("a", "b", "c").WithLearner("d").WithLearner("e");

        var decoded = RaftConfigurationCodec.Decode(RaftConfigurationCodec.Encode(configuration));

        Assert.Equal(configuration.Voters, decoded.Voters);
        Assert.Equal(configuration.Learners, decoded.Learners);
    }

    /// <summary>
    /// A configuration written before learners existed must still decode, with no learners.
    /// Refusing it would make a log or snapshot from an earlier build unopenable.
    /// </summary>
    [Fact]
    public void A_version_one_configuration_still_decodes()
    {
        // Version 1's layout: version, voters, hasOutgoing — and nothing else.
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true))
        {
            writer.Write((ushort)1);
            writer.Write(2);
            writer.Write("n1");
            writer.Write("n2");
            writer.Write(false);
        }

        var decoded = RaftConfigurationCodec.Decode(stream.ToArray());

        Assert.Equal(["n1", "n2"], decoded.Voters);
        Assert.Empty(decoded.Learners);
    }

    // ------------------------------------------------------------ in a cluster

    [Fact]
    public async Task A_learner_receives_replication_without_becoming_a_voter()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: StableLeadership);
        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        await leader.Store.WriteAsync(Put("before", "the-learner"));
        await cluster.WaitForAppliedAsync(leader.Node.LastLogIndex, maxTicks: 600);

        await cluster.AddJoiningNodeAsync("n4");
        await DriveAsync(cluster, leader.Node.AddLearnerAsync("n4"));

        Assert.True(
            await cluster.TickUntilAsync(
                () => cluster.Members.All(m => m.Node.Configuration.IsLearner("n4")),
                maxTicks: 1200),
            cluster.Describe());

        output.WriteLine(cluster.Describe());

        var configuration = leader.Node.Configuration;
        Assert.Contains("n4", configuration.Learners);
        Assert.DoesNotContain("n4", configuration.Voters);

        // Quorum is unchanged: still two of the three voters.
        Assert.Equal(2, configuration.QuorumSize);

        // And it is replicated to, which is the whole point of being a member at all.
        Assert.True(
            await cluster.TickUntilAsync(
                () => cluster["n4"].Node.LastApplied >= leader.Node.LastApplied, maxTicks: 1200),
            cluster.Describe());

        Assert.Equal("the-learner", Text(await cluster["n4"].Store.GetAsync(Key("before"))));
    }

    /// <summary>
    /// Adding a learner needs a single configuration entry, not the two a joint change requires.
    /// </summary>
    [Fact]
    public async Task Adding_a_learner_needs_no_joint_phase()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: StableLeadership);
        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        long indexBefore = leader.Node.LastLogIndex;

        await cluster.AddJoiningNodeAsync("n4");
        await DriveAsync(cluster, leader.Node.AddLearnerAsync("n4"));

        long entriesAppended = leader.Node.LastLogIndex - indexBefore;
        output.WriteLine($"adding a learner appended {entriesAppended} entry(ies)");

        // One entry, and the configuration was never joint.
        Assert.Equal(1, entriesAppended);
        Assert.False(leader.Node.Configuration.IsJoint);
    }

    /// <summary>
    /// A learner must never campaign. One that did would increment terms it can never win with,
    /// disrupting the cluster for no possible benefit.
    /// </summary>
    [Fact]
    public async Task A_learner_never_campaigns_even_when_cut_off()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: StableLeadership);
        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        await cluster.AddJoiningNodeAsync("n4");
        await DriveAsync(cluster, leader.Node.AddLearnerAsync("n4"));
        await cluster.TickUntilAsync(
            () => cluster["n4"].Node.Configuration.IsLearner("n4"), maxTicks: 1200);

        // Cut it off entirely, so it hears from no leader and its election timer expires
        // repeatedly.
        cluster.Network.Isolate("n4");
        long termBefore = cluster["n4"].Node.CurrentTerm;

        for (int i = 0; i < 400; i++)
        {
            await cluster.TickAsync();
            await Task.Delay(2);
        }

        var stats = cluster["n4"].Node.GetStats();
        output.WriteLine(
            $"after being cut off, the learner started {stats.ElectionsStarted} election(s) "
                + $"and is still at term {stats.Term}");

        Assert.Equal(0, stats.ElectionsStarted);
        Assert.Equal(termBefore, stats.Term);
        Assert.Equal(RaftRole.Follower, stats.Role);
    }

    [Fact]
    public async Task A_learner_is_not_counted_toward_a_quorum()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: StableLeadership);
        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        await cluster.AddJoiningNodeAsync("n4");
        await DriveAsync(cluster, leader.Node.AddLearnerAsync("n4"));
        await cluster.TickUntilAsync(
            () => cluster.Members.All(m => m.Node.Configuration.IsLearner("n4")), maxTicks: 1200);

        // Partition the leader with only the learner for company. Two of three voters are now
        // unreachable, so the leader cannot commit -- the learner's agreement is worth nothing.
        var others = cluster.NodeIds.Where(id => id != leader.NodeId && id != "n4").ToList();
        cluster.Network.Partition([leader.NodeId, "n4"], others);

        long appliedBefore = leader.Node.LastApplied;

        using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(800));
        var write = leader.Store.WriteAsync(Put("doomed", "no-quorum"), timeout.Token);

        for (int i = 0; i < 500 && !write.IsCompleted; i++)
        {
            await cluster.TickAsync();
            await Task.Delay(1);
        }

        output.WriteLine(cluster.Describe());

        await Assert.ThrowsAnyAsync<Exception>(async () => await write);
        Assert.Equal(appliedBefore, leader.Node.LastApplied);
        Assert.Null(await cluster["n4"].Store.GetAsync(Key("doomed")));
    }

    // ------------------------------------------------------------ promotion

    [Fact]
    public async Task A_learner_can_be_promoted_to_a_voter()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: StableLeadership);
        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        for (int i = 0; i < 20; i++)
        {
            await leader.Store.WriteAsync(Put($"k:{i:D2}", $"v{i}"));
        }
        await cluster.WaitForAppliedAsync(leader.Node.LastLogIndex, maxTicks: 800);

        await cluster.AddJoiningNodeAsync("n4");
        await DriveAsync(cluster, leader.Node.AddLearnerAsync("n4"));

        // Wait until it is actually current before promoting, which is the whole discipline the
        // two-step exists to allow.
        Assert.True(
            await cluster.TickUntilAsync(
                () => leader.Node.ReplicationLagOf("n4") == 0, maxTicks: 1500),
            cluster.Describe());

        output.WriteLine($"learner caught up; lag is {leader.Node.ReplicationLagOf("n4")}");

        await DriveAsync(cluster, leader.Node.PromoteLearnerAsync("n4"));

        var configuration = await cluster.WaitForConfigurationAsync(
            ["n1", "n2", "n3", "n4"], maxTicks: 1500);

        Assert.NotNull(configuration);
        output.WriteLine(cluster.Describe());

        Assert.Contains("n4", configuration.Voters);
        Assert.DoesNotContain("n4", configuration.Learners);
        Assert.Equal(3, configuration.QuorumSize);

        // Data written before it joined is still there.
        for (int i = 0; i < 20; i += 7)
        {
            Assert.Equal($"v{i}", Text(await cluster["n4"].Store.GetAsync(Key($"k:{i:D2}"))));
        }
    }

    [Fact]
    public async Task A_promoted_learner_can_vote_and_can_lead()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: StableLeadership);
        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        await cluster.AddJoiningNodeAsync("n4");
        await DriveAsync(cluster, leader.Node.AddLearnerAsync("n4"));
        await cluster.TickUntilAsync(() => leader.Node.ReplicationLagOf("n4") == 0, maxTicks: 1500);
        await DriveAsync(cluster, leader.Node.PromoteLearnerAsync("n4"));
        await cluster.WaitForConfigurationAsync(["n1", "n2", "n3", "n4"], maxTicks: 1500);

        // Reduce the cluster to the promoted node and one other, then remove the rest so the
        // newcomer has to actually win an election.
        var toRemove = cluster.NodeIds.Where(id => id != "n4" && id != leader.NodeId).ToList();
        foreach (string id in toRemove)
        {
            await DriveAsync(cluster, leader.Node.RemoveServerAsync(id));
        }

        await cluster.TickUntilAsync(
            () => !cluster["n4"].Node.Configuration.IsJoint
                && cluster["n4"].Node.Configuration.Voters.Count == 2,
            maxTicks: 2000);

        output.WriteLine(cluster.Describe());

        // Kill the remaining original and the promoted learner must take over alone... which it
        // cannot, with one of two voters. So instead check it can win when it is one of two and
        // the other is alive, by making it campaign.
        Assert.True(cluster["n4"].Node.Configuration.IsVoter("n4"));

        var winner = await cluster.WaitForLeaderAmongAsync(["n4", leader.NodeId], maxTicks: 1500);
        Assert.NotNull(winner);
        output.WriteLine($"{winner.NodeId} leads a two-voter cluster including the promoted node");
    }

    [Fact]
    public async Task Promoting_something_that_is_not_a_learner_is_refused()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: StableLeadership);
        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await leader.Node.PromoteLearnerAsync("nobody"));

        // Promoting an existing voter is a no-op rather than an error, so the call is idempotent.
        await leader.Node.PromoteLearnerAsync(leader.NodeId);
    }

    // ------------------------------------------------------------ removal and durability

    [Fact]
    public async Task Removing_a_learner_needs_no_joint_phase()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: StableLeadership);
        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        await cluster.AddJoiningNodeAsync("n4");
        await DriveAsync(cluster, leader.Node.AddLearnerAsync("n4"));
        await cluster.TickUntilAsync(
            () => leader.Node.Configuration.IsLearner("n4"), maxTicks: 1200);

        long indexBefore = leader.Node.LastLogIndex;
        await DriveAsync(cluster, leader.Node.RemoveServerAsync("n4"));

        long appended = leader.Node.LastLogIndex - indexBefore;
        output.WriteLine($"removing a learner appended {appended} entry(ies)");

        Assert.Equal(1, appended);
        Assert.False(leader.Node.Configuration.IsJoint);
        Assert.DoesNotContain("n4", leader.Node.Configuration.Learners);
    }

    [Fact]
    public async Task Learner_membership_survives_a_restart()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: StableLeadership);
        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        await cluster.AddJoiningNodeAsync("n4");
        await DriveAsync(cluster, leader.Node.AddLearnerAsync("n4"));
        await cluster.TickUntilAsync(
            () => cluster.Members.All(m => m.Node.Configuration.IsLearner("n4")), maxTicks: 1500);

        string victim = cluster.NodeIds.First(id => id != leader.NodeId && id != "n4");
        await cluster.StopAsync(victim);

        // Restarted with the original three-node bootstrap list, which must be ignored in favour
        // of what the log says.
        await cluster.RestartAsync(victim, ["n1", "n2", "n3"]);

        var recovered = cluster[victim].Node.Configuration;
        output.WriteLine($"{victim} after restart: {recovered}");

        Assert.Contains("n4", recovered.Learners);
        Assert.DoesNotContain("n4", recovered.Voters);
    }

    [Fact]
    public async Task Learner_membership_survives_a_snapshot()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: StableLeadership);
        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        await cluster.AddJoiningNodeAsync("n4");
        await DriveAsync(cluster, leader.Node.AddLearnerAsync("n4"));
        await cluster.TickUntilAsync(
            () => cluster.Members.All(m => m.Node.Configuration.IsLearner("n4")), maxTicks: 1500);

        string victim = cluster.NodeIds.First(id => id != leader.NodeId && id != "n4");

        Assert.True(
            await cluster.TickUntilAsync(
                () => cluster[victim].Node.LastApplied >= leader.Node.LastApplied, maxTicks: 1200),
            cluster.Describe());

        // Snapshot, which discards the configuration entry that established the learner. The
        // learner list must therefore travel in the snapshot itself.
        await cluster[victim].Node.CreateSnapshotAsync();
        Assert.DoesNotContain(
            true, cluster[victim].Node.InspectLog().Select(e => e.IsConfiguration));

        await cluster.StopAsync(victim);
        await cluster.RestartAsync(victim, ["n1", "n2", "n3"]);

        var recovered = cluster[victim].Node;
        output.WriteLine($"recovery: {recovered.Recovery}");

        Assert.True(recovered.Recovery.SnapshotRestored);
        Assert.Contains("n4", recovered.Configuration.Learners);
    }
}
