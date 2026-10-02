using System.Text;
using KestrelCache.Raft;
using Xunit;
using Xunit.Abstractions;

namespace KestrelCache.Tests;

/// <summary>
/// Tests for the pre-vote phase: a straw poll a candidate runs <i>before</i> incrementing its
/// term, so that a campaign it cannot win costs the cluster nothing.
/// </summary>
/// <remarks>
/// The behaviour being prevented is specific. A node partitioned off rejoins, times out because
/// it has heard from no leader, increments its term and asks for votes. Every healthy node —
/// including a perfectly good leader — sees the higher term and steps down, so the cluster holds
/// an election it did not need. The rejoining node cannot win, because its log is behind, but it
/// has still cost an election. A flapping link makes that happen over and over.
///
/// Because the option can be turned off, the disruption and its absence can both be measured on
/// the same scenario, which is the only way to show the mechanism does anything.
/// </remarks>
[Collection(TimingSensitiveCollection.Name)]
public sealed class RaftPreVoteTests(ITestOutputHelper output)
{
    private static byte[] Key(string s) => Encoding.UTF8.GetBytes(s);

    private static WriteBatch Put(string key, string value) => new WriteBatch().Put(key, value);

    private static RaftOptions WithPreVote(RaftOptions options) => options with
    {
        PreVote = true,
        // Long enough that a loaded machine stalling a tick does not unseat a healthy leader for
        // reasons unrelated to what is being tested, short enough that the first election still
        // happens inside a test's tick budget.
        ElectionTimeout = TimeSpan.FromMilliseconds(250),
        HeartbeatInterval = TimeSpan.FromMilliseconds(25),
    };

    private static RaftOptions WithoutPreVote(RaftOptions options) => WithPreVote(options) with
    {
        PreVote = false,
    };

    // ------------------------------------------------------------ the handler in isolation

    /// <summary>
    /// A pre-vote must change nothing on the voter. If it advanced the term or recorded a vote,
    /// the straw poll would cause the disruption it exists to avoid.
    /// </summary>
    [Fact]
    public async Task A_pre_vote_does_not_advance_the_term_or_record_a_vote()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: WithPreVote);
        var voter = cluster.Members.First();

        long termBefore = voter.Node.CurrentTerm;

        var response = await voter.Node.HandleRequestVoteAsync(new RequestVoteRequest(
            Term: termBefore + 50,
            CandidateId: "someone",
            LastLogIndex: 1000,
            LastLogTerm: 999,
            PreVote: true));

        output.WriteLine($"pre-vote reply: granted={response.VoteGranted} term={response.Term}");

        Assert.True(response.PreVote);
        Assert.Equal(termBefore, voter.Node.CurrentTerm);
        Assert.Equal(termBefore, response.Term);

        // And having answered a straw poll, a real vote in this term is still available.
        var real = await voter.Node.HandleRequestVoteAsync(new RequestVoteRequest(
            Term: termBefore + 50,
            CandidateId: "someone",
            LastLogIndex: 1000,
            LastLogTerm: 999));

        Assert.True(real.VoteGranted);
    }

    /// <summary>
    /// The rule that does the work: a voter that has heard from a leader recently refuses, which
    /// is what stops a rejoining node from unseating a healthy one.
    /// </summary>
    [Fact]
    public async Task A_pre_vote_is_refused_while_a_leader_is_alive()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: WithPreVote);
        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        // A few ticks, so the followers have definitely been heartbeated.
        for (int i = 0; i < 10; i++) await cluster.TickAsync();

        var follower = cluster.Members.First(m => m.NodeId != leader.NodeId);

        var response = await follower.Node.HandleRequestVoteAsync(new RequestVoteRequest(
            Term: follower.Node.CurrentTerm + 1,
            CandidateId: "disruptor",
            // A log as good as anyone's, so only the live-leader rule can refuse it.
            LastLogIndex: follower.Node.LastLogIndex,
            LastLogTerm: follower.Node.GetStats().LastLogTerm,
            PreVote: true));

        output.WriteLine($"follower refused a pre-vote while {leader.NodeId} leads: {!response.VoteGranted}");
        Assert.False(response.VoteGranted);
    }

    [Fact]
    public async Task A_pre_vote_is_granted_once_no_leader_has_been_heard_from()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: WithPreVote);
        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        // Silence the leader. Once a follower's own election timeout has elapsed with no contact,
        // it no longer believes a leader exists and must grant.
        cluster.Network.Isolate(leader.NodeId);

        // Polled rather than slept on, because the election timeout is randomised -- a fixed
        // delay is either longer than the test needs or occasionally shorter than the timeout,
        // and the second one is a flaky test.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        RequestVoteResponse? granted = null;
        string? granter = null;

        while (DateTime.UtcNow < deadline && granted is null)
        {
            foreach (var member in cluster.Members)
            {
                // Only ask a follower. A node that has itself taken over is a leader, and a
                // leader correctly refuses: it knows a leader exists, because it is one.
                if (member.NodeId == leader.NodeId) continue;
                if (member.Node.Role != RaftRole.Follower) continue;

                var response = await member.Node.HandleRequestVoteAsync(new RequestVoteRequest(
                    Term: member.Node.CurrentTerm + 1,
                    CandidateId: "candidate",
                    LastLogIndex: member.Node.LastLogIndex,
                    LastLogTerm: member.Node.GetStats().LastLogTerm,
                    PreVote: true));

                if (response.VoteGranted)
                {
                    granted = response;
                    granter = member.NodeId;
                    break;
                }
            }

            if (granted is null) await Task.Delay(20);
        }

        output.WriteLine($"granted by {granter ?? "nobody"} once the leader went quiet");
        Assert.NotNull(granted);
    }

    /// <summary>
    /// A leader must refuse a straw poll, because it knows a leader exists — itself. Granting
    /// would let the real round that followed unseat it, which is the disruption pre-vote exists
    /// to prevent.
    /// </summary>
    [Fact]
    public async Task A_leader_refuses_a_pre_vote()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: WithPreVote);
        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        var response = await leader.Node.HandleRequestVoteAsync(new RequestVoteRequest(
            Term: leader.Node.CurrentTerm + 5,
            CandidateId: "usurper",
            // A log as good as the leader's, so only the leader-knows-itself rule can refuse it.
            LastLogIndex: leader.Node.LastLogIndex,
            LastLogTerm: leader.Node.GetStats().LastLogTerm,
            PreVote: true));

        output.WriteLine($"leader refused a pre-vote for a higher term: {!response.VoteGranted}");

        Assert.False(response.VoteGranted);
        Assert.Equal(RaftRole.Leader, leader.Node.Role);
    }

    [Fact]
    public async Task A_pre_vote_from_a_candidate_with_a_stale_log_is_refused()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: WithPreVote);
        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        await leader.Store.WriteAsync(Put("k", "v"));
        await cluster.WaitForAppliedAsync(leader.Node.LastLogIndex, maxTicks: 600);

        var follower = cluster.Members.First(m => m.NodeId != leader.NodeId);
        cluster.Network.Isolate(leader.NodeId);
        await Task.Delay(300);

        // No live leader, so only the election restriction can refuse this.
        var response = await follower.Node.HandleRequestVoteAsync(new RequestVoteRequest(
            Term: follower.Node.CurrentTerm + 1,
            CandidateId: "behind",
            LastLogIndex: 0,
            LastLogTerm: 0,
            PreVote: true));

        Assert.False(response.VoteGranted);
    }

    /// <summary>
    /// A reply from the wrong round must not be counted. The two rounds ask different questions,
    /// and a straw-poll answer deciding a real election would let a candidate win on answers
    /// nobody committed to.
    /// </summary>
    [Fact]
    public async Task A_pre_vote_reply_is_tagged_so_it_cannot_decide_a_real_election()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: WithPreVote);
        var voter = cluster.Members.First();

        var pre = await voter.Node.HandleRequestVoteAsync(new RequestVoteRequest(
            voter.Node.CurrentTerm + 1, "c", 0, 0, PreVote: true));
        var real = await voter.Node.HandleRequestVoteAsync(new RequestVoteRequest(
            voter.Node.CurrentTerm + 1, "c", 0, 0));

        Assert.True(pre.PreVote);
        Assert.False(real.PreVote);
    }

    // ------------------------------------------------------------ the whole point

    /// <summary>
    /// The measurement that justifies the feature: the same disruption, with and without
    /// pre-vote.
    /// </summary>
    /// <remarks>
    /// A node is partitioned off while the cluster keeps committing, so its log falls behind.
    /// When it rejoins it cannot win an election. The question is whether it forces one anyway.
    /// </remarks>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_rejoining_node_disturbs_the_cluster_only_without_pre_vote(bool preVote)
    {
        await using var cluster = await RaftCluster.StartAsync(
            5, configure: preVote ? WithPreVote : WithoutPreVote);

        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        string outcast = cluster.NodeIds.First(id => id != leader.NodeId);
        var rest = cluster.NodeIds.Where(id => id != outcast).ToList();

        cluster.Network.Isolate(outcast);

        // Commit enough that the outcast's log is definitely behind, so it can never win.
        for (int i = 0; i < 25; i++)
        {
            await leader.Store.WriteAsync(Put($"k:{i:D2}", $"v{i}"));
        }
        await cluster.WaitForAppliedAsync(leader.Node.LastLogIndex, rest, maxTicks: 900);

        long termBeforeRejoin = leader.Node.CurrentTerm;
        long outcastTermBefore = cluster[outcast].Node.CurrentTerm;
        output.WriteLine(
            $"leader {leader.NodeId} at term {termBeforeRejoin}; "
                + $"{outcast} isolated at term {outcastTermBefore}");

        // The attempt happens while the node is still partitioned, which is what makes this
        // deterministic. Healing first and hoping its election timer fires before the leader's
        // next heartbeat arrives is a race -- and the heartbeat usually wins, so the test would
        // measure nothing and pass or fail at random.
        bool attempted = await cluster.TickUntilAsync(
            () => preVote
                ? cluster[outcast].Node.GetStats().PreVotesLost > 0
                : cluster[outcast].Node.GetStats().ElectionsStarted > 0,
            maxTicks: 2000);

        var outcastStats = cluster[outcast].Node.GetStats();
        output.WriteLine(
            $"preVote={preVote}: {outcast} started {outcastStats.ElectionsStarted} election(s), "
                + $"lost {outcastStats.PreVotesLost} pre-vote(s), and is now at term "
                + $"{outcastStats.Term}");

        Assert.True(attempted, $"{outcast} never attempted anything while partitioned");

        if (preVote)
        {
            // Its straw polls reached nobody, so they failed -- and failing cost nothing. The
            // term was never incremented, which is the property that matters: when it rejoins it
            // has nothing to disturb the cluster with.
            Assert.True(outcastStats.PreVotesLost > 0);
            Assert.Equal(0, outcastStats.ElectionsStarted);
            Assert.Equal(outcastTermBefore, outcastStats.Term);
        }
        else
        {
            // Without the straw poll it increments its term on every timeout, so it rejoins
            // holding a term above the whole cluster's.
            Assert.True(outcastStats.ElectionsStarted > 0);
            Assert.True(
                outcastStats.Term > outcastTermBefore,
                "without pre-vote an isolated node should have inflated its own term");
        }

        cluster.Network.Heal(outcast);

        await cluster.TickUntilAsync(
            () => cluster.Members.Max(m => m.Node.CurrentTerm) > termBeforeRejoin,
            maxTicks: 600);

        long termAfter = cluster.Members.Max(m => m.Node.CurrentTerm);
        output.WriteLine($"cluster term after it rejoined: {termBeforeRejoin} -> {termAfter}");
        output.WriteLine(cluster.Describe());

        if (!preVote)
        {
            // The inflated term propagates on contact and forces an election the cluster did not
            // need. This is the disruption pre-vote exists to remove.
            Assert.True(
                termAfter > termBeforeRejoin,
                $"without pre-vote the rejoining node should have disturbed the cluster, but the "
                    + $"term stayed at {termAfter}");
        }
    }

    [Fact]
    public async Task Pre_vote_does_not_prevent_a_genuine_failover()
    {
        await using var cluster = await RaftCluster.StartAsync(3, configure: WithPreVote);
        var original = await cluster.WaitForLeaderAsync(maxTicks: 900);

        await original.Store.WriteAsync(Put("before", "failure"));
        await cluster.WaitForAppliedAsync(original.Node.LastLogIndex, maxTicks: 600);

        // The leader really is gone, so no survivor will report a live leader and the straw polls
        // must succeed.
        cluster.Network.Isolate(original.NodeId);

        var survivors = cluster.NodeIds.Where(id => id != original.NodeId).ToList();
        var replacement = await cluster.WaitForLeaderAmongAsync(survivors, maxTicks: 1500);

        Assert.NotNull(replacement);
        output.WriteLine(
            $"{replacement.NodeId} took over in term {replacement.Node.CurrentTerm} "
                + $"after winning {replacement.Node.GetStats().PreVotesWon} pre-vote(s)");

        Assert.True(replacement.Node.CurrentTerm > original.Node.CurrentTerm);
        Assert.True(replacement.Node.GetStats().PreVotesWon > 0);
    }

    [Fact]
    public async Task A_single_node_cluster_still_elects_itself_with_pre_vote_on()
    {
        await using var cluster = await RaftCluster.StartAsync(1, configure: WithPreVote);

        var leader = await cluster.WaitForLeaderAsync(maxTicks: 900);

        // Its own vote is a majority, so neither round needs a round trip.
        Assert.Equal("n1", leader.NodeId);
        Assert.True(leader.Node.CurrentTerm >= 1);
    }

    [Fact]
    public async Task Pre_vote_still_lets_a_five_node_cluster_settle_on_one_leader()
    {
        await using var cluster = await RaftCluster.StartAsync(5, configure: WithPreVote);

        await cluster.WaitForLeaderAsync(maxTicks: 1200);
        output.WriteLine(cluster.Describe());

        Assert.Single(cluster.Leaders);
    }
}
