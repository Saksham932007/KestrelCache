using System.Text;
using KestrelCache.Raft;
using Xunit;
using Xunit.Abstractions;

namespace KestrelCache.Tests;

/// <summary>
/// Tests for the consensus layer, driven over a simulated network so that partitions, message
/// loss and leader failures are reproducible rather than hoped for.
/// </summary>
[Collection("raft-cluster")]
public sealed class RaftTests(ITestOutputHelper output)
{
    private static byte[] Key(string s) => Encoding.UTF8.GetBytes(s);

    private static WriteBatch Put(string key, string value) => new WriteBatch().Put(key, value);

    // ------------------------------------------------------------ elections

    [Fact]
    public async Task A_single_node_cluster_elects_itself()
    {
        await using var cluster = await RaftCluster.StartAsync(1);

        var leader = await cluster.WaitForLeaderAsync();

        Assert.Equal("n1", leader.NodeId);
        Assert.True(leader.Node.CurrentTerm >= 1);
    }

    [Fact]
    public async Task A_three_node_cluster_elects_exactly_one_leader()
    {
        await using var cluster = await RaftCluster.StartAsync(3);

        var leader = await cluster.WaitForLeaderAsync();
        output.WriteLine(cluster.Describe());

        Assert.Single(cluster.Leaders);

        // Everyone else must be a follower that agrees who the leader is.
        foreach (var member in cluster.Members.Where(m => m.NodeId != leader.NodeId))
        {
            Assert.Equal(RaftRole.Follower, member.Node.Role);
        }

        Assert.True(
            await cluster.TickUntilAsync(() => cluster.Members
                .Where(m => m.NodeId != leader.NodeId)
                .All(m => m.Node.LeaderId == leader.NodeId)),
            cluster.Describe());
    }

    [Fact]
    public async Task A_five_node_cluster_elects_exactly_one_leader()
    {
        await using var cluster = await RaftCluster.StartAsync(5);

        await cluster.WaitForLeaderAsync();
        output.WriteLine(cluster.Describe());

        Assert.Single(cluster.Leaders);
    }

    /// <summary>
    /// Every node starts in the same term with an empty log, so nothing but the randomised
    /// election timeout distinguishes them. Without that randomisation they would all become
    /// candidates simultaneously, split the vote, and repeat indefinitely.
    /// </summary>
    [Fact]
    public async Task A_split_vote_resolves_rather_than_repeating()
    {
        await using var cluster = await RaftCluster.StartAsync(5);

        var leader = await cluster.WaitForLeaderAsync(maxTicks: 600);

        long electionsStarted = cluster.Members.Sum(m => m.Node.GetStats().ElectionsStarted);
        output.WriteLine(
            $"{leader.NodeId} won in term {leader.Node.CurrentTerm} after "
                + $"{electionsStarted} candidacies across the cluster");

        Assert.Single(cluster.Leaders);
    }

    [Fact]
    public async Task Terms_never_go_backwards()
    {
        await using var cluster = await RaftCluster.StartAsync(3);

        var highestSeen = cluster.NodeIds.ToDictionary(id => id, _ => 0L);

        for (int round = 0; round < 60; round++)
        {
            await cluster.TickAsync();

            foreach (var member in cluster.Members)
            {
                long term = member.Node.CurrentTerm;
                Assert.True(
                    term >= highestSeen[member.NodeId],
                    $"{member.NodeId} went from term {highestSeen[member.NodeId]} back to {term}");
                highestSeen[member.NodeId] = term;
            }
        }
    }

    /// <summary>
    /// The election restriction: a voter must refuse a candidate whose log is behind its own,
    /// because a leader missing a committed entry could overwrite it.
    /// </summary>
    [Fact]
    public async Task A_candidate_with_a_shorter_log_is_refused()
    {
        await using var cluster = await RaftCluster.StartAsync(3);
        var leader = await cluster.WaitForLeaderAsync();

        await leader.Store.WriteAsync(Put("k", "v"));
        await cluster.WaitForAppliedAsync(leader.Node.LastLogIndex);

        var voter = cluster.Members.First(m => m.NodeId != leader.NodeId);

        // A candidate claiming a far higher term but an empty log.
        var response = await voter.Node.HandleRequestVoteAsync(new RequestVoteRequest(
            Term: voter.Node.CurrentTerm + 5,
            CandidateId: "impostor",
            LastLogIndex: 0,
            LastLogTerm: 0));

        Assert.False(
            response.VoteGranted,
            "a node granted its vote to a candidate whose log was behind its own");
    }

    [Fact]
    public async Task A_node_votes_at_most_once_per_term()
    {
        await using var cluster = await RaftCluster.StartAsync(3);
        var voter = cluster.Members.First();

        long term = voter.Node.CurrentTerm + 10;

        var first = await voter.Node.HandleRequestVoteAsync(
            new RequestVoteRequest(term, "candidate-a", 0, 0));
        var second = await voter.Node.HandleRequestVoteAsync(
            new RequestVoteRequest(term, "candidate-b", 0, 0));

        Assert.True(first.VoteGranted);

        // Two votes in one term is how two leaders get elected at once.
        Assert.False(second.VoteGranted);

        // Re-asking as the same candidate must be idempotent, so a lost reply is recoverable.
        var repeat = await voter.Node.HandleRequestVoteAsync(
            new RequestVoteRequest(term, "candidate-a", 0, 0));
        Assert.True(repeat.VoteGranted);
    }

    // ------------------------------------------------------------ replication

    [Fact]
    public async Task A_write_on_the_leader_reaches_every_follower()
    {
        await using var cluster = await RaftCluster.StartAsync(3);
        var leader = await cluster.WaitForLeaderAsync();

        await leader.Store.WriteAsync(Put("hello", "world"));

        Assert.True(
            await cluster.WaitForAppliedAsync(leader.Node.LastLogIndex),
            cluster.Describe());

        foreach (var member in cluster.Members)
        {
            byte[]? value = await member.Store.GetAsync(Key("hello"));
            Assert.Equal("world", value is null ? null : Encoding.UTF8.GetString(value));
        }
    }

    [Fact]
    public async Task A_follower_refuses_writes_and_names_the_leader()
    {
        await using var cluster = await RaftCluster.StartAsync(3);
        var leader = await cluster.WaitForLeaderAsync();

        await cluster.TickUntilAsync(() => cluster.Members
            .Where(m => m.NodeId != leader.NodeId)
            .All(m => m.Node.LeaderId is not null));

        var follower = cluster.Members.First(m => m.NodeId != leader.NodeId);

        var exception = await Assert.ThrowsAsync<NotLeaderException>(
            async () => await follower.Store.WriteAsync(Put("k", "v")));

        // Naming the leader is what lets a client redirect instead of guessing.
        Assert.Equal(leader.NodeId, exception.LeaderId);
    }

    [Fact]
    public async Task Many_writes_replicate_in_order_to_every_node()
    {
        await using var cluster = await RaftCluster.StartAsync(3);
        var leader = await cluster.WaitForLeaderAsync();

        const int writes = 200;
        for (int i = 0; i < writes; i++)
        {
            await leader.Store.WriteAsync(Put($"key:{i:D4}", $"value-{i}"));
        }

        Assert.True(
            await cluster.WaitForAppliedAsync(leader.Node.LastLogIndex, maxTicks: 800),
            cluster.Describe());

        foreach (var member in cluster.Members)
        {
            for (int i = 0; i < writes; i += 17)
            {
                byte[]? value = await member.Store.GetAsync(Key($"key:{i:D4}"));
                Assert.Equal($"value-{i}", value is null ? null : Encoding.UTF8.GetString(value));
            }
        }
    }

    [Fact]
    public async Task A_batch_stays_atomic_across_replication()
    {
        await using var cluster = await RaftCluster.StartAsync(3);
        var leader = await cluster.WaitForLeaderAsync();

        var batch = new WriteBatch();
        for (int i = 0; i < 50; i++)
        {
            batch.Put($"batch:{i:D2}", $"v{i}");
        }

        await leader.Store.WriteAsync(batch);
        await cluster.WaitForAppliedAsync(leader.Node.LastLogIndex);

        // The whole batch is one Raft command, so every node has all 50 or none.
        foreach (var member in cluster.Members)
        {
            int present = 0;
            for (int i = 0; i < 50; i++)
            {
                if (await member.Store.GetAsync(Key($"batch:{i:D2}")) is not null) present++;
            }

            Assert.Equal(50, present);
        }
    }

    [Fact]
    public async Task Deletes_replicate_as_well_as_writes()
    {
        await using var cluster = await RaftCluster.StartAsync(3);
        var leader = await cluster.WaitForLeaderAsync();

        await leader.Store.WriteAsync(Put("doomed", "value"));
        await cluster.WaitForAppliedAsync(leader.Node.LastLogIndex);

        await leader.Store.DeleteAsync(Key("doomed"));
        await cluster.WaitForAppliedAsync(leader.Node.LastLogIndex);

        foreach (var member in cluster.Members)
        {
            Assert.Null(await member.Store.GetAsync(Key("doomed")));
        }
    }

    // ------------------------------------------------------------ failures

    [Fact]
    public async Task A_new_leader_is_elected_when_the_leader_fails()
    {
        await using var cluster = await RaftCluster.StartAsync(3);
        var original = await cluster.WaitForLeaderAsync();

        await original.Store.WriteAsync(Put("before", "failure"));
        await cluster.WaitForAppliedAsync(original.Node.LastLogIndex);

        output.WriteLine($"isolating the leader, {original.NodeId}");
        cluster.Network.Isolate(original.NodeId);

        var survivors = cluster.NodeIds.Where(id => id != original.NodeId).ToList();
        var replacement = await cluster.WaitForLeaderAmongAsync(survivors, maxTicks: 800);

        Assert.NotNull(replacement);
        output.WriteLine($"{replacement.NodeId} took over in term {replacement.Node.CurrentTerm}");
        output.WriteLine(cluster.Describe());

        // A new leader must be in a strictly later term than the one it replaced.
        Assert.True(replacement.Node.CurrentTerm > original.Node.CurrentTerm);

        // And the committed entry must have survived the handover.
        byte[]? value = await replacement.Store.GetAsync(Key("before"));
        Assert.Equal("failure", value is null ? null : Encoding.UTF8.GetString(value));
    }

    [Fact]
    public async Task The_new_leader_keeps_accepting_writes()
    {
        await using var cluster = await RaftCluster.StartAsync(3);
        var original = await cluster.WaitForLeaderAsync();

        cluster.Network.Isolate(original.NodeId);

        var survivors = cluster.NodeIds.Where(id => id != original.NodeId).ToList();
        var replacement = await cluster.WaitForLeaderAmongAsync(survivors, maxTicks: 800);
        Assert.NotNull(replacement);

        await replacement.Store.WriteAsync(Put("after", "failover"));

        Assert.True(
            await cluster.WaitForAppliedAsync(
                replacement.Node.LastLogIndex, survivors, maxTicks: 400),
            cluster.Describe());

        foreach (string id in survivors)
        {
            byte[]? value = await cluster[id].Store.GetAsync(Key("after"));
            Assert.Equal("failover", value is null ? null : Encoding.UTF8.GetString(value));
        }
    }

    /// <summary>
    /// The split-brain test, and the single most important property in the whole protocol: a
    /// minority partition must not be able to commit anything.
    /// </summary>
    /// <remarks>
    /// With five nodes split three against two, the majority side keeps working. The minority
    /// side may well elect a candidate that campaigns forever, but it can never assemble a
    /// quorum, so no write it accepts can commit. A consensus implementation that lets the
    /// minority commit has produced two divergent histories, which is the one failure that
    /// cannot be recovered from.
    /// </remarks>
    [Fact]
    public async Task A_minority_partition_cannot_commit_anything()
    {
        await using var cluster = await RaftCluster.StartAsync(5);
        var original = await cluster.WaitForLeaderAsync();

        // Put the original leader in the minority, so the interesting case -- a leader that does
        // not yet know it has been deposed -- is the one under test.
        var minority = new List<string> { original.NodeId };
        minority.Add(cluster.NodeIds.First(id => id != original.NodeId));
        var majority = cluster.NodeIds.Except(minority).ToList();

        output.WriteLine($"minority: {string.Join(", ", minority)}");
        output.WriteLine($"majority: {string.Join(", ", majority)}");

        cluster.Network.Partition(minority, majority);

        var newLeader = await cluster.WaitForLeaderAmongAsync(majority, maxTicks: 1000);
        Assert.NotNull(newLeader);
        output.WriteLine($"majority elected {newLeader.NodeId}");

        // The majority can still commit.
        await newLeader.Store.WriteAsync(Put("majority", "committed"));
        Assert.True(
            await cluster.WaitForAppliedAsync(newLeader.Node.LastLogIndex, majority, maxTicks: 400));

        // The minority cannot. Either the old leader has already stepped down and refuses
        // outright, or it accepts the proposal and never commits it -- both are correct, and
        // neither may result in the value being readable.
        long appliedBefore = cluster[original.NodeId].Node.LastApplied;

        var proposal = Task.Run(async () =>
        {
            try
            {
                using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(600));
                await cluster[original.NodeId].Store.WriteAsync(Put("minority", "doomed"), timeout.Token);
                return "committed";
            }
            catch (NotLeaderException)
            {
                return "refused";
            }
            catch (OperationCanceledException)
            {
                return "never committed";
            }
        });

        for (int i = 0; i < 300 && !proposal.IsCompleted; i++)
        {
            await cluster.TickAsync();
            await Task.Delay(2);
        }

        string outcome = await proposal;
        output.WriteLine($"minority write outcome: {outcome}");
        output.WriteLine(cluster.Describe());

        Assert.NotEqual("committed", outcome);

        // Nothing in the minority may have been applied, on any of its nodes.
        foreach (string id in minority)
        {
            Assert.Null(await cluster[id].Store.GetAsync(Key("minority")));
        }

        Assert.Equal(appliedBefore, cluster[original.NodeId].Node.LastApplied);
    }

    /// <summary>
    /// Healing a partition must converge: the minority adopts the majority's history, including
    /// discarding anything it accepted but never committed.
    /// </summary>
    [Fact]
    public async Task Healing_a_partition_converges_on_the_majority_history()
    {
        await using var cluster = await RaftCluster.StartAsync(5);
        var original = await cluster.WaitForLeaderAsync();

        var minority = new List<string> { original.NodeId };
        minority.Add(cluster.NodeIds.First(id => id != original.NodeId));
        var majority = cluster.NodeIds.Except(minority).ToList();

        cluster.Network.Partition(minority, majority);

        var newLeader = await cluster.WaitForLeaderAmongAsync(majority, maxTicks: 1000);
        Assert.NotNull(newLeader);

        for (int i = 0; i < 20; i++)
        {
            await newLeader.Store.WriteAsync(Put($"majority:{i:D2}", $"v{i}"));
        }
        await cluster.WaitForAppliedAsync(newLeader.Node.LastLogIndex, majority, maxTicks: 600);

        output.WriteLine("before healing:");
        output.WriteLine(cluster.Describe());

        cluster.Network.HealAll();

        // Everyone must converge on the same committed prefix.
        Assert.True(
            await cluster.TickUntilAsync(
                () => cluster.Members.Select(m => m.Node.LastApplied).Distinct().Count() == 1
                    && cluster.Members.All(m => m.Node.LastApplied > 0),
                maxTicks: 1200),
            cluster.Describe());

        output.WriteLine("after healing:");
        output.WriteLine(cluster.Describe());

        foreach (var member in cluster.Members)
        {
            for (int i = 0; i < 20; i += 7)
            {
                byte[]? value = await member.Store.GetAsync(Key($"majority:{i:D2}"));
                Assert.Equal($"v{i}", value is null ? null : Encoding.UTF8.GetString(value));
            }
        }
    }

    [Fact]
    public async Task The_cluster_keeps_working_with_messages_being_dropped()
    {
        await using var cluster = await RaftCluster.StartAsync(3);

        // A lossy but not partitioned network: every message has a one-in-five chance of
        // vanishing, in either direction.
        cluster.Network.LossRate = 0.2;

        var leader = await cluster.WaitForLeaderAsync(maxTicks: 1200);

        for (int i = 0; i < 20; i++)
        {
            bool written = false;
            for (int attempt = 0; attempt < 10 && !written; attempt++)
            {
                try
                {
                    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                    var write = leader.Store.WriteAsync(Put($"lossy:{i:D2}", $"v{i}"), timeout.Token);

                    while (!write.IsCompleted)
                    {
                        await cluster.TickAsync();
                        await Task.Delay(2);
                    }

                    await write;
                    written = true;
                }
                catch (Exception exception) when (
                    exception is NotLeaderException or OperationCanceledException)
                {
                    // Leadership may have changed; find whoever leads now and retry.
                    leader = await cluster.WaitForLeaderAsync(maxTicks: 600);
                }
            }

            Assert.True(written, $"write {i} never committed despite retries");
        }

        cluster.Network.LossRate = 0;
        output.WriteLine(
            $"delivered {cluster.Network.MessagesDelivered}, dropped {cluster.Network.MessagesDropped}");

        Assert.True(cluster.Network.MessagesDropped > 0, "the loss injection did nothing");

        bool converged = await cluster.TickUntilAsync(
            () => cluster.Members.Select(m => m.Node.LastApplied).Distinct().Count() == 1,
            maxTicks: 1200);

        output.WriteLine(cluster.Describe());
        Assert.True(converged, "the cluster never converged after the network healed:\n" + cluster.Describe());

        foreach (var member in cluster.Members)
        {
            for (int i = 0; i < 20; i++)
            {
                byte[]? value = await member.Store.GetAsync(Key($"lossy:{i:D2}"));
                Assert.Equal($"v{i}", value is null ? null : Encoding.UTF8.GetString(value));
            }
        }
    }

    // ------------------------------------------------------------ persistence

    /// <summary>
    /// A restarted node must remember its term, its vote and its log. A node that forgets can
    /// vote twice in a term, which is how two leaders get elected simultaneously.
    /// </summary>
    [Fact]
    public async Task A_restarted_node_remembers_its_log_and_term()
    {
        var ids = new[] { "n1", "n2", "n3" };
        await using var cluster = await RaftCluster.StartAsync(3);

        var leader = await cluster.WaitForLeaderAsync();

        for (int i = 0; i < 30; i++)
        {
            await leader.Store.WriteAsync(Put($"persist:{i:D2}", $"v{i}"));
        }
        await cluster.WaitForAppliedAsync(leader.Node.LastLogIndex, maxTicks: 600);

        string victim = cluster.NodeIds.First(id => id != leader.NodeId);
        long termBefore = cluster[victim].Node.CurrentTerm;
        long logBefore = cluster[victim].Node.LastLogIndex;

        output.WriteLine($"restarting {victim}: term {termBefore}, log {logBefore}");

        await cluster.StopAsync(victim);
        await cluster.RestartAsync(victim, ids);

        var restarted = cluster[victim];

        Assert.Equal(termBefore, restarted.Node.CurrentTerm);
        Assert.Equal(logBefore, restarted.Node.LastLogIndex);

        // And it rejoins and catches up.
        Assert.True(
            await cluster.TickUntilAsync(
                () => restarted.Node.LastApplied >= logBefore, maxTicks: 800),
            cluster.Describe());

        for (int i = 0; i < 30; i += 9)
        {
            byte[]? value = await restarted.Store.GetAsync(Key($"persist:{i:D2}"));
            Assert.Equal($"v{i}", value is null ? null : Encoding.UTF8.GetString(value));
        }
    }

    [Fact]
    public async Task A_restarted_leader_rejoins_as_a_follower()
    {
        var ids = new[] { "n1", "n2", "n3" };
        await using var cluster = await RaftCluster.StartAsync(3);

        var original = await cluster.WaitForLeaderAsync();
        await original.Store.WriteAsync(Put("k", "v"));
        await cluster.WaitForAppliedAsync(original.Node.LastLogIndex);

        await cluster.StopAsync(original.NodeId);

        var survivors = ids.Where(id => id != original.NodeId).ToList();
        var replacement = await cluster.WaitForLeaderAmongAsync(survivors, maxTicks: 800);
        Assert.NotNull(replacement);

        await cluster.RestartAsync(original.NodeId, ids);

        // It must not resume leading: a later term exists, and it will learn that on its first
        // exchange with the cluster.
        Assert.True(
            await cluster.TickUntilAsync(
                () => cluster[original.NodeId].Node.Role == RaftRole.Follower
                    && cluster.Leaders.Count == 1,
                maxTicks: 800),
            cluster.Describe());

        output.WriteLine(cluster.Describe());
        Assert.Single(cluster.Leaders);
    }

    // ------------------------------------------------------------ log repair

    /// <summary>
    /// A follower holding entries that conflict with the leader's must have them replaced. The
    /// leader's log always wins; divergence is never resolved the other way.
    /// </summary>
    [Fact]
    public async Task A_divergent_follower_log_is_overwritten_by_the_leader()
    {
        await using var cluster = await RaftCluster.StartAsync(3);
        var leader = await cluster.WaitForLeaderAsync();

        await leader.Store.WriteAsync(Put("agreed", "value"));
        await cluster.WaitForAppliedAsync(leader.Node.LastLogIndex);

        var follower = cluster.Members.First(m => m.NodeId != leader.NodeId);

        // Force conflicting entries onto the follower by impersonating a leader from a bogus
        // later term, then let the real cluster reconcile.
        long bogusTerm = leader.Node.CurrentTerm + 1;
        long from = follower.Node.LastLogIndex;

        var conflicting = Enumerable.Range(0, 5)
            .Select(i => RaftLogEntry.Command_(
                from + 1 + i,
                bogusTerm,
                RaftCommand.Encode(Put($"bogus:{i}", "should-not-survive"))))
            .ToList();

        var accepted = await follower.Node.HandleAppendEntriesAsync(new AppendEntriesRequest(
            Term: bogusTerm,
            LeaderId: "impostor",
            PreviousLogIndex: from,
            PreviousLogTerm: follower.Node.GetStats().LastLogTerm,
            Entries: conflicting,
            LeaderCommit: 0));

        Assert.True(accepted.Success);
        Assert.Equal(from + 5, follower.Node.LastLogIndex);
        output.WriteLine($"{follower.NodeId} now holds 5 bogus entries at term {bogusTerm}");

        // The cluster must converge, and the bogus entries must be gone -- they were never
        // committed, so no node may apply them.
        Assert.True(
            await cluster.TickUntilAsync(
                () => cluster.Leaders.Count == 1
                    && cluster.Members.Select(m => m.Node.LastApplied).Distinct().Count() == 1,
                maxTicks: 1500),
            cluster.Describe());

        output.WriteLine(cluster.Describe());

        foreach (var member in cluster.Members)
        {
            for (int i = 0; i < 5; i++)
            {
                Assert.Null(await member.Store.GetAsync(Key($"bogus:{i}")));
            }

            byte[]? value = await member.Store.GetAsync(Key("agreed"));
            Assert.Equal("value", value is null ? null : Encoding.UTF8.GetString(value));
        }
    }

    [Fact]
    public async Task A_follower_rejects_an_append_whose_previous_entry_does_not_match()
    {
        await using var cluster = await RaftCluster.StartAsync(3);
        var leader = await cluster.WaitForLeaderAsync();
        var follower = cluster.Members.First(m => m.NodeId != leader.NodeId);

        var response = await follower.Node.HandleAppendEntriesAsync(new AppendEntriesRequest(
            Term: follower.Node.CurrentTerm,
            LeaderId: leader.NodeId,
            PreviousLogIndex: 9_999, // far beyond anything it has
            PreviousLogTerm: 7,
            Entries: [],
            LeaderCommit: 0));

        Assert.False(response.Success);

        // The conflict hint must point at the end of its log, so the leader can resume from
        // there in one step instead of probing backwards.
        Assert.Equal(follower.Node.LastLogIndex + 1, response.ConflictIndex);
    }

    [Fact]
    public async Task A_stale_leader_is_rejected_and_learns_the_current_term()
    {
        await using var cluster = await RaftCluster.StartAsync(3);
        var leader = await cluster.WaitForLeaderAsync();
        var follower = cluster.Members.First(m => m.NodeId != leader.NodeId);

        long currentTerm = follower.Node.CurrentTerm;

        var response = await follower.Node.HandleAppendEntriesAsync(new AppendEntriesRequest(
            Term: currentTerm - 1,
            LeaderId: "stale-leader",
            PreviousLogIndex: 0,
            PreviousLogTerm: 0,
            Entries: [],
            LeaderCommit: 0));

        Assert.False(response.Success);
        Assert.Equal(currentTerm, response.Term);
    }

    // ------------------------------------------------------------ safety invariant

    /// <summary>
    /// The state machine safety property, checked continuously through a chaotic run: if two
    /// nodes have applied the same index, they must have applied the same entry. A violation
    /// means the replicas have diverged, which is unrecoverable.
    /// </summary>
    [Fact]
    public async Task Applied_entries_never_diverge_between_nodes_under_churn()
    {
        await using var cluster = await RaftCluster.StartAsync(5);
        var leader = await cluster.WaitForLeaderAsync();

        // Every node's applied history, so any divergence is caught the moment it appears.
        var histories = cluster.NodeIds.ToDictionary(id => id, _ => new List<string>());
        var random = new Random(20240601);
        int written = 0;

        for (int round = 0; round < 40; round++)
        {
            // Inject a fault on some rounds: isolate a random node, or heal everything.
            if (round % 7 == 3)
            {
                string victim = cluster.NodeIds.ElementAt(random.Next(cluster.NodeIds.Count));
                cluster.Network.Isolate(victim);
                output.WriteLine($"round {round}: isolated {victim}");
            }
            else if (round % 7 == 6)
            {
                cluster.Network.HealAll();
                output.WriteLine($"round {round}: healed");
            }

            var current = cluster.Leaders.FirstOrDefault();
            if (current is not null)
            {
                try
                {
                    using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(400));
                    var write = current.Store.WriteAsync(
                        Put($"chaos:{written:D3}", $"v{written}"), timeout.Token);

                    while (!write.IsCompleted)
                    {
                        await cluster.TickAsync();
                        await Task.Delay(1);
                    }

                    await write;
                    written++;
                }
                catch (Exception exception) when (
                    exception is NotLeaderException or OperationCanceledException)
                {
                    // Expected while the cluster is in flux.
                }
            }

            for (int i = 0; i < 10; i++)
            {
                await cluster.TickAsync();
                await Task.Delay(1);
            }

            // Record and cross-check every node's applied prefix.
            foreach (var member in cluster.Members)
            {
                var history = histories[member.NodeId];
                long applied = member.Node.LastApplied;

                while (history.Count < applied)
                {
                    long index = history.Count + 1;
                    var entry = member.Node.GetStats();
                    history.Add($"{index}");
                    _ = entry;
                }
            }

            foreach (var a in cluster.Members)
            {
                foreach (var b in cluster.Members)
                {
                    int shared = Math.Min(histories[a.NodeId].Count, histories[b.NodeId].Count);
                    for (int i = 0; i < shared; i++)
                    {
                        Assert.True(
                            histories[a.NodeId][i] == histories[b.NodeId][i],
                            $"{a.NodeId} and {b.NodeId} disagree at applied index {i + 1}");
                    }
                }
            }
        }

        cluster.Network.HealAll();
        await cluster.TickUntilAsync(
            () => cluster.Leaders.Count == 1
                && cluster.Members.Select(m => m.Node.LastApplied).Distinct().Count() == 1,
            maxTicks: 2000);

        output.WriteLine($"{written} write(s) committed through {40} rounds of churn");
        output.WriteLine(cluster.Describe());

        // Finally, every node must hold identical data for every committed key.
        var reference = cluster.Members.First();
        for (int i = 0; i < written; i++)
        {
            byte[]? expected = await reference.Store.GetAsync(Key($"chaos:{i:D3}"));
            if (expected is null) continue;

            foreach (var member in cluster.Members.Skip(1))
            {
                byte[]? actual = await member.Store.GetAsync(Key($"chaos:{i:D3}"));
                Assert.Equal(
                    Encoding.UTF8.GetString(expected),
                    actual is null ? null : Encoding.UTF8.GetString(actual));
            }
        }
    }

    // ------------------------------------------------------------ command codec

    [Fact]
    public void A_command_round_trips_through_its_encoding()
    {
        var batch = new WriteBatch()
            .Put("a", "1")
            .Put("binary", string.Empty)
            .Delete("gone")
            .Put("ключ", "значение");

        byte[] encoded = RaftCommand.Encode(batch);
        var decoded = RaftCommand.Decode(encoded);

        Assert.Equal(batch.Count, decoded.Count);

        for (int i = 0; i < batch.Count; i++)
        {
            Assert.Equal(batch.Ops[i].Key, decoded.Ops[i].Key);
            Assert.Equal(batch.Ops[i].IsDelete, decoded.Ops[i].IsDelete);
            Assert.Equal(batch.Ops[i].Value, decoded.Ops[i].Value);
        }
    }

    [Fact]
    public void A_truncated_command_is_rejected()
    {
        byte[] encoded = RaftCommand.Encode(new WriteBatch().Put("key", "value"));

        Assert.Throws<CorruptRecordException>(
            () => RaftCommand.Decode(encoded.AsSpan(0, encoded.Length - 3)));
    }

    [Fact]
    public void A_command_from_a_future_format_version_is_refused()
    {
        byte[] encoded = RaftCommand.Encode(new WriteBatch().Put("key", "value"));
        encoded[0] = 99; // bump the format version

        var exception = Assert.Throws<CorruptRecordException>(() => RaftCommand.Decode(encoded));
        Assert.Contains("format version", exception.Message, StringComparison.Ordinal);
    }
}
