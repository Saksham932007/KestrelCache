using System.Text;
using KestrelCache.Raft;
using Xunit;
using Xunit.Abstractions;

namespace KestrelCache.Tests;

/// <summary>
/// Checks Raft's Log Matching Property directly against every node's log.
/// </summary>
/// <remarks>
/// The property: if two logs hold an entry with the same index and term, those entries are
/// identical, and so is every entry before them. It is the foundation the whole protocol rests
/// on — the reason a leader can reconcile a follower by comparing one entry instead of the whole
/// log — so a violation is not a performance problem but a correctness one, and it is invisible
/// from the outside until replicas have silently diverged.
///
/// This test earned its place. It found three bugs that every other test in the suite passed
/// straight through, because all three produced a cluster that looked entirely healthy from
/// the outside -- one leader, matching commit indices, writes returning success -- while the
/// logs underneath had quietly diverged:
///
/// 1. Two concurrent proposals were assigned the same log index, so one was silently discarded
///    while its client was told the write had committed.
/// 2. A follower could pass the consistency check for a position beyond the end of its own log,
///    accept entries starting there, and open a gap that shifted every later index by one.
/// 3. A proposal's waiter was keyed on index alone, so an entry written at that index by a
///    later leader would satisfy a client whose own write had been replaced.
///
/// Each one reported success for a write that did not happen, which is the worst failure a
/// database can have, and none was visible without comparing the logs entry by entry.
/// </remarks>
[Collection("raft-cluster")]
public sealed class RaftLogMatchingTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData(3, 0.0)]
    [InlineData(3, 0.1)]
    [InlineData(3, 0.25)]
    [InlineData(3, 0.4)]
    [InlineData(5, 0.0)]
    [InlineData(5, 0.25)]
    public async Task Logs_match_across_nodes_under_message_loss(int size, double lossRate)
    {
        await using var cluster = await RaftCluster.StartAsync(size);
        cluster.Network.LossRate = lossRate;

        var leader = await cluster.WaitForLeaderAsync(maxTicks: 1500);

        for (int i = 0; i < 25; i++)
        {
            for (int attempt = 0; attempt < 12; attempt++)
            {
                try
                {
                    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                    var write = leader.Store.WriteAsync(
                        new WriteBatch().Put($"k:{i:D2}", $"v{i}"), timeout.Token);

                    while (!write.IsCompleted)
                    {
                        await cluster.TickAsync();
                        await Task.Delay(1);
                    }

                    await write;
                    break;
                }
                catch (Exception exception) when (
                    exception is NotLeaderException or OperationCanceledException)
                {
                    leader = await cluster.WaitForLeaderAsync(maxTicks: 800);
                }
            }
        }

        cluster.Network.LossRate = 0;
        await cluster.TickUntilAsync(
            () => cluster.Leaders.Count == 1
                && cluster.Members.Select(m => m.Node.LastApplied).Distinct().Count() == 1,
            maxTicks: 1500);

        output.WriteLine($"{size} nodes at {lossRate:P0} loss");
        output.WriteLine(cluster.Describe());

        // Read every node's log through the live node. Reopening the files from outside would
        // replay them, and replay truncates a torn tail -- so the measurement would destroy what
        // it was measuring. (This is not hypothetical: an earlier version of this test did
        // exactly that and reported followers holding two entries out of twenty-six.)
        var logs = cluster.Members.ToDictionary(
            member => member.NodeId,
            member => member.Node.InspectLog().ToList());

        foreach (var (id, entries) in logs)
        {
            output.WriteLine($"{id}: {entries.Count} entries, terms "
                + string.Join(",", entries.Select(e => e.Term).Distinct()));
        }

        var violations = new List<string>();

        foreach (var (idA, logA) in logs)
        {
            foreach (var (idB, logB) in logs)
            {
                if (string.CompareOrdinal(idA, idB) >= 0) continue;

                int shared = Math.Min(logA.Count, logB.Count);
                for (int i = 0; i < shared; i++)
                {
                    var a = logA[i];
                    var b = logB[i];

                    if (a.Term != b.Term) break; // divergence from here on is allowed

                    if (a.IsNoOp != b.IsNoOp || !a.Command.AsSpan().SequenceEqual(b.Command))
                    {
                        violations.Add(
                            $"index {i + 1} term {a.Term}: {idA} holds "
                                + $"{Describe(a)} but {idB} holds {Describe(b)}");
                    }
                }
            }
        }

        if (violations.Count > 0)
        {
            output.WriteLine("--- LOG MATCHING VIOLATIONS ---");
            foreach (string violation in violations) output.WriteLine("  " + violation);
            output.WriteLine("--- trace ---");
            foreach (string line in cluster.Trace) output.WriteLine("  " + line);
        }

        Assert.Empty(violations);

        // And the data must agree on every key.
        var reference = cluster.Members.First();
        for (int i = 0; i < 25; i++)
        {
            byte[]? expected = await reference.Store.GetAsync(Encoding.UTF8.GetBytes($"k:{i:D2}"));
            foreach (var member in cluster.Members.Skip(1))
            {
                byte[]? actual = await member.Store.GetAsync(Encoding.UTF8.GetBytes($"k:{i:D2}"));
                Assert.Equal(
                    expected is null ? null : Encoding.UTF8.GetString(expected),
                    actual is null ? null : Encoding.UTF8.GetString(actual));
            }
        }
    }

    private static string Describe(RaftLogEntry entry)
    {
        if (entry.IsNoOp) return "<no-op>";
        try
        {
            var batch = RaftCommand.Decode(entry.Command);
            return string.Join("+", batch.Ops.Select(op => Encoding.UTF8.GetString(op.Key)));
        }
        catch (CorruptRecordException)
        {
            return $"<undecodable {entry.Command.Length} bytes>";
        }
    }
}
