using System.Collections.Concurrent;

namespace KestrelCache.Raft;

/// <summary>
/// An in-process network that can be partitioned, delayed and made to drop messages.
/// </summary>
/// <remarks>
/// <para>
/// This exists because Raft's interesting behaviour is entirely about how it copes with a
/// network that misbehaves, and almost none of that is reproducible over real sockets on one
/// machine. A test cannot sever a loopback connection at a precise instant, drop exactly the one
/// reply that would have completed an election, or hold a message for 400 ms and then deliver
/// it — and those are precisely the scenarios where consensus implementations are wrong.
/// </para>
/// <para>
/// With the network in process and under the test's control, a split-brain scenario becomes
/// three lines and runs in milliseconds, deterministically. The production
/// <see cref="TcpRaftTransport"/> implements the same interface, so the consensus module itself
/// cannot tell the difference.
/// </para>
/// </remarks>
public sealed class InMemoryRaftNetwork
{
    private readonly ConcurrentDictionary<string, RaftNode> _nodes = new();
    private readonly ConcurrentDictionary<string, byte> _isolated = new();
    private readonly ConcurrentDictionary<(string From, string To), byte> _severed = new();

    private long _messagesDelivered;
    private long _messagesDropped;

    /// <summary>Simulated one-way latency applied to every message.</summary>
    public TimeSpan Latency { get; set; } = TimeSpan.Zero;

    /// <summary>Fraction of messages to drop at random, in [0, 1].</summary>
    public double LossRate { get; set; }

    /// <summary>Messages that reached their destination.</summary>
    public long MessagesDelivered => Interlocked.Read(ref _messagesDelivered);

    /// <summary>Messages dropped by a partition or by random loss.</summary>
    public long MessagesDropped => Interlocked.Read(ref _messagesDropped);

    private readonly Random _random = new(12345);

    /// <summary>Registers a node so its peers can reach it.</summary>
    public void Register(RaftNode node) => _nodes[node.NodeId] = node;

    /// <summary>A transport for one node's outbound messages.</summary>
    public IRaftTransport TransportFor(string nodeId) => new Transport(this, nodeId);

    // ---------------------------------------------------------------- fault injection

    /// <summary>Cuts a node off from every other node, in both directions.</summary>
    public void Isolate(string nodeId) => _isolated[nodeId] = 0;

    /// <summary>Reconnects a previously isolated node.</summary>
    public void Heal(string nodeId) => _isolated.TryRemove(nodeId, out _);

    /// <summary>Removes every partition.</summary>
    public void HealAll()
    {
        _isolated.Clear();
        _severed.Clear();
    }

    /// <summary>
    /// Splits the cluster into two groups that can talk within themselves but not across.
    /// </summary>
    /// <remarks>
    /// The canonical test: with five nodes split three against two, the majority side must elect
    /// a leader and keep committing, and the minority side must not be able to commit anything
    /// at all. A consensus implementation that lets the minority commit has a split-brain bug,
    /// and this is how to prove it does not.
    /// </remarks>
    public void Partition(IEnumerable<string> groupA, IEnumerable<string> groupB)
    {
        var a = groupA.ToArray();
        var b = groupB.ToArray();

        foreach (string from in a)
        {
            foreach (string to in b)
            {
                _severed[(from, to)] = 0;
                _severed[(to, from)] = 0;
            }
        }
    }

    /// <summary>Blocks messages in one direction only, modelling an asymmetric failure.</summary>
    public void Sever(string from, string to) => _severed[(from, to)] = 0;

    /// <summary>Restores a one-way link.</summary>
    public void Restore(string from, string to) => _severed.TryRemove((from, to), out _);

    private bool CanDeliver(string from, string to)
    {
        if (_isolated.ContainsKey(from) || _isolated.ContainsKey(to)) return false;
        if (_severed.ContainsKey((from, to))) return false;

        if (LossRate > 0)
        {
            lock (_random)
            {
                if (_random.NextDouble() < LossRate) return false;
            }
        }

        return true;
    }

    private async Task<bool> TravelAsync(string from, string to, CancellationToken cancellationToken)
    {
        if (!CanDeliver(from, to))
        {
            Interlocked.Increment(ref _messagesDropped);
            return false;
        }

        if (Latency > TimeSpan.Zero)
        {
            await Task.Delay(Latency, cancellationToken).ConfigureAwait(false);

            // The link may have been cut while the message was in flight, which is exactly what
            // happens in reality and a case worth exercising.
            if (!CanDeliver(from, to))
            {
                Interlocked.Increment(ref _messagesDropped);
                return false;
            }
        }

        Interlocked.Increment(ref _messagesDelivered);
        return true;
    }

    private sealed class Transport(InMemoryRaftNetwork network, string from) : IRaftTransport
    {
        public async Task<RequestVoteResponse?> RequestVoteAsync(
            string peerId,
            RequestVoteRequest request,
            CancellationToken cancellationToken)
        {
            if (!await network.TravelAsync(from, peerId, cancellationToken).ConfigureAwait(false))
            {
                return null;
            }

            if (!network._nodes.TryGetValue(peerId, out var peer)) return null;

            var response = await peer.HandleRequestVoteAsync(request, cancellationToken)
                .ConfigureAwait(false);

            // The reply travels too, so a test can drop a reply while letting the request
            // through -- which is how a node ends up having voted without the candidate knowing.
            if (!await network.TravelAsync(peerId, from, cancellationToken).ConfigureAwait(false))
            {
                return null;
            }

            return response;
        }

        public async Task<InstallSnapshotResponse?> InstallSnapshotAsync(
            string peerId,
            InstallSnapshotRequest request,
            CancellationToken cancellationToken)
        {
            if (!await network.TravelAsync(from, peerId, cancellationToken).ConfigureAwait(false))
            {
                return null;
            }

            if (!network._nodes.TryGetValue(peerId, out var peer)) return null;

            var response = await peer.HandleInstallSnapshotAsync(request, cancellationToken)
                .ConfigureAwait(false);

            if (!await network.TravelAsync(peerId, from, cancellationToken).ConfigureAwait(false))
            {
                return null;
            }

            return response;
        }

        public async Task<AppendEntriesResponse?> AppendEntriesAsync(
            string peerId,
            AppendEntriesRequest request,
            CancellationToken cancellationToken)
        {
            if (!await network.TravelAsync(from, peerId, cancellationToken).ConfigureAwait(false))
            {
                return null;
            }

            if (!network._nodes.TryGetValue(peerId, out var peer)) return null;

            var response = await peer.HandleAppendEntriesAsync(request, cancellationToken)
                .ConfigureAwait(false);

            if (!await network.TravelAsync(peerId, from, cancellationToken).ConfigureAwait(false))
            {
                return null;
            }

            return response;
        }
    }
}
