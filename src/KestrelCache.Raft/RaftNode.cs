namespace KestrelCache.Raft;

/// <summary>Raised when a write is sent to a node that is not the leader.</summary>
public sealed class NotLeaderException(string? leaderId)
    : KestrelCacheException(
        leaderId is null
            ? "This node is not the leader and does not currently know who is."
            : $"This node is not the leader; the leader is '{leaderId}'.")
{
    /// <summary>The leader this node last heard from, if any.</summary>
    public string? LeaderId { get; } = leaderId;
}

/// <summary>
/// One node of a Raft cluster: leader election, log replication, and commitment.
/// </summary>
/// <remarks>
/// <para>
/// Raft solves one problem: getting a group of machines to agree on an ordered sequence of
/// commands, such that the agreed prefix is never lost and never contradicted, even when
/// machines crash and the network misbehaves. Everything here follows from that.
/// </para>
/// <para><b>The three rules that carry the safety argument</b></para>
/// <list type="number">
/// <item>
/// <b>One leader per term.</b> A node votes at most once per term and persists that vote before
/// replying, so two candidates cannot both collect a majority in the same term. This is why
/// <see cref="RaftPersistentState"/> fsyncs: a node that forgot its vote could grant a second
/// one after restarting, and two leaders accepting conflicting writes is the failure Raft
/// exists to prevent.
/// </item>
/// <item>
/// <b>Only an up-to-date candidate can win.</b> A voter refuses any candidate whose log is
/// behind its own. Combined with the majority requirement, this guarantees the winner's log
/// already contains every committed entry — so a new leader never needs to recover entries from
/// followers, which is what keeps the protocol simple enough to reason about.
/// </item>
/// <item>
/// <b>A leader never overwrites its own log.</b> It only appends, and it brings followers into
/// line by finding the last index where they agree and overwriting their divergent suffix.
/// Divergence is always resolved in the leader's favour, never the reverse.
/// </item>
/// </list>
/// <para><b>Concurrency</b></para>
/// <para>
/// All state transitions happen under a single mutex. This is deliberate: Raft's correctness
/// arguments are about the ordering of state changes, and fine-grained locking would make them
/// very hard to verify for a throughput gain that does not matter — the bottleneck is the fsync
/// and the network round trip, both of which happen outside the lock.
/// </para>
/// </remarks>
public sealed class RaftNode : IAsyncDisposable
{
    private readonly RaftOptions _options;
    private readonly IRaftTransport _transport;
    private readonly IRaftStateMachine _stateMachine;
    private readonly RaftLog _log;
    private readonly RaftPersistentState _state;
    private readonly Random _random;
    private readonly SemaphoreSlim _mutex = new(1, 1);
    private readonly CancellationTokenSource _shutdown = new();

    /// <summary>
    /// Client proposals awaiting commitment, keyed by the log index they occupy.
    /// </summary>
    /// <remarks>
    /// The term is held alongside the waiter, and matched on completion. An index alone does not
    /// identify an entry: a deposed leader's uncommitted entry at index N can be replaced by a
    /// different entry at index N from a later term, and completing the waiter on index alone
    /// would tell the client its write committed when a different write took that slot.
    /// </remarks>
    private readonly Dictionary<long, List<(long Term, TaskCompletionSource Completion)>> _pending = [];

    private readonly Dictionary<string, long> _nextIndex = [];
    private readonly Dictionary<string, long> _matchIndex = [];

    private RaftRole _role = RaftRole.Follower;
    private string? _leaderId;
    private long _commitIndex;
    private long _lastApplied;
    private DateTime _lastHeardFromLeader = DateTime.UtcNow;
    private TimeSpan _currentElectionTimeout;
    private Task? _driver;
    private volatile bool _disposed;

    private void Trace(string message) =>
        _options.Trace?.Invoke($"[{_options.NodeId}] {message}");

    private long _electionsStarted;
    private long _electionsWon;
    private long _appendEntriesSent;
    private long _appendEntriesRejected;
    private long _entriesApplied;

    /// <summary>Creates a node. Call <see cref="StartAsync"/> to begin participating.</summary>
    public RaftNode(
        RaftOptions options,
        IRaftTransport transport,
        IRaftStateMachine stateMachine)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(transport);
        ArgumentNullException.ThrowIfNull(stateMachine);
        options.Validate();

        _options = options;
        _transport = transport;
        _stateMachine = stateMachine;
        _random = options.RandomSeed is { } seed
            ? new Random(seed)
            : new Random(options.NodeId.GetHashCode(StringComparison.Ordinal));

        Directory.CreateDirectory(options.DataDirectory);
        _log = RaftLog.Open(Path.Combine(options.DataDirectory, "raft.log"));
        _state = RaftPersistentState.Open(Path.Combine(options.DataDirectory, "raft.state"));

        _currentElectionTimeout = NextElectionTimeout();
    }

    /// <summary>This node's identifier.</summary>
    public string NodeId => _options.NodeId;

    /// <summary>The role this node is currently in.</summary>
    public RaftRole Role
    {
        get
        {
            _mutex.Wait();
            try { return _role; }
            finally { _mutex.Release(); }
        }
    }

    /// <summary>The leader this node last heard from, if any.</summary>
    public string? LeaderId
    {
        get
        {
            _mutex.Wait();
            try { return _leaderId; }
            finally { _mutex.Release(); }
        }
    }

    /// <summary>The term this node believes it is in.</summary>
    public long CurrentTerm => _state.CurrentTerm;

    /// <summary>The highest index known to be committed.</summary>
    public long CommitIndex
    {
        get
        {
            _mutex.Wait();
            try { return _commitIndex; }
            finally { _mutex.Release(); }
        }
    }

    /// <summary>The highest index applied to the state machine.</summary>
    public long LastApplied
    {
        get
        {
            _mutex.Wait();
            try { return _lastApplied; }
            finally { _mutex.Release(); }
        }
    }

    /// <summary>Index of the last entry in this node's log.</summary>
    public long LastLogIndex => _log.LastIndex;

    /// <summary>True when this node is the leader.</summary>
    public bool IsLeader => Role == RaftRole.Leader;

    /// <summary>
    /// Reads entries from this node's log, for diagnostics and for verifying the Log Matching
    /// Property across a cluster.
    /// </summary>
    /// <remarks>
    /// Exposed here rather than left to callers opening the log file themselves, because a
    /// second opener would replay it — and replay truncates a torn tail, so inspecting a live
    /// node's log from outside can destroy the entries it was meant to examine. The log is
    /// opened exclusively for exactly that reason, and this is the way in.
    /// </remarks>
    public IReadOnlyList<RaftLogEntry> InspectLog(long fromIndex = 1, int maxEntries = int.MaxValue)
    {
        _mutex.Wait();
        try
        {
            return _log.Read(fromIndex, maxEntries);
        }
        finally
        {
            _mutex.Release();
        }
    }

    /// <summary>Counters, for tests and metrics.</summary>
    public RaftStats GetStats()
    {
        _mutex.Wait();
        try
        {
            return new RaftStats
            {
                NodeId = NodeId,
                Role = _role,
                Term = _state.CurrentTerm,
                LeaderId = _leaderId,
                CommitIndex = _commitIndex,
                LastApplied = _lastApplied,
                LastLogIndex = _log.LastIndex,
                LastLogTerm = _log.LastTerm,
                LogSizeBytes = _log.SizeBytes,
                ElectionsStarted = Interlocked.Read(ref _electionsStarted),
                ElectionsWon = Interlocked.Read(ref _electionsWon),
                AppendEntriesSent = Interlocked.Read(ref _appendEntriesSent),
                AppendEntriesRejected = Interlocked.Read(ref _appendEntriesRejected),
                EntriesApplied = Interlocked.Read(ref _entriesApplied),
                MatchIndex = new Dictionary<string, long>(_matchIndex),
            };
        }
        finally
        {
            _mutex.Release();
        }
    }

    // ================================================================ lifecycle

    /// <summary>Starts the election and replication timers.</summary>
    public Task StartAsync()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _driver ??= Task.Run(() => DriveAsync(_shutdown.Token));
        return Task.CompletedTask;
    }

    /// <summary>
    /// The single timer loop that drives everything.
    /// </summary>
    /// <remarks>
    /// One loop rather than a timer per responsibility, because the roles are mutually exclusive:
    /// a leader never needs an election timer and a follower never needs to send heartbeats.
    /// Ticking frequently and branching on the current role keeps all the timing in one place
    /// where it can be reasoned about, and makes the whole thing straightforward to drive
    /// manually from a test.
    /// </remarks>
    private async Task DriveAsync(CancellationToken cancellationToken)
    {
        // A tick well below the heartbeat interval, so heartbeats go out close to on time.
        var tick = TimeSpan.FromMilliseconds(
            Math.Max(5, _options.HeartbeatInterval.TotalMilliseconds / 4));

        using var timer = new PeriodicTimer(tick);

        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                await TickAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Shutting down.
        }
    }

    /// <summary>
    /// Advances the node by one step. Exposed so tests can drive time deterministically instead
    /// of sleeping.
    /// </summary>
    public async Task TickAsync(CancellationToken cancellationToken = default)
    {
        if (_disposed) return;

        RaftRole role;
        bool electionDue;

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            role = _role;
            electionDue = role != RaftRole.Leader
                && DateTime.UtcNow - _lastHeardFromLeader >= _currentElectionTimeout;
        }
        finally
        {
            _mutex.Release();
        }

        if (role == RaftRole.Leader)
        {
            await ReplicateToAllAsync(cancellationToken).ConfigureAwait(false);
        }
        else if (electionDue)
        {
            await StartElectionAsync(cancellationToken).ConfigureAwait(false);
        }

        await ApplyCommittedAsync(cancellationToken).ConfigureAwait(false);
    }

    // ================================================================ elections

    private TimeSpan NextElectionTimeout()
    {
        double baseMs = _options.ElectionTimeout.TotalMilliseconds;
        double jitter = baseMs * _options.ElectionTimeoutJitter * _random.NextDouble();
        return TimeSpan.FromMilliseconds(baseMs + jitter);
    }

    /// <summary>
    /// Becomes a candidate and campaigns for the next term.
    /// </summary>
    private async Task StartElectionAsync(CancellationToken cancellationToken)
    {
        long term;
        long lastLogIndex;
        long lastLogTerm;

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_role == RaftRole.Leader) return;

            // Term increment, self-vote and the fsync all happen before a single message goes
            // out. Campaigning first and persisting afterwards would let a crash mid-election
            // leave the node able to vote again in the same term.
            _state.AdvanceTerm(_state.CurrentTerm + 1);
            _state.RecordVote(_options.NodeId);

            _role = RaftRole.Candidate;
            _leaderId = null;
            _lastHeardFromLeader = DateTime.UtcNow;
            _currentElectionTimeout = NextElectionTimeout();

            term = _state.CurrentTerm;
            lastLogIndex = _log.LastIndex;
            lastLogTerm = _log.LastTerm;

            Interlocked.Increment(ref _electionsStarted);
            Trace($"became candidate in term {term} (log {lastLogIndex}@{lastLogTerm})");
        }
        finally
        {
            _mutex.Release();
        }

        // A single-node cluster is its own majority and needs no round trip.
        if (_options.Peers.Count == 1)
        {
            await BecomeLeaderAsync(term, cancellationToken).ConfigureAwait(false);
            return;
        }

        var request = new RequestVoteRequest(term, _options.NodeId, lastLogIndex, lastLogTerm);
        int votes = 1; // our own
        var voters = new HashSet<string> { _options.NodeId };

        var ballots = _options.OtherPeers
            .Select(peer => SolicitVoteAsync(peer, request, cancellationToken))
            .ToList();

        // Votes are counted as they arrive, so the election completes as soon as a majority is
        // reached rather than waiting for the slowest or a dead peer.
        while (ballots.Count > 0)
        {
            var finished = await Task.WhenAny(ballots).ConfigureAwait(false);
            ballots.Remove(finished);

            var (peer, response) = await finished.ConfigureAwait(false);
            if (response is null) continue;

            if (response.Value.Term > term)
            {
                await StepDownAsync(response.Value.Term, cancellationToken).ConfigureAwait(false);
                return;
            }

            if (response.Value.VoteGranted && voters.Add(peer))
            {
                votes++;
                Trace($"got vote {votes}/{_options.QuorumSize} from {peer} in term {term}");
                if (votes >= _options.QuorumSize)
                {
                    await BecomeLeaderAsync(term, cancellationToken).ConfigureAwait(false);
                    return;
                }
            }
        }

        // No majority. The randomised timeout means the retry will not collide with the other
        // candidates' retries, which is what lets a split vote resolve instead of repeating.
    }

    private async Task<(string Peer, RequestVoteResponse? Response)> SolicitVoteAsync(
        string peer,
        RequestVoteRequest request,
        CancellationToken cancellationToken)
    {
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(_options.RpcTimeout);

            var response = await _transport
                .RequestVoteAsync(peer, request, timeout.Token)
                .ConfigureAwait(false);

            return (peer, response);
        }
        catch (OperationCanceledException)
        {
            return (peer, null);
        }
        catch (IOException)
        {
            // A peer being unreachable is an ordinary condition in a consensus protocol, not an
            // error: that is precisely what a majority requirement is for.
            return (peer, null);
        }
    }

    private async Task BecomeLeaderAsync(long term, CancellationToken cancellationToken)
    {
        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // The term may have moved on while votes were being collected.
            if (_role != RaftRole.Candidate || _state.CurrentTerm != term) return;

            _role = RaftRole.Leader;
            _leaderId = _options.NodeId;

            _nextIndex.Clear();
            _matchIndex.Clear();
            foreach (string peer in _options.OtherPeers)
            {
                // Optimistic: assume followers are up to date and back off on rejection. The
                // alternative, starting from zero, would re-send the entire log to every
                // follower after every election.
                _nextIndex[peer] = _log.LastIndex + 1;
                _matchIndex[peer] = 0;
            }

            Interlocked.Increment(ref _electionsWon);
            Trace($"BECAME LEADER in term {term}");
        }
        finally
        {
            _mutex.Release();
        }

        // The no-op is what makes entries inherited from previous terms safe to commit. See the
        // remarks on RaftLogEntry: counting replicas of an old-term entry can lose a committed
        // one, so the leader commits an entry of its own term instead and the prefix follows.
        //
        // The log assigns the index, so it cannot collide with a concurrent proposal.
        await _log.AppendAsLeaderAsync(term, [], isNoOp: true, cancellationToken)
            .ConfigureAwait(false);

        await ReplicateToAllAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task StepDownAsync(long newTerm, CancellationToken cancellationToken)
    {
        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            StepDownLocked(newTerm);
        }
        finally
        {
            _mutex.Release();
        }
    }

    private void StepDownLocked(long newTerm)
    {
        if (newTerm > _state.CurrentTerm)
        {
            Trace($"term {_state.CurrentTerm} -> {newTerm}, clearing vote");
            _state.AdvanceTerm(newTerm);
        }

        if (_role != RaftRole.Follower)
        {
            Trace($"stepping down from {_role} in term {_state.CurrentTerm}");
            _role = RaftRole.Follower;

            // Anything a client is waiting on cannot be guaranteed any more: this node no longer
            // decides what commits. Failing those waits is correct, and far better than leaving
            // callers hanging on a promise it cannot keep.
            FailPendingLocked(new NotLeaderException(null));
        }

        _leaderId = null;
        _lastHeardFromLeader = DateTime.UtcNow;
        _currentElectionTimeout = NextElectionTimeout();
    }

    // ================================================================ RPC handlers

    /// <summary>Handles a vote request from a candidate.</summary>
    public async Task<RequestVoteResponse> HandleRequestVoteAsync(
        RequestVoteRequest request,
        CancellationToken cancellationToken = default)
    {
        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // A candidate from an older term is stale; telling it our term makes it step down.
            if (request.Term < _state.CurrentTerm)
            {
                return new RequestVoteResponse(_state.CurrentTerm, false, _options.NodeId);
            }

            if (request.Term > _state.CurrentTerm)
            {
                StepDownLocked(request.Term);
            }

            bool alreadyVotedElsewhere =
                _state.VotedFor is not null && _state.VotedFor != request.CandidateId;

            // The election restriction. A candidate whose log is behind ours must not win,
            // because a leader is never allowed to be missing a committed entry -- and since a
            // winner needs a majority, refusing here guarantees the winner's log contains
            // everything committed.
            bool candidateUpToDate =
                request.LastLogTerm > _log.LastTerm
                || (request.LastLogTerm == _log.LastTerm && request.LastLogIndex >= _log.LastIndex);

            bool grant = !alreadyVotedElsewhere && candidateUpToDate;

            Trace(
                $"vote request from {request.CandidateId} term {request.Term} "
                    + $"(log {request.LastLogIndex}@{request.LastLogTerm}): "
                    + $"myTerm={_state.CurrentTerm} myVote={_state.VotedFor ?? "none"} "
                    + $"myLog={_log.LastIndex}@{_log.LastTerm} -> {(grant ? "GRANT" : "refuse")}");

            if (grant)
            {
                // Persisted before the reply is sent. Replying first and crashing would let this
                // node vote again in the same term after restarting, which is how two leaders
                // get elected at once.
                _state.RecordVote(request.CandidateId);

                // Granting a vote counts as hearing from the cluster, so this node does not
                // immediately launch a competing candidacy of its own.
                _lastHeardFromLeader = DateTime.UtcNow;
                _currentElectionTimeout = NextElectionTimeout();
            }

            return new RequestVoteResponse(_state.CurrentTerm, grant, _options.NodeId);
        }
        finally
        {
            _mutex.Release();
        }
    }

    /// <summary>Handles replication or a heartbeat from a leader.</summary>
    public async Task<AppendEntriesResponse> HandleAppendEntriesAsync(
        AppendEntriesRequest request,
        CancellationToken cancellationToken = default)
    {
        IReadOnlyList<RaftLogEntry> toAppend = [];
        long newCommitIndex = -1;
        AppendEntriesResponse response;

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (request.Term < _state.CurrentTerm)
            {
                return new AppendEntriesResponse(
                    _state.CurrentTerm, false, _options.NodeId, 0, 0, 0);
            }

            if (request.Term > _state.CurrentTerm)
            {
                StepDownLocked(request.Term);
            }
            else if (_role != RaftRole.Follower)
            {
                // A leader or candidate hearing from a leader of the same term has lost; there
                // is exactly one leader per term.
                Trace(
                    $"SAME-TERM DEMOTION: was {_role} in term {_state.CurrentTerm}, "
                        + $"{request.LeaderId} claims leadership of the same term");
                _role = RaftRole.Follower;
                FailPendingLocked(new NotLeaderException(request.LeaderId));
            }

            _leaderId = request.LeaderId;
            _lastHeardFromLeader = DateTime.UtcNow;
            _currentElectionTimeout = NextElectionTimeout();

            // The consistency check. Agreeing on the entry immediately before the batch proves,
            // by induction, that the whole prefix agrees -- which is the property that lets the
            // logs be reconciled by comparing one entry instead of all of them.
            if (!_log.Matches(request.PreviousLogIndex, request.PreviousLogTerm))
            {
                Interlocked.Increment(ref _appendEntriesRejected);

                long conflictIndex;
                long conflictTerm;

                if (request.PreviousLogIndex > _log.LastIndex)
                {
                    // Simply too short: ask the leader to resume from our end.
                    conflictIndex = _log.LastIndex + 1;
                    conflictTerm = 0;
                }
                else
                {
                    // We have an entry there but with a different term. Report the start of that
                    // whole term so the leader can skip it in one step rather than backing up
                    // one index per round trip.
                    conflictTerm = _log.TermAt(request.PreviousLogIndex);
                    conflictIndex = request.PreviousLogIndex;
                    while (conflictIndex > 1 && _log.TermAt(conflictIndex - 1) == conflictTerm)
                    {
                        conflictIndex--;
                    }
                }

                return new AppendEntriesResponse(
                    _state.CurrentTerm, false, _options.NodeId, 0, conflictTerm, conflictIndex);
            }

            toAppend = request.Entries;
            long matchIndex = request.Entries.Count > 0
                ? request.Entries[^1].Index
                : request.PreviousLogIndex;

            if (request.LeaderCommit > _commitIndex)
            {
                newCommitIndex = Math.Min(request.LeaderCommit, matchIndex);
            }

            response = new AppendEntriesResponse(
                _state.CurrentTerm, true, _options.NodeId, matchIndex, 0, 0);
        }
        finally
        {
            _mutex.Release();
        }

        // The append happens outside the mutex because it fsyncs, and holding a lock across a
        // millisecond-scale syscall would serialise the whole node behind the disk.
        if (toAppend.Count > 0)
        {
            await _log.AppendAsync(toAppend, cancellationToken).ConfigureAwait(false);
        }

        if (newCommitIndex >= 0)
        {
            await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                _commitIndex = Math.Max(_commitIndex, Math.Min(newCommitIndex, _log.LastIndex));
            }
            finally
            {
                _mutex.Release();
            }

            await ApplyCommittedAsync(cancellationToken).ConfigureAwait(false);
        }

        return response;
    }

    // ================================================================ replication

    private async Task ReplicateToAllAsync(CancellationToken cancellationToken)
    {
        if (_options.Peers.Count == 1)
        {
            // Alone in the cluster, so this node is its own majority and everything in its log
            // is committed the moment it is durable.
            await AdvanceCommitIndexAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        var rounds = _options.OtherPeers
            .Select(peer => ReplicateToAsync(peer, cancellationToken))
            .ToArray();

        await Task.WhenAll(rounds).ConfigureAwait(false);
        await AdvanceCommitIndexAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task ReplicateToAsync(string peer, CancellationToken cancellationToken)
    {
        AppendEntriesRequest request;
        long term;

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_role != RaftRole.Leader) return;

            term = _state.CurrentTerm;
            long next = _nextIndex.GetValueOrDefault(peer, _log.LastIndex + 1);
            long previousIndex = next - 1;
            long previousTerm = _log.TermAt(previousIndex);

            var entries = _log.Read(next, _options.MaxEntriesPerAppend);

            request = new AppendEntriesRequest(
                term, _options.NodeId, previousIndex, previousTerm, entries, _commitIndex);
        }
        finally
        {
            _mutex.Release();
        }

        Interlocked.Increment(ref _appendEntriesSent);

        AppendEntriesResponse? response;
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(_options.RpcTimeout);

            response = await _transport
                .AppendEntriesAsync(peer, request, timeout.Token)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return;
        }
        catch (IOException)
        {
            return;
        }

        if (response is null) return;

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (response.Value.Term > _state.CurrentTerm)
            {
                StepDownLocked(response.Value.Term);
                return;
            }

            // A reply that arrives after this node has stopped leading, or from an older term,
            // must be ignored rather than acted on.
            if (_role != RaftRole.Leader || _state.CurrentTerm != term) return;

            if (response.Value.Success)
            {
                _matchIndex[peer] = Math.Max(_matchIndex.GetValueOrDefault(peer), response.Value.MatchIndex);
                _nextIndex[peer] = _matchIndex[peer] + 1;
                return;
            }

            // Back off using the follower's reported conflict point, which skips a whole
            // divergent term per round trip instead of one entry.
            long retryFrom = response.Value.ConflictIndex > 0
                ? response.Value.ConflictIndex
                : Math.Max(1, _nextIndex.GetValueOrDefault(peer, 1) - 1);

            _nextIndex[peer] = Math.Max(1, Math.Min(retryFrom, _log.LastIndex + 1));
        }
        finally
        {
            _mutex.Release();
        }
    }

    /// <summary>
    /// Advances the commit index to the highest entry replicated on a majority.
    /// </summary>
    /// <remarks>
    /// The term check is the part that is easy to get wrong and that matters most. An entry from
    /// an earlier term must not be committed by counting replicas, even when a majority holds
    /// it: there is an interleaving in which that entry is subsequently overwritten, so counting
    /// it as committed can lose data a client was told was durable. Only an entry of the
    /// leader's own term may be committed this way — and committing it commits everything before
    /// it, which is how the earlier entries become safe. This is why a new leader appends a
    /// no-op.
    /// </remarks>
    private async Task AdvanceCommitIndexAsync(CancellationToken cancellationToken)
    {
        bool advanced = false;

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_role != RaftRole.Leader) return;

            long term = _state.CurrentTerm;

            for (long index = _log.LastIndex; index > _commitIndex; index--)
            {
                if (_log.TermAt(index) != term) continue;

                int replicas = 1; // the leader itself
                foreach (string peer in _options.OtherPeers)
                {
                    if (_matchIndex.GetValueOrDefault(peer) >= index) replicas++;
                }

                if (replicas >= _options.QuorumSize)
                {
                    _commitIndex = index;
                    advanced = true;
                    break;
                }
            }
        }
        finally
        {
            _mutex.Release();
        }

        if (advanced)
        {
            await ApplyCommittedAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    // ================================================================ application

    private readonly SemaphoreSlim _applyMutex = new(1, 1);

    /// <summary>
    /// Applies every newly committed entry to the state machine, in order.
    /// </summary>
    /// <remarks>
    /// Guarded by its own mutex rather than the main one, for two reasons. Applying touches the
    /// storage engine and can be slow, and holding the state lock across it would stall
    /// heartbeats and elections. And the state machine must see entries exactly once and in
    /// order, which a dedicated lock guarantees without blocking anything else.
    /// </remarks>
    private async Task ApplyCommittedAsync(CancellationToken cancellationToken)
    {
        await _applyMutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            while (true)
            {
                RaftLogEntry entry;
                long applyingIndex;

                await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    if (_lastApplied >= _commitIndex) return;
                    applyingIndex = _lastApplied + 1;
                    if (applyingIndex > _log.LastIndex) return;
                    entry = _log.ReadAt(applyingIndex);
                }
                finally
                {
                    _mutex.Release();
                }

                if (!entry.IsNoOp)
                {
                    await _stateMachine.ApplyAsync(entry, cancellationToken).ConfigureAwait(false);
                }

                await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    _lastApplied = applyingIndex;
                    Interlocked.Increment(ref _entriesApplied);
                    CompletePendingLocked(applyingIndex, entry.Term);
                }
                finally
                {
                    _mutex.Release();
                }
            }
        }
        finally
        {
            _applyMutex.Release();
        }
    }

    // ================================================================ client proposals

    /// <summary>
    /// Replicates a command and returns once it is committed and applied.
    /// </summary>
    /// <exception cref="NotLeaderException">This node is not the leader.</exception>
    /// <remarks>
    /// Returning only after the entry is <i>applied</i>, not merely committed, is what makes a
    /// subsequent read on this node see the write. Returning at commit would be correct for
    /// durability but would allow a client to write and then fail to read its own write, which
    /// is the kind of surprise that is very hard to debug from the outside.
    /// </remarks>
    public async Task<long> ProposeAsync(byte[] command, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(command);
        ObjectDisposedException.ThrowIf(_disposed, this);

        long term;

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_role != RaftRole.Leader)
            {
                throw new NotLeaderException(_leaderId);
            }

            term = _state.CurrentTerm;
        }
        finally
        {
            _mutex.Release();
        }

        // The log picks the index while holding its own append lock, so two concurrent proposals
        // cannot be assigned the same one.
        var entry = await _log
            .AppendAsLeaderAsync(term, command, isNoOp: false, cancellationToken)
            .ConfigureAwait(false);

        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Leadership or the term may have changed between the role check and here, in which
            // case this entry is not going to commit under this node's authority.
            if (_role != RaftRole.Leader || _state.CurrentTerm != term)
            {
                throw new NotLeaderException(_leaderId);
            }

            // The entry may already have committed and been applied while the waiter was being
            // created, in which case there is nothing to wait for.
            if (_lastApplied >= entry.Index)
            {
                return entry.Index;
            }

            if (!_pending.TryGetValue(entry.Index, out var waiters))
            {
                waiters = [];
                _pending[entry.Index] = waiters;
            }
            waiters.Add((term, completion));
        }
        finally
        {
            _mutex.Release();
        }

        // Replicate immediately rather than waiting for the next heartbeat, so a write costs one
        // round trip instead of up to a heartbeat interval.
        await ReplicateToAllAsync(cancellationToken).ConfigureAwait(false);

        using var registration = cancellationToken.Register(
            () => completion.TrySetCanceled(cancellationToken));

        await completion.Task.ConfigureAwait(false);
        return entry.Index;
    }

    /// <summary>
    /// Completes the waiters for an applied entry, and fails any whose term does not match.
    /// </summary>
    /// <remarks>
    /// A waiter registered for index N in term T is only satisfied by the entry at index N
    /// <i>of term T</i>. If a later leader put a different entry there, the original proposal
    /// never committed and the client must be told so rather than being handed a false success.
    /// </remarks>
    private void CompletePendingLocked(long index, long appliedTerm)
    {
        if (!_pending.Remove(index, out var waiters)) return;

        foreach (var (term, completion) in waiters)
        {
            if (term == appliedTerm)
            {
                completion.TrySetResult();
            }
            else
            {
                completion.TrySetException(new NotLeaderException(_leaderId));
            }
        }
    }

    private void FailPendingLocked(Exception exception)
    {
        foreach (var (_, waiters) in _pending)
        {
            foreach (var (_, completion) in waiters)
            {
                completion.TrySetException(exception);
            }
        }
        _pending.Clear();
    }

    // ================================================================ teardown

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        await _shutdown.CancelAsync().ConfigureAwait(false);

        if (_driver is not null)
        {
            try
            {
                await _driver.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected.
            }
        }

        await _mutex.WaitAsync().ConfigureAwait(false);
        try
        {
            FailPendingLocked(new NotLeaderException(null));
        }
        finally
        {
            _mutex.Release();
        }

        await _log.DisposeAsync().ConfigureAwait(false);
        _shutdown.Dispose();
        _mutex.Dispose();
        _applyMutex.Dispose();
    }
}

/// <summary>A snapshot of one node's consensus state.</summary>
public sealed record RaftStats
{
    /// <summary>The node these numbers describe.</summary>
    public required string NodeId { get; init; }

    /// <summary>Its current role.</summary>
    public required RaftRole Role { get; init; }

    /// <summary>Its current term.</summary>
    public long Term { get; init; }

    /// <summary>The leader it last heard from.</summary>
    public string? LeaderId { get; init; }

    /// <summary>Highest committed index.</summary>
    public long CommitIndex { get; init; }

    /// <summary>Highest index applied to the state machine.</summary>
    public long LastApplied { get; init; }

    /// <summary>Index of its last log entry.</summary>
    public long LastLogIndex { get; init; }

    /// <summary>Term of its last log entry.</summary>
    public long LastLogTerm { get; init; }

    /// <summary>Bytes its log occupies.</summary>
    public long LogSizeBytes { get; init; }

    /// <summary>Elections this node has started.</summary>
    public long ElectionsStarted { get; init; }

    /// <summary>Elections this node has won.</summary>
    public long ElectionsWon { get; init; }

    /// <summary>AppendEntries messages sent as leader.</summary>
    public long AppendEntriesSent { get; init; }

    /// <summary>AppendEntries messages this node rejected as follower.</summary>
    public long AppendEntriesRejected { get; init; }

    /// <summary>Entries applied to the state machine.</summary>
    public long EntriesApplied { get; init; }

    /// <summary>Per-peer replication progress, when this node is the leader.</summary>
    public IReadOnlyDictionary<string, long> MatchIndex { get; init; } =
        new Dictionary<string, long>();
}
