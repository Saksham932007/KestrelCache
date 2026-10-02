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
    private readonly RaftSnapshotStore _snapshots;
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

    /// <summary>
    /// Servers removed by a membership change, mapped to the log index of the configuration entry
    /// that removed them.
    /// </summary>
    /// <remarks>
    /// A removed server has to be told it was removed. Dropping it the instant the final
    /// configuration is appended leaves it believing it is still a voter — in the joint
    /// configuration, no less — so it keeps timing out and campaigning, bumping the term on every
    /// attempt and forcing the healthy cluster to react. It cannot win, but it can be
    /// persistently disruptive: in testing, two such servers drove a settled three-node cluster
    /// from term 1 to term 22.
    ///
    /// So replication continues to a departed server until it has acknowledged the entry that
    /// removed it, at which point it stops campaigning of its own accord and can be shut down.
    /// Its acknowledgements are never counted toward a quorum, because quorum is asked of the
    /// configuration and it is no longer in it.
    /// </remarks>
    private readonly Dictionary<string, long> _departing = [];

    private RaftRole _role = RaftRole.Follower;
    private RaftConfiguration _configuration;
    private long _configurationIndex;
    private int _completingTransition;
    private string? _leaderId;
    private long _commitIndex;
    private long _lastApplied;
    private DateTime _lastHeardFromLeader = DateTime.UtcNow;
    private TimeSpan _currentElectionTimeout;
    private Task? _driver;
    private volatile bool _disposed;

    private void Trace(string message) =>
        _options.Trace?.Invoke($"[{_options.NodeId}] {message}");

    /// <summary>
    /// Every server this node must replicate to under the active configuration.
    /// </summary>
    /// <remarks>
    /// Derived from the configuration rather than from the options, and it includes the outgoing
    /// voters during a joint configuration. A leader that stopped replicating to the servers it
    /// is transitioning away from could never commit the joint entry, because committing it
    /// requires a majority of exactly that set — so the membership change would wedge.
    /// </remarks>
    private IReadOnlyList<string> PeersLocked() =>
        [.. _configuration.AllServers
            .Concat(_departing.Keys)
            .Distinct()
            .Where(peer => peer != _options.NodeId)];

    /// <summary>True when this node alone constitutes a quorum.</summary>
    private bool IsSingleNodeLocked() =>
        _configuration.AllServers.Count == 1
        && _configuration.AllServers[0] == _options.NodeId;

    private long _electionsStarted;
    private long _electionsWon;
    private long _appendEntriesSent;
    private long _appendEntriesRejected;
    private long _entriesApplied;
    private long _snapshotsTaken;
    private long _snapshotsInstalled;
    private long _snapshotChunksSent;
    private long _membershipChanges;

    private int _snapshotInProgress;
    private IncomingSnapshot? _incoming;

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
        _snapshots = new RaftSnapshotStore(options.DataDirectory);

        // Recovery order matters. The snapshot establishes the baseline -- state machine
        // contents, commit position and membership -- and the log's surviving entries are
        // replayed on top of it. Reading the configuration from the options instead would
        // resurrect the bootstrap membership and discard every change since, which is how a node
        // comes back up disagreeing with the cluster about who may vote.
        var snapshotMetadata = _snapshots.TryReadMetadata();

        if (snapshotMetadata is { } metadata)
        {
            RestoreSnapshotOnStartup(metadata);
            _configuration = metadata.Configuration;
            _configurationIndex = metadata.LastIncludedIndex;
            _lastApplied = metadata.LastIncludedIndex;
            _commitIndex = metadata.LastIncludedIndex;
            Recovery = new RaftRecoveryReport
            {
                SnapshotRestored = true,
                SnapshotIndex = metadata.LastIncludedIndex,
                SnapshotTerm = metadata.LastIncludedTerm,
            };
        }
        else
        {
            Recovery = new RaftRecoveryReport();
        }

        // A configuration entry later in the log supersedes the snapshot's.
        if (_log.FindLatestConfiguration(_log.LastIndex) is { } fromLog)
        {
            _configuration = fromLog.Configuration;
            _configurationIndex = fromLog.Index;
        }

        _configuration ??= options.BootstrapConfiguration;

        Recovery = Recovery with
        {
            LogEntriesRecovered = _log.Count,
            BytesDiscardedAtStartup = _log.BytesDiscardedAtStartup,
            Configuration = _configuration,
        };

        _currentElectionTimeout = NextElectionTimeout();
    }

    /// <summary>
    /// Restores the state machine from a snapshot during construction.
    /// </summary>
    /// <remarks>
    /// Synchronous because it runs in the constructor, before the node participates in anything.
    /// Doing it lazily would leave a window in which the node could vote or serve a read against
    /// state it had not yet loaded.
    /// </remarks>
    private void RestoreSnapshotOnStartup(RaftSnapshotMetadata metadata)
    {
        var (_, payload) = _snapshots.OpenForRestore();
        using (payload)
        {
            _stateMachine.RestoreSnapshotAsync(payload, CancellationToken.None)
                .AsTask().GetAwaiter().GetResult();
        }

        Trace(
            $"restored snapshot through index {metadata.LastIncludedIndex}@"
                + $"{metadata.LastIncludedTerm}, configuration {metadata.Configuration}");
    }

    /// <summary>What startup recovery found and did.</summary>
    public RaftRecoveryReport Recovery { get; private set; }

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
                LogEntryCount = _log.Count,
                FirstLogIndex = _log.FirstIndex,
                SnapshotIndex = _log.SnapshotIndex,
                SnapshotTerm = _log.SnapshotTerm,
                SnapshotSizeBytes = _snapshots.SizeBytes,
                SnapshotsTaken = Interlocked.Read(ref _snapshotsTaken),
                SnapshotsInstalled = Interlocked.Read(ref _snapshotsInstalled),
                SnapshotChunksSent = Interlocked.Read(ref _snapshotChunksSent),
                MembershipChanges = Interlocked.Read(ref _membershipChanges),
                Configuration = _configuration,
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

        await CompleteCommittedTransitionAsync(cancellationToken).ConfigureAwait(false);

        bool snapshotDue;
        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            snapshotDue = ShouldSnapshotLocked();
        }
        finally
        {
            _mutex.Release();
        }

        if (snapshotDue)
        {
            await CreateSnapshotAsync(cancellationToken).ConfigureAwait(false);
        }
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
        RaftConfiguration configuration;
        IReadOnlyList<string> peers;

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_role == RaftRole.Leader) return;

            // A server that is not a voter under the active configuration must not campaign. Two
            // cases reach here: a server joining a cluster, which starts with no configuration
            // and is waiting to learn it, and a server that has been removed and has not yet
            // been shut down. Neither can win, and both would disturb a healthy cluster by
            // forcing term increments it has to react to.
            if (!_configuration.Contains(_options.NodeId))
            {
                return;
            }

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
            configuration = _configuration;
            peers = PeersLocked();

            Interlocked.Increment(ref _electionsStarted);
            Trace(
                $"became candidate in term {term} (log {lastLogIndex}@{lastLogTerm}, "
                    + $"configuration {configuration})");
        }
        finally
        {
            _mutex.Release();
        }

        // A genuine single-node cluster is its own majority and needs no round trip. Reached
        // only when this node is the sole voter, which the check above has already established.
        if (peers.Count == 0)
        {
            await BecomeLeaderAsync(term, cancellationToken).ConfigureAwait(false);
            return;
        }

        var request = new RequestVoteRequest(term, _options.NodeId, lastLogIndex, lastLogTerm);
        var voters = new HashSet<string>(StringComparer.Ordinal) { _options.NodeId };

        var ballots = peers
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
                Trace($"got vote from {peer} in term {term} ({voters.Count} so far)");

                // Quorum is asked of the configuration, not computed here, so the joint rule --
                // a majority of both old and new voters during a membership change -- applies
                // uniformly and cannot be forgotten at one call site.
                if (configuration.HasQuorum(voters))
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
            _departing.Clear();
            foreach (string peer in PeersLocked())
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
        await _log.AppendAsLeaderAsync(term, RaftEntryKind.NoOp, [], cancellationToken)
            .ConfigureAwait(false);

        await ReplicateToAllAsync(cancellationToken).ConfigureAwait(false);

        // A joint configuration inherited from a previous leader is completed by
        // CompleteCommittedTransitionAsync on the next tick, so nothing special is needed here.
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

                if (request.PreviousLogIndex < _log.SnapshotIndex)
                {
                    // Below our snapshot boundary. The discarded prefix is committed state, so we
                    // already agree with the leader further ahead than it is asking about; point
                    // it at the first index we can actually verify.
                    conflictIndex = _log.SnapshotIndex + 1;
                    conflictTerm = 0;
                }
                else if (request.PreviousLogIndex > _log.LastIndex)
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
                    while (conflictIndex > _log.FirstIndex
                        && _log.TermAt(conflictIndex - 1) == conflictTerm)
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
            await AdoptConfigurationFromAsync(toAppend, cancellationToken).ConfigureAwait(false);
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
        IReadOnlyList<string> peers;
        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_role != RaftRole.Leader) return;
            peers = PeersLocked();
        }
        finally
        {
            _mutex.Release();
        }

        if (peers.Count == 0)
        {
            // Alone in the cluster, so this node is its own majority and everything in its log
            // is committed the moment it is durable.
            await AdvanceCommitIndexAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        var rounds = peers
            .Select(peer => ReplicateToAsync(peer, cancellationToken))
            .ToArray();

        await Task.WhenAll(rounds).ConfigureAwait(false);
        await AdvanceCommitIndexAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task ReplicateToAsync(string peer, CancellationToken cancellationToken)
    {
        AppendEntriesRequest request;
        long term;
        bool needsSnapshot;

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_role != RaftRole.Leader) return;

            term = _state.CurrentTerm;
            long next = _nextIndex.GetValueOrDefault(peer, _log.LastIndex + 1);

            // The gap snapshotting opens: a follower this far behind needs entries the leader has
            // already discarded, so AppendEntries can never succeed for it however far back
            // nextIndex is wound. The leader has to send the state instead of the history.
            needsSnapshot = next <= _log.SnapshotIndex;

            if (needsSnapshot)
            {
                request = default;
            }
            else
            {
                long previousIndex = next - 1;
                long previousTerm = _log.TermAt(previousIndex);
                var entries = _log.Read(next, _options.MaxEntriesPerAppend);

                request = new AppendEntriesRequest(
                    term, _options.NodeId, previousIndex, previousTerm, entries, _commitIndex);
            }
        }
        finally
        {
            _mutex.Release();
        }

        if (needsSnapshot)
        {
            await SendSnapshotAsync(peer, term, cancellationToken).ConfigureAwait(false);
            return;
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

                // Once a departed server has acknowledged the entry that removed it, it knows it
                // is out and will stop campaigning, so there is nothing left to send it.
                if (_departing.TryGetValue(peer, out long requiredIndex)
                    && _matchIndex[peer] >= requiredIndex)
                {
                    _departing.Remove(peer);
                    _nextIndex.Remove(peer);
                    _matchIndex.Remove(peer);
                    Trace($"{peer} has acknowledged its removal; no longer replicating to it");
                }

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

                var replicas = new HashSet<string>(StringComparer.Ordinal) { _options.NodeId };
                foreach (string peer in PeersLocked())
                {
                    if (_matchIndex.GetValueOrDefault(peer) >= index) replicas.Add(peer);
                }

                if (_configuration.HasQuorum(replicas))
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

                // Only commands reach the state machine. A no-op carries nothing, and a
                // configuration entry is consumed by the consensus module itself -- it was
                // already adopted when it was appended, because waiting for commitment would
                // mean counting quorums under a superseded membership.
                if (entry.Kind == RaftEntryKind.Command)
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

    // ================================================================ configuration

    /// <summary>
    /// Adopts the newest configuration among freshly appended entries.
    /// </summary>
    /// <remarks>
    /// A configuration takes effect when it is <b>appended</b>, not when it commits. That is not
    /// an optimisation — waiting for commitment would mean counting votes and replicas under a
    /// configuration the node has already superseded, which reopens the very window joint
    /// consensus exists to close.
    /// </remarks>
    private async Task AdoptConfigurationFromAsync(
        IReadOnlyList<RaftLogEntry> entries,
        CancellationToken cancellationToken)
    {
        RaftConfiguration? adopted = null;
        long adoptedIndex = 0;

        foreach (var entry in entries)
        {
            if (entry.Kind == RaftEntryKind.Configuration)
            {
                adopted = entry.AsConfiguration();
                adoptedIndex = entry.Index;
            }
        }

        if (adopted is null) return;

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            SetConfigurationLocked(adopted, adoptedIndex);
        }
        finally
        {
            _mutex.Release();
        }
    }

    private void SetConfigurationLocked(RaftConfiguration configuration, long index)
    {
        var previous = _configuration;
        _configuration = configuration;
        _configurationIndex = index;
        Trace($"configuration {previous} -> {configuration} (at index {index})");

        if (_role != RaftRole.Leader) return;

        // Replication bookkeeping has to follow the configuration: a newly added server needs
        // nextIndex seeded so the leader starts probing it, and a removed one's entries would
        // otherwise linger and be counted toward quorums it is no longer part of.
        var servers = configuration.AllServers.Where(s => s != _options.NodeId).ToHashSet();

        foreach (string peer in servers)
        {
            if (!_nextIndex.ContainsKey(peer))
            {
                _nextIndex[peer] = _log.LastIndex + 1;
                _matchIndex[peer] = 0;
                Trace($"tracking new peer {peer} from index {_nextIndex[peer]}");
            }
        }

        foreach (string departed in _nextIndex.Keys.Where(k => !servers.Contains(k)).ToList())
        {
            // Kept, not dropped: it still has to learn that it was removed. See the remarks on
            // _departing.
            _departing[departed] = index;
            Trace($"{departed} has departed; replicating index {index} to it so it learns");
        }

        // A server that rejoins is no longer departing.
        foreach (string rejoined in servers.Where(_departing.ContainsKey).ToList())
        {
            _departing.Remove(rejoined);
        }
    }

    /// <summary>The configuration currently in force.</summary>
    public RaftConfiguration Configuration
    {
        get
        {
            _mutex.Wait();
            try { return _configuration; }
            finally { _mutex.Release(); }
        }
    }

    /// <summary>
    /// Changes cluster membership to <paramref name="newVoters"/>, via joint consensus.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Two phases, both of which must commit. First a joint entry naming the old and new voter
    /// sets, during which every decision needs a majority of both; then an entry naming the new
    /// set alone. See <see cref="RaftConfiguration"/> for why the intermediate step is necessary
    /// rather than merely cautious.
    /// </para>
    /// <para>
    /// Returns once the final configuration has committed, so a caller that sees success knows
    /// the change is durable and no longer depends on this node remaining leader.
    /// </para>
    /// </remarks>
    /// <exception cref="NotLeaderException">This node is not the leader.</exception>
    public async Task ChangeMembershipAsync(
        IReadOnlyList<string> newVoters,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(newVoters);
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (newVoters.Count == 0)
        {
            throw new ArgumentException(
                "A configuration must contain at least one voter.", nameof(newVoters));
        }

        RaftConfiguration joint;

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_role != RaftRole.Leader) throw new NotLeaderException(_leaderId);

            if (_configuration.IsJoint)
            {
                throw new InvalidOperationException(
                    "A membership change is already in progress; wait for it to commit.");
            }

            if (_configuration.Voters.Count == newVoters.Count
                && newVoters.All(_configuration.Voters.Contains))
            {
                return; // already there
            }

            joint = _configuration.BeginTransitionTo(newVoters);
        }
        finally
        {
            _mutex.Release();
        }

        Trace($"membership change: entering {joint}");
        await ProposeConfigurationAsync(joint, cancellationToken).ConfigureAwait(false);

        // Only once the joint entry has committed -- under both majorities -- is it safe to move
        // to the new configuration alone. The tick-driven completion may have already done this,
        // in which case there is nothing left to do.
        await CompleteCommittedTransitionAsync(cancellationToken).ConfigureAwait(false);

        // Wait for the transition to actually clear, since the completion may be running on the
        // tick rather than on this call.
        for (int attempt = 0; attempt < 200 && Configuration.IsJoint; attempt++)
        {
            await Task.Delay(5, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Adds a server to the cluster.</summary>
    public Task AddServerAsync(string nodeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);

        var target = Configuration.Voters.ToList();
        if (target.Contains(nodeId)) return Task.CompletedTask;

        target.Add(nodeId);
        return ChangeMembershipAsync(target, cancellationToken);
    }

    /// <summary>Removes a server from the cluster.</summary>
    public Task RemoveServerAsync(string nodeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);

        var target = Configuration.Voters.Where(v => v != nodeId).ToList();
        if (target.Count == Configuration.Voters.Count) return Task.CompletedTask;

        if (target.Count == 0)
        {
            throw new InvalidOperationException(
                "Refusing to remove the last voter; the cluster would have no one to elect.");
        }

        return ChangeMembershipAsync(target, cancellationToken);
    }

    private async Task ProposeConfigurationAsync(
        RaftConfiguration configuration,
        CancellationToken cancellationToken)
    {
        long term;

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_role != RaftRole.Leader) throw new NotLeaderException(_leaderId);
            term = _state.CurrentTerm;
        }
        finally
        {
            _mutex.Release();
        }

        var entry = await _log
            .AppendAsLeaderAsync(
                term,
                RaftEntryKind.Configuration,
                RaftConfigurationCodec.Encode(configuration),
                cancellationToken)
            .ConfigureAwait(false);

        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_role != RaftRole.Leader || _state.CurrentTerm != term)
            {
                throw new NotLeaderException(_leaderId);
            }

            // Effective on append, before it commits.
            SetConfigurationLocked(configuration, entry.Index);

            if (_lastApplied >= entry.Index) return;

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

        await ReplicateToAllAsync(cancellationToken).ConfigureAwait(false);

        using var registration = cancellationToken.Register(
            () => completion.TrySetCanceled(cancellationToken));

        await completion.Task.ConfigureAwait(false);
    }

    /// <summary>
    /// Appends the final configuration once a joint one has committed, whatever left it joint.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Driven from the tick rather than only from <see cref="ChangeMembershipAsync"/>, because
    /// the second phase can be abandoned in ways that call has no say over: the caller cancels,
    /// the caller's process dies, or the leader that started the change is replaced. Any of those
    /// used to leave the cluster joint indefinitely — not broken, but permanently requiring two
    /// majorities, so tolerating fewer failures than either configuration alone, and with no
    /// mechanism to ever recover.
    /// </para>
    /// <para>
    /// Making this the leader's standing responsibility rather than a step in one method means a
    /// half-finished change always completes, which is the behaviour an operator would assume
    /// anyway.
    /// </para>
    /// </remarks>
    private async Task CompleteCommittedTransitionAsync(CancellationToken cancellationToken)
    {
        RaftConfiguration final;

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_role != RaftRole.Leader || !_configuration.IsJoint) return;

            // Only once the joint entry itself has committed. Appending the final configuration
            // before then would skip the phase that makes the change safe.
            if (_commitIndex < _configurationIndex) return;

            final = _configuration.CompleteTransition();
        }
        finally
        {
            _mutex.Release();
        }

        // One attempt at a time; the tick will come round again if this one loses leadership.
        if (Interlocked.Exchange(ref _completingTransition, 1) != 0) return;

        try
        {
            Trace($"joint configuration has committed; settling on {final}");
            await ProposeConfigurationAsync(final, cancellationToken).ConfigureAwait(false);
            Interlocked.Increment(ref _membershipChanges);

            await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (_role == RaftRole.Leader && !_configuration.ContainsIncoming(_options.NodeId))
                {
                    Trace("stepping down: this node is no longer a voter");
                    _role = RaftRole.Follower;
                    _leaderId = null;
                    FailPendingLocked(new NotLeaderException(null));
                }
            }
            finally
            {
                _mutex.Release();
            }
        }
        catch (Exception exception) when (
            exception is NotLeaderException or OperationCanceledException
                or ObjectDisposedException)
        {
            // Leadership moved on; whoever leads next picks it up on their own tick.
        }
        finally
        {
            Interlocked.Exchange(ref _completingTransition, 0);
        }
    }

    // ================================================================ snapshots

    /// <summary>
    /// Captures the state machine, installs the snapshot, and discards the log prefix it
    /// replaces.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The ordering is the crash-safety argument: capture, make the snapshot durable, and only
    /// then discard the entries. Reversing the last two steps means a crash in between loses the
    /// history <i>and</i> the state meant to replace it, which is unrecoverable — and it is a
    /// mistake that only shows up as data loss under a crash, never in ordinary testing.
    /// </para>
    /// <para>
    /// Snapshots are taken at <see cref="LastApplied"/>, never at the commit index. An entry that
    /// is committed but not yet applied is not in the state machine, so including it would
    /// produce a snapshot that claims to cover state it does not contain.
    /// </para>
    /// </remarks>
    public async Task<RaftSnapshotMetadata?> CreateSnapshotAsync(
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (Interlocked.Exchange(ref _snapshotInProgress, 1) != 0) return null;

        try
        {
            // The apply mutex is held for the whole capture, so the state machine is quiesced and
            // the image is a consistent view of exactly lastApplied.
            await _applyMutex.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                long index;
                long term;
                RaftConfiguration configuration;

                await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    index = _lastApplied;
                    if (index <= _log.SnapshotIndex) return null; // nothing new to fold in

                    term = _log.TermAt(index);
                    configuration = _configuration;
                }
                finally
                {
                    _mutex.Release();
                }

                var metadata = new RaftSnapshotMetadata(index, term, configuration);
                await _snapshots.WriteAsync(metadata, _stateMachine, cancellationToken)
                    .ConfigureAwait(false);

                await _log.DiscardPrefixAsync(index, cancellationToken).ConfigureAwait(false);

                Interlocked.Increment(ref _snapshotsTaken);
                Trace(
                    $"snapshot at index {index}@{term}, log prefix discarded, "
                        + $"{_snapshots.SizeBytes} byte(s) on disk");

                return metadata;
            }
            finally
            {
                _applyMutex.Release();
            }
        }
        finally
        {
            Interlocked.Exchange(ref _snapshotInProgress, 0);
        }
    }

    private bool ShouldSnapshotLocked()
    {
        if (_options.SnapshotThresholdEntries > 0
            && _log.Count >= _options.SnapshotThresholdEntries
            && _lastApplied > _log.SnapshotIndex)
        {
            return true;
        }

        return _options.SnapshotThresholdBytes > 0
            && _log.SizeBytes >= _options.SnapshotThresholdBytes
            && _lastApplied > _log.SnapshotIndex;
    }

    /// <summary>Streams the snapshot to a follower that is too far behind for AppendEntries.</summary>
    private async Task SendSnapshotAsync(
        string peer,
        long term,
        CancellationToken cancellationToken)
    {
        if (!_snapshots.Exists)
        {
            // Nothing to send. The follower's nextIndex is below our log start but we have no
            // snapshot either, which can only happen transiently; the next round will retry.
            return;
        }

        var metadata = _snapshots.TryReadMetadata();
        if (metadata is not { } snapshot) return;

        byte[] configuration = RaftConfigurationCodec.Encode(snapshot.Configuration);

        await using var source = _snapshots.OpenForSending();
        long offset = 0;
        byte[] buffer = new byte[_options.SnapshotChunkBytes];

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            int read = await source.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
            bool done = read == 0 || source.Position >= source.Length;

            var request = new InstallSnapshotRequest(
                term,
                _options.NodeId,
                snapshot.LastIncludedIndex,
                snapshot.LastIncludedTerm,
                configuration,
                offset,
                read == buffer.Length ? buffer : buffer[..read],
                done);

            InstallSnapshotResponse? response;
            try
            {
                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                timeout.CancelAfter(_options.RpcTimeout);

                response = await _transport
                    .InstallSnapshotAsync(peer, request, timeout.Token)
                    .ConfigureAwait(false);
            }
            catch (Exception exception) when (
                exception is OperationCanceledException or IOException)
            {
                return; // retried on the next replication round
            }

            Interlocked.Increment(ref _snapshotChunksSent);

            if (response is null) return;

            if (response.Value.Term > term)
            {
                await StepDownAsync(response.Value.Term, cancellationToken).ConfigureAwait(false);
                return;
            }

            if (!response.Value.Success) return;

            if (done)
            {
                await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    if (_role != RaftRole.Leader || _state.CurrentTerm != term) return;

                    // The follower now holds everything through the snapshot, so replication
                    // resumes from the entry after it.
                    _matchIndex[peer] = Math.Max(
                        _matchIndex.GetValueOrDefault(peer), snapshot.LastIncludedIndex);
                    _nextIndex[peer] = _matchIndex[peer] + 1;
                    Trace($"{peer} installed the snapshot; resuming at {_nextIndex[peer]}");
                }
                finally
                {
                    _mutex.Release();
                }

                return;
            }

            // The follower reports how much it holds, so a partially-received transfer resumes
            // rather than restarting.
            offset = response.Value.BytesReceived;
            source.Position = offset;
        }
    }

    /// <summary>Receives one chunk of a snapshot from the leader.</summary>
    /// <remarks>
    /// Chunks are accumulated into a side file and installed only when the final one arrives, so
    /// a transfer interrupted halfway leaves the node's existing state untouched rather than
    /// half-replaced.
    /// </remarks>
    public async Task<InstallSnapshotResponse> HandleInstallSnapshotAsync(
        InstallSnapshotRequest request,
        CancellationToken cancellationToken = default)
    {
        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (request.Term < _state.CurrentTerm)
            {
                return new InstallSnapshotResponse(
                    _state.CurrentTerm, _options.NodeId, 0, Success: false);
            }

            if (request.Term > _state.CurrentTerm)
            {
                StepDownLocked(request.Term);
            }
            else if (_role != RaftRole.Follower)
            {
                _role = RaftRole.Follower;
                FailPendingLocked(new NotLeaderException(request.LeaderId));
            }

            _leaderId = request.LeaderId;
            _lastHeardFromLeader = DateTime.UtcNow;
            _currentElectionTimeout = NextElectionTimeout();
        }
        finally
        {
            _mutex.Release();
        }

        // Offset zero starts a fresh transfer, discarding any partial one. A leader restarts from
        // zero after a failure, so this is how a stalled transfer is abandoned.
        if (request.Offset == 0)
        {
            _incoming?.Dispose();
            _incoming = new IncomingSnapshot(
                Path.Combine(_options.DataDirectory, "snapshot.incoming"));
        }

        if (_incoming is null || _incoming.Length != request.Offset)
        {
            // Out of order: tell the leader what we actually hold so it can resume correctly.
            return new InstallSnapshotResponse(
                _state.CurrentTerm, _options.NodeId, _incoming?.Length ?? 0, Success: true);
        }

        await _incoming.AppendAsync(request.Data, cancellationToken).ConfigureAwait(false);

        if (!request.Done)
        {
            return new InstallSnapshotResponse(
                _state.CurrentTerm, _options.NodeId, _incoming.Length, Success: true);
        }

        string assembled = _incoming.Path;
        _incoming.Complete();
        _incoming = null;

        await InstallReceivedSnapshotAsync(request, assembled, cancellationToken)
            .ConfigureAwait(false);

        return new InstallSnapshotResponse(
            _state.CurrentTerm, _options.NodeId, request.Offset + request.Data.Length, Success: true);
    }

    private async Task InstallReceivedSnapshotAsync(
        InstallSnapshotRequest request,
        string assembledPath,
        CancellationToken cancellationToken)
    {
        // The apply mutex is held across the restore so the state machine cannot be mutated by
        // ordinary application while it is being replaced wholesale.
        await _applyMutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _snapshots.InstallFromFile(assembledPath);

            var (metadata, payload) = _snapshots.OpenForRestore();
            await using (payload)
            {
                await _stateMachine.RestoreSnapshotAsync(payload, cancellationToken)
                    .ConfigureAwait(false);
            }

            // Everything is discarded, not merely the prefix: a follower far enough behind to
            // need a snapshot may hold entries that were never committed and have since been
            // overwritten, and keeping them would leave it permanently divergent.
            await _log
                .ResetToSnapshotAsync(
                    metadata.LastIncludedIndex, metadata.LastIncludedTerm, cancellationToken)
                .ConfigureAwait(false);

            await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                _lastApplied = Math.Max(_lastApplied, metadata.LastIncludedIndex);
                _commitIndex = Math.Max(_commitIndex, metadata.LastIncludedIndex);
                SetConfigurationLocked(metadata.Configuration, metadata.LastIncludedIndex);
            }
            finally
            {
                _mutex.Release();
            }

            Interlocked.Increment(ref _snapshotsInstalled);
            Trace(
                $"installed snapshot from {request.LeaderId} through index "
                    + $"{metadata.LastIncludedIndex}@{metadata.LastIncludedTerm}");
        }
        finally
        {
            _applyMutex.Release();
        }
    }

    /// <summary>A snapshot being received in chunks.</summary>
    private sealed class IncomingSnapshot : IDisposable
    {
        private readonly FileStream _stream;

        internal IncomingSnapshot(string path)
        {
            Path = path;
            _stream = new FileStream(
                path,
                new FileStreamOptions
                {
                    Mode = FileMode.Create,
                    Access = FileAccess.Write,
                    Share = FileShare.None,
                    BufferSize = 64 * 1024,
                });
        }

        internal string Path { get; }

        internal long Length => _stream.Length;

        internal async ValueTask AppendAsync(byte[] data, CancellationToken cancellationToken)
        {
            await _stream.WriteAsync(data, cancellationToken).ConfigureAwait(false);
            await _stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        internal void Complete()
        {
            _stream.Flush(flushToDisk: true);
            _stream.Dispose();
        }

        public void Dispose()
        {
            try
            {
                _stream.Dispose();
                File.Delete(Path);
            }
            catch (IOException)
            {
                // A stale partial snapshot is harmless; the next transfer overwrites it.
            }
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
            .AppendAsLeaderAsync(term, RaftEntryKind.Command, command, cancellationToken)
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

        _incoming?.Dispose();
        _incoming = null;

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

    /// <summary>Entries the log currently holds, excluding anything folded into a snapshot.</summary>
    public long LogEntryCount { get; init; }

    /// <summary>Lowest index still held as a log entry.</summary>
    public long FirstLogIndex { get; init; }

    /// <summary>Last index covered by the snapshot; zero when none has been taken.</summary>
    public long SnapshotIndex { get; init; }

    /// <summary>Term of the entry at <see cref="SnapshotIndex"/>.</summary>
    public long SnapshotTerm { get; init; }

    /// <summary>Size of the snapshot file.</summary>
    public long SnapshotSizeBytes { get; init; }

    /// <summary>Snapshots this node has taken of its own state machine.</summary>
    public long SnapshotsTaken { get; init; }

    /// <summary>Snapshots this node has received from a leader and installed.</summary>
    public long SnapshotsInstalled { get; init; }

    /// <summary>Snapshot chunks this node has sent as leader.</summary>
    public long SnapshotChunksSent { get; init; }

    /// <summary>Membership changes this node has driven to completion as leader.</summary>
    public long MembershipChanges { get; init; }

    /// <summary>The cluster configuration in force.</summary>
    public RaftConfiguration? Configuration { get; init; }
}
