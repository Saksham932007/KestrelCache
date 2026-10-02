namespace KestrelCache.Raft;

/// <summary>Which of the three roles a node is currently in.</summary>
public enum RaftRole
{
    /// <summary>Passive: replicates whatever the leader sends and votes when asked.</summary>
    Follower,

    /// <summary>Standing for election after not hearing from a leader.</summary>
    Candidate,

    /// <summary>Accepting client writes and replicating them.</summary>
    Leader,
}

/// <summary>What a log entry represents.</summary>
/// <remarks>
/// A single byte rather than a pair of booleans, because the set is closed and mutually
/// exclusive: an entry is exactly one of these three things. The earlier version carried an
/// <c>IsNoOp</c> flag, which worked until configuration entries arrived and a second boolean
/// would have admitted the nonsensical combination of both.
/// </remarks>
public enum RaftEntryKind : byte
{
    /// <summary>An opaque state-machine command.</summary>
    Command = 0,

    /// <summary>
    /// The entry a new leader appends on election, carrying no payload. See
    /// <see cref="RaftLogEntry"/> for why it exists.
    /// </summary>
    NoOp = 1,

    /// <summary>
    /// A cluster configuration. Applied to the consensus module rather than the state machine,
    /// and takes effect when appended rather than when committed.
    /// </summary>
    Configuration = 2,
}

/// <summary>One entry in the replicated log.</summary>
/// <param name="Index">Position in the log, starting at 1.</param>
/// <param name="Term">The term of the leader that created this entry.</param>
/// <param name="Kind">What this entry represents.</param>
/// <param name="Command">
/// The state-machine command for <see cref="RaftEntryKind.Command"/>, the encoded configuration
/// for <see cref="RaftEntryKind.Configuration"/>, and empty for a no-op.
/// </param>
/// <remarks>
/// <para>
/// The <paramref name="Term"/> is not decoration. Raft's whole safety argument rests on the
/// pair (index, term) identifying an entry uniquely: if two logs contain an entry with the same
/// index and term, then the entire prefix up to that point is identical. That property is what
/// lets a leader bring a divergent follower back into line by comparing one entry rather than
/// the whole log.
/// </para>
/// <para>
/// The no-op entry exists for a subtler reason. A newly elected leader may hold entries from
/// previous terms that are replicated on a majority but not yet committed, and Raft forbids it
/// from committing them by counting replicas — doing so can lose a committed entry in a
/// specific interleaving (the scenario in figure 8 of the Raft paper). The fix is for the leader
/// to append one entry of its own term and commit that; committing an entry implicitly commits
/// everything before it, so the stale prefix becomes committed safely as a side effect.
/// </para>
/// </remarks>
public readonly record struct RaftLogEntry(
    long Index,
    long Term,
    RaftEntryKind Kind,
    byte[] Command)
{
    /// <summary>True when this entry carries no state-machine command.</summary>
    public bool IsNoOp => Kind == RaftEntryKind.NoOp;

    /// <summary>True when this entry carries a cluster configuration.</summary>
    public bool IsConfiguration => Kind == RaftEntryKind.Configuration;

    /// <summary>Creates a client command entry.</summary>
    public static RaftLogEntry Command_(long index, long term, byte[] command) =>
        new(index, term, RaftEntryKind.Command, command);

    /// <summary>Creates the no-op entry a new leader appends on election.</summary>
    public static RaftLogEntry NoOp(long index, long term) =>
        new(index, term, RaftEntryKind.NoOp, []);

    /// <summary>Creates a configuration entry.</summary>
    public static RaftLogEntry Configuration(long index, long term, RaftConfiguration configuration) =>
        new(index, term, RaftEntryKind.Configuration, RaftConfigurationCodec.Encode(configuration));

    /// <summary>Decodes the configuration this entry carries.</summary>
    public RaftConfiguration AsConfiguration() =>
        Kind == RaftEntryKind.Configuration
            ? RaftConfigurationCodec.Decode(Command)
            : throw new InvalidOperationException($"This entry is a {Kind}, not a configuration.");
}

/// <summary>A candidate's request for a vote.</summary>
/// <param name="Term">The candidate's term.</param>
/// <param name="CandidateId">Who is asking.</param>
/// <param name="LastLogIndex">Index of the candidate's last log entry.</param>
/// <param name="LastLogTerm">Term of the candidate's last log entry.</param>
public readonly record struct RequestVoteRequest(
    long Term,
    string CandidateId,
    long LastLogIndex,
    long LastLogTerm);

/// <summary>A reply to <see cref="RequestVoteRequest"/>.</summary>
/// <param name="Term">The responder's term, so a stale candidate learns it is behind.</param>
/// <param name="VoteGranted">Whether the vote was given.</param>
/// <param name="VoterId">Who replied.</param>
public readonly record struct RequestVoteResponse(long Term, bool VoteGranted, string VoterId);

/// <summary>A leader's replication or heartbeat message.</summary>
/// <param name="Term">The leader's term.</param>
/// <param name="LeaderId">Who is leading, so followers can redirect clients.</param>
/// <param name="PreviousLogIndex">Index of the entry immediately before <paramref name="Entries"/>.</param>
/// <param name="PreviousLogTerm">Term of that entry, used for the consistency check.</param>
/// <param name="Entries">Entries to append; empty for a pure heartbeat.</param>
/// <param name="LeaderCommit">How far the leader has committed.</param>
public readonly record struct AppendEntriesRequest(
    long Term,
    string LeaderId,
    long PreviousLogIndex,
    long PreviousLogTerm,
    IReadOnlyList<RaftLogEntry> Entries,
    long LeaderCommit);

/// <summary>A reply to <see cref="AppendEntriesRequest"/>.</summary>
/// <param name="Term">The responder's term.</param>
/// <param name="Success">Whether the consistency check passed and the entries were appended.</param>
/// <param name="FollowerId">Who replied.</param>
/// <param name="MatchIndex">
/// The highest index this follower now agrees with, which lets the leader advance its view in
/// one round trip instead of probing.
/// </param>
/// <param name="ConflictTerm">
/// On rejection, the term of the follower's conflicting entry. Zero when the follower's log is
/// simply too short.
/// </param>
/// <param name="ConflictIndex">
/// On rejection, the index the leader should retry from.
/// </param>
/// <remarks>
/// The conflict fields are an optimisation over the paper's bare rejection. Decrementing
/// <c>nextIndex</c> by one per failed round trip means a follower that is a thousand entries
/// behind needs a thousand round trips to resynchronise. Reporting where the logs actually
/// diverge collapses that to roughly one round trip per differing term, which in practice is a
/// handful.
/// </remarks>
public readonly record struct AppendEntriesResponse(
    long Term,
    bool Success,
    string FollowerId,
    long MatchIndex,
    long ConflictTerm,
    long ConflictIndex);

/// <summary>
/// A leader sending a snapshot to a follower that has fallen too far behind.
/// </summary>
/// <param name="Term">The leader's term.</param>
/// <param name="LeaderId">Who is leading.</param>
/// <param name="LastIncludedIndex">The snapshot replaces every entry up to and including this index.</param>
/// <param name="LastIncludedTerm">Term of the entry at <paramref name="LastIncludedIndex"/>.</param>
/// <param name="Configuration">
/// The cluster configuration as of the snapshot, encoded. Carried because the log entries that
/// established it may have been discarded.
/// </param>
/// <param name="Offset">Byte offset of this chunk within the snapshot.</param>
/// <param name="Data">This chunk's bytes.</param>
/// <param name="Done">True on the final chunk, which is what triggers installation.</param>
/// <remarks>
/// <para>
/// This RPC exists because of a gap that opens the moment a log can be truncated. A follower
/// that is far behind — a new server, or one that was down for a while — needs entries the
/// leader has already discarded, so <c>AppendEntries</c> can never succeed for it no matter how
/// far back <c>nextIndex</c> is wound. The leader has to send the state itself rather than the
/// history that produced it.
/// </para>
/// <para>
/// Sent in chunks rather than one message, because a snapshot is the size of the entire dataset.
/// A single frame would mean buffering all of it on both sides and would make one lost packet
/// cost the whole transfer.
/// </para>
/// </remarks>
public readonly record struct InstallSnapshotRequest(
    long Term,
    string LeaderId,
    long LastIncludedIndex,
    long LastIncludedTerm,
    byte[] Configuration,
    long Offset,
    byte[] Data,
    bool Done);

/// <summary>A reply to <see cref="InstallSnapshotRequest"/>.</summary>
/// <param name="Term">The responder's term.</param>
/// <param name="FollowerId">Who replied.</param>
/// <param name="BytesReceived">
/// How much of the snapshot the follower now holds, so the leader knows where to resume.
/// </param>
/// <param name="Success">False when the request was rejected, e.g. for a stale term.</param>
public readonly record struct InstallSnapshotResponse(
    long Term,
    string FollowerId,
    long BytesReceived,
    bool Success);

/// <summary>
/// How a node reaches its peers. Abstracted so that tests can replace the network.
/// </summary>
/// <remarks>
/// This interface is the single most important thing in making the consensus module testable.
/// Raft's interesting behaviour is all in how it responds to a network that loses, delays,
/// reorders and partitions messages — and none of that is reproducible over a real socket on
/// one machine. With the transport behind an interface, a test can partition the cluster at an
/// exact moment, drop precisely the reply that would have completed an election, and assert on
/// what happens, deterministically and in milliseconds.
/// </remarks>
public interface IRaftTransport
{
    /// <summary>Asks <paramref name="peerId"/> for a vote.</summary>
    Task<RequestVoteResponse?> RequestVoteAsync(
        string peerId,
        RequestVoteRequest request,
        CancellationToken cancellationToken);

    /// <summary>Sends entries or a heartbeat to <paramref name="peerId"/>.</summary>
    Task<AppendEntriesResponse?> AppendEntriesAsync(
        string peerId,
        AppendEntriesRequest request,
        CancellationToken cancellationToken);

    /// <summary>Sends one chunk of a snapshot to <paramref name="peerId"/>.</summary>
    Task<InstallSnapshotResponse?> InstallSnapshotAsync(
        string peerId,
        InstallSnapshotRequest request,
        CancellationToken cancellationToken);
}

/// <summary>
/// What a node does with an entry once it is committed.
/// </summary>
/// <remarks>
/// Raft deliberately knows nothing about what it is replicating: its job is to agree on an
/// ordered sequence of opaque commands, and the state machine's job is to apply them. Keeping
/// that boundary clean is what makes the consensus module testable against a trivial in-memory
/// state machine, and what would let the same module replicate something other than this
/// key-value store.
/// </remarks>
public interface IRaftStateMachine
{
    /// <summary>
    /// Applies a committed command. Called in index order, exactly once per entry, and never
    /// concurrently with itself.
    /// </summary>
    ValueTask ApplyAsync(RaftLogEntry entry, CancellationToken cancellationToken);

    /// <summary>
    /// Writes the complete current state to <paramref name="destination"/>.
    /// </summary>
    /// <remarks>
    /// Called with the state machine quiesced, so the result is a consistent image of everything
    /// applied up to the snapshot's index. The format is entirely the state machine's business:
    /// Raft treats it as opaque bytes, exactly as it treats commands.
    /// </remarks>
    ValueTask CaptureSnapshotAsync(Stream destination, CancellationToken cancellationToken);

    /// <summary>
    /// Replaces the entire current state with the image in <paramref name="source"/>.
    /// </summary>
    /// <remarks>
    /// This must <i>replace</i> rather than merge. A follower receiving a snapshot may hold state
    /// from entries that were never committed and have since been overwritten, and leaving those
    /// behind would leave it permanently divergent from the leader.
    /// </remarks>
    ValueTask RestoreSnapshotAsync(Stream source, CancellationToken cancellationToken);
}
