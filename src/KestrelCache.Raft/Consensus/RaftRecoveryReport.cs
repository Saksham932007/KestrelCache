namespace KestrelCache.Raft;

/// <summary>
/// What a node found and did while recovering its state from disk.
/// </summary>
/// <remarks>
/// Returned so that tests and operators can assert on recovery behaviour rather than infer it.
/// "The cluster came back up" is not the same claim as "the snapshot was restored, 412 log
/// entries were replayed on top of it, and 83 bytes of torn tail were discarded", and only the
/// second one is checkable.
/// </remarks>
public sealed record RaftRecoveryReport
{
    /// <summary>Whether a snapshot was found and restored into the state machine.</summary>
    public bool SnapshotRestored { get; init; }

    /// <summary>Last index the restored snapshot covered.</summary>
    public long SnapshotIndex { get; init; }

    /// <summary>Term of the entry at <see cref="SnapshotIndex"/>.</summary>
    public long SnapshotTerm { get; init; }

    /// <summary>Log entries recovered after the snapshot boundary.</summary>
    public long LogEntriesRecovered { get; init; }

    /// <summary>Bytes of torn log tail discarded, the expected result of crashing mid-append.</summary>
    public long BytesDiscardedAtStartup { get; init; }

    /// <summary>
    /// The configuration the node came up with, taken from the log if it holds one, else from
    /// the snapshot, else from the bootstrap options.
    /// </summary>
    public RaftConfiguration? Configuration { get; init; }

    /// <summary>True when nothing had to be discarded.</summary>
    public bool Clean => BytesDiscardedAtStartup == 0;
}
