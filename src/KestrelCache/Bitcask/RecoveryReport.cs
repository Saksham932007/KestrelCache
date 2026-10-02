namespace KestrelCache.Bitcask;

/// <summary>
/// What happened while replaying the log at startup. Returned so that tests (and operators) can
/// assert on recovery behaviour rather than infer it.
/// </summary>
public sealed record RecoveryReport
{
    /// <summary>Records successfully validated and applied.</summary>
    public long RecordsReplayed { get; init; }

    /// <summary>Live keys in the index once replay finished.</summary>
    public long LiveKeys { get; init; }

    /// <summary>Bytes of torn tail discarded, if any.</summary>
    public long BytesTruncated { get; init; }

    /// <summary>
    /// Records that belonged to a batch whose terminating record never arrived, and were
    /// therefore rolled back.
    /// </summary>
    public long UncommittedBatchRecordsDiscarded { get; init; }

    /// <summary>Why the tail was truncated, when it was.</summary>
    public string? TruncationReason { get; init; }

    /// <summary>True when the log was found fully intact.</summary>
    public bool Clean => BytesTruncated == 0 && UncommittedBatchRecordsDiscarded == 0;
}
