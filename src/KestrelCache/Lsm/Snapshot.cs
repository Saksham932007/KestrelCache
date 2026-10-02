namespace KestrelCache.Lsm;

/// <summary>
/// A consistent, point-in-time view of the database that stays readable while writes continue.
/// </summary>
/// <remarks>
/// <para>
/// A snapshot is nothing but a sequence number. Reads taken against it ignore every entry
/// written later, and because an LSM tree never overwrites anything, the versions the snapshot
/// needs are still sitting in the tree where they always were. So a snapshot costs no copying,
/// no locking and no extra I/O — it is the single cheapest thing this design makes possible, and
/// it falls directly out of the decision to put sequence numbers in the key.
/// </para>
/// <para>
/// The one real cost is that it holds compaction back. An open snapshot pins every version it
/// could still need, so compaction may not drop superseded values or tombstones at or above that
/// sequence. A snapshot left open indefinitely therefore stops space from being reclaimed, which
/// is why this type is <see cref="IDisposable"/> and why disposing it matters.
/// </para>
/// </remarks>
public sealed class Snapshot : IDisposable
{
    private readonly Action<Snapshot> _onDispose;
    private int _disposed;

    internal Snapshot(ulong sequence, Action<Snapshot> onDispose)
    {
        Sequence = sequence;
        _onDispose = onDispose;
    }

    /// <summary>The sequence number this snapshot reads at. Nothing newer is visible.</summary>
    public ulong Sequence { get; }

    /// <summary>Releases the snapshot, allowing compaction to reclaim what it was pinning.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _onDispose(this);
    }
}
