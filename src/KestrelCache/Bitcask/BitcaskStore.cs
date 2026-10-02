using System.Collections.Concurrent;
using Microsoft.Win32.SafeHandles;

namespace KestrelCache.Bitcask;

/// <summary>Where a live record sits in the log.</summary>
/// <param name="Offset">Byte offset of the record's first header byte.</param>
/// <param name="RecordLength">Total on-disk length, so a lookup is a single sized read.</param>
internal readonly record struct IndexEntry(long Offset, int RecordLength)
{
    /// <summary>Bytes of user key+value data this record holds, excluding framing.</summary>
    internal int PayloadLength => RecordLength - RecordFormat.HeaderSize;
}

/// <summary>
/// One generation of the database: an open log file plus the in-memory index describing it.
/// </summary>
/// <remarks>
/// <para>
/// Compaction replaces the log file wholesale, which raises the question of what happens to a
/// read that is already in flight against the old file. Wrapping the handle and its index in a
/// single reference-counted object answers it: a reader atomically acquires the current
/// generation, works entirely within it, and releases it when done. Compaction publishes a new
/// generation and drops the engine's reference to the old one, whose handle closes only once
/// the last in-flight reader has finished.
/// </para>
/// <para>
/// This keeps the read path free of locks. The alternative — a reader/writer lock around the
/// handle — would reintroduce contention on exactly the path that is supposed to scale, and
/// could not be held across an <c>await</c> anyway, because <see cref="ReaderWriterLockSlim"/>
/// has thread affinity.
/// </para>
/// <para>
/// On Unix the handoff is doubly safe: compaction installs the new file with <c>rename(2)</c>,
/// which unlinks the old path but leaves the inode alive as long as a descriptor holds it open.
/// Readers on the old generation keep seeing a consistent, complete file even though it no
/// longer has a name.
/// </para>
/// </remarks>
internal sealed class BitcaskStore
{
    private int _refs = 1;
    private long _liveBytes;

    private BitcaskStore(
        string path,
        FileStream stream,
        ConcurrentDictionary<byte[], IndexEntry> index,
        long writeOffset,
        long liveBytes)
    {
        Path = path;
        Stream = stream;
        Index = index;
        WriteOffset = writeOffset;
        _liveBytes = liveBytes;
    }

    /// <summary>Path the log file currently lives at.</summary>
    internal string Path { get; }

    /// <summary>
    /// Kept only so the engine can call <see cref="FileStream.Flush(bool)"/> to issue a real
    /// fsync, which has no equivalent on <see cref="SafeFileHandle"/>. All actual I/O goes
    /// through <see cref="Handle"/> positionally; the stream's own cursor is never used.
    /// </summary>
    internal FileStream Stream { get; }

    /// <summary>The file descriptor used for every positional read and write.</summary>
    internal SafeFileHandle Handle => Stream.SafeFileHandle;

    /// <summary>Key to log location. Lock-free for readers, mutated only under the engine's write lock.</summary>
    internal ConcurrentDictionary<byte[], IndexEntry> Index { get; }

    /// <summary>Offset the next append lands at. Guarded by the engine's write lock.</summary>
    internal long WriteOffset { get; set; }

    /// <summary>Live (non-superseded, non-deleted) user bytes.</summary>
    internal long LiveBytes => Volatile.Read(ref _liveBytes);

    internal void AddLiveBytes(long delta) => Interlocked.Add(ref _liveBytes, delta);

    /// <summary>Creates a generation around an already-open, already-recovered file.</summary>
    internal static BitcaskStore Create(
        string path,
        FileStream stream,
        ConcurrentDictionary<byte[], IndexEntry> index,
        long writeOffset,
        long liveBytes) => new(path, stream, index, writeOffset, liveBytes);

    /// <summary>
    /// Takes a reference, returning false if this generation has already been retired — in which
    /// case the caller should re-read the engine's current generation and try again.
    /// </summary>
    internal bool TryAddRef()
    {
        int current = Volatile.Read(ref _refs);
        while (current > 0)
        {
            int previous = Interlocked.CompareExchange(ref _refs, current + 1, current);
            if (previous == current) return true;
            current = previous;
        }
        return false;
    }

    /// <summary>Drops a reference, closing the file once none remain.</summary>
    internal void Release()
    {
        if (Interlocked.Decrement(ref _refs) == 0)
        {
            Stream.Dispose();
        }
    }

    /// <summary>Current on-disk size of this generation.</summary>
    internal long DiskSize => Volatile.Read(ref _refs) <= 0 ? 0 : WriteOffset;
}
