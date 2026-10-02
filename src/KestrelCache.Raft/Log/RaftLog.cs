using System.Buffers;
using System.Buffers.Binary;
using System.IO.Hashing;
using KestrelCache.Internal;

namespace KestrelCache.Raft;

/// <summary>
/// The replicated log, persisted to disk, with a discardable prefix.
/// </summary>
/// <remarks>
/// <para>
/// This has to be durable, truncatable at the tail, and truncatable at the head. Durable because
/// Raft's safety argument assumes a node that acknowledges an entry still has it after a restart
/// — a node that forgets can vote twice in a term, or lose an entry a leader believes is
/// replicated. Truncatable at the tail because a follower whose log has diverged must discard the
/// conflicting suffix. Truncatable at the head because otherwise the log grows forever: a
/// cluster that has served a billion writes would replay a billion entries on every restart and
/// never reclaim the space, however small the live dataset is.
/// </para>
/// <para>
/// Head truncation is what snapshotting buys, and it changes the log's shape: entry 1 may no
/// longer exist. So the file carries a header naming the last index the discarded prefix
/// covered, which makes the file self-describing — a reader learns where the log starts rather
/// than assuming index 1.
/// </para>
/// <para><b>File layout</b></para>
/// <code>
/// header (24 bytes, rewritten whenever the prefix is discarded)
///   magic          4   "KCRL"
///   formatVersion  2   currently 1
///   reserved       2   zero
///   snapshotIndex  8   last index covered by the snapshot; 0 when nothing is discarded
///   snapshotTerm   8   term of the entry at snapshotIndex
///
/// then, for each entry:
///   crc32          4   over everything that follows in this record
///   index          8
///   term           8
///   kind           1   command | no-op | configuration
///   commandLength  4
///   command        n
/// </code>
/// </remarks>
public sealed class RaftLog : IAsyncDisposable
{
    private const int FileHeaderSize = 24;
    private const int EntryHeaderSize = 25; // crc(4) + index(8) + term(8) + kind(1) + length(4)
    private const ushort FormatVersion = 1;
    private const int MaxCommandLength = 64 * 1024 * 1024;

    private static ReadOnlySpan<byte> Magic => "KCRL"u8;

    private readonly string _path;
    private readonly SemaphoreSlim _appendLock = new(1, 1);
    private readonly object _gate = new();

    // _offsets[i] and _terms[i] describe the entry at index (_snapshotIndex + 1 + i).
    private readonly List<long> _offsets = [];
    private readonly List<long> _terms = [];

    private FileStream _stream;
    private int _disposed;
    private long _writeOffset;
    private long _snapshotIndex;
    private long _snapshotTerm;

    private RaftLog(string path, FileStream stream)
    {
        _path = path;
        _stream = stream;
    }

    /// <summary>Index of the last entry, or <see cref="SnapshotIndex"/> when the log is empty.</summary>
    public long LastIndex
    {
        get { lock (_gate) return _snapshotIndex + _offsets.Count; }
    }

    /// <summary>Term of the last entry, or <see cref="SnapshotTerm"/> when the log is empty.</summary>
    public long LastTerm
    {
        get { lock (_gate) return _terms.Count > 0 ? _terms[^1] : _snapshotTerm; }
    }

    /// <summary>
    /// Lowest index still held as an entry. Equals <see cref="SnapshotIndex"/> + 1, and may
    /// exceed <see cref="LastIndex"/> when every entry has been folded into a snapshot.
    /// </summary>
    public long FirstIndex
    {
        get { lock (_gate) return _snapshotIndex + 1; }
    }

    /// <summary>Last index covered by the discarded prefix. Zero when nothing has been discarded.</summary>
    public long SnapshotIndex
    {
        get { lock (_gate) return _snapshotIndex; }
    }

    /// <summary>Term of the entry at <see cref="SnapshotIndex"/>.</summary>
    public long SnapshotTerm
    {
        get { lock (_gate) return _snapshotTerm; }
    }

    /// <summary>Entries currently held, excluding anything folded into a snapshot.</summary>
    public int Count
    {
        get { lock (_gate) return _offsets.Count; }
    }

    /// <summary>Bytes the log file occupies.</summary>
    public long SizeBytes => Volatile.Read(ref _writeOffset);

    /// <summary>Bytes of torn tail discarded at startup.</summary>
    public long BytesDiscardedAtStartup { get; private set; }

    /// <summary>Opens or creates a log file, replaying it to rebuild the index.</summary>
    public static RaftLog Open(string path)
    {
        string? directory = Path.GetDirectoryName(Path.GetFullPath(path));
        if (!string.IsNullOrEmpty(directory)) System.IO.Directory.CreateDirectory(directory);

        var stream = OpenFile(path);
        var log = new RaftLog(path, stream);

        try
        {
            log.Replay();
            return log;
        }
        catch
        {
            stream.Dispose();
            throw;
        }
    }

    /// <remarks>
    /// FileShare.None, deliberately. Replay truncates a torn tail, so a second opener would
    /// happily shorten a log that a live node is in the middle of appending to — destroying
    /// committed entries. Two owners of one Raft log is always a mistake, and it should fail at
    /// open rather than corrupt data later.
    /// </remarks>
    private static FileStream OpenFile(string path) =>
        new(
            path,
            new FileStreamOptions
            {
                Mode = FileMode.OpenOrCreate,
                Access = FileAccess.ReadWrite,
                Share = FileShare.None,
                BufferSize = 0,
            });

    // ================================================================ recovery

    private void Replay()
    {
        var handle = _stream.SafeFileHandle;
        long length = RandomAccess.GetLength(handle);

        if (length == 0)
        {
            WriteFileHeader(0, 0);
            _writeOffset = FileHeaderSize;
            return;
        }

        if (length < FileHeaderSize)
        {
            throw new CorruptRecordException(
                $"Raft log is {length} bytes, shorter than its {FileHeaderSize}-byte header.",
                0,
                _path);
        }

        Span<byte> header = stackalloc byte[FileHeaderSize];
        PositionalIo.ReadExactly(handle, header, 0);

        if (!header[..4].SequenceEqual(Magic))
        {
            throw new CorruptRecordException("Bad magic; not a KestrelCache Raft log.", 0, _path);
        }

        ushort version = BinaryPrimitives.ReadUInt16LittleEndian(header[4..]);
        if (version != FormatVersion)
        {
            throw new CorruptRecordException(
                $"Raft log format version {version} is not supported by this build "
                    + $"(expected {FormatVersion}).",
                0,
                _path);
        }

        _snapshotIndex = BinaryPrimitives.ReadInt64LittleEndian(header[8..]);
        _snapshotTerm = BinaryPrimitives.ReadInt64LittleEndian(header[16..]);

        if (_snapshotIndex < 0 || _snapshotTerm < 0)
        {
            throw new CorruptRecordException(
                $"Raft log header holds an implausible snapshot position "
                    + $"({_snapshotIndex}@{_snapshotTerm}).",
                0,
                _path);
        }

        long offset = FileHeaderSize;
        var entryHeader = new byte[EntryHeaderSize];

        while (offset < length)
        {
            if (length - offset < EntryHeaderSize) break;

            // A looping read, not a bare RandomAccess.Read. pread is explicitly allowed to
            // return fewer bytes than asked for, and treating a short read as end-of-log would
            // silently truncate a perfectly good log -- the worst possible outcome for the one
            // file whose durability the whole protocol depends on.
            if (PositionalIo.ReadAtMost(handle, entryHeader, offset) < EntryHeaderSize) break;

            uint storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(entryHeader);
            long entryIndex = BinaryPrimitives.ReadInt64LittleEndian(entryHeader.AsSpan(4));
            long entryTerm = BinaryPrimitives.ReadInt64LittleEndian(entryHeader.AsSpan(12));
            byte kind = entryHeader[20];
            int commandLength = BinaryPrimitives.ReadInt32LittleEndian(entryHeader.AsSpan(21));

            // Implausible framing means a torn or corrupt record; stop rather than allocate.
            if (commandLength < 0
                || commandLength > MaxCommandLength
                || offset + EntryHeaderSize + commandLength > length
                || !Enum.IsDefined((RaftEntryKind)kind))
            {
                break;
            }

            int total = EntryHeaderSize + commandLength;
            byte[] record = ArrayPool<byte>.Shared.Rent(total);
            try
            {
                var span = record.AsSpan(0, total);
                if (PositionalIo.ReadAtMost(handle, span, offset) < total) break;
                if (Crc32.HashToUInt32(span[4..]) != storedCrc) break;

                // Indices must be contiguous from where the snapshot left off; anything else
                // means the file is not a log this build wrote.
                if (entryIndex != _snapshotIndex + _offsets.Count + 1) break;

                _offsets.Add(offset);
                _terms.Add(entryTerm);
                offset += total;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(record);
            }
        }

        if (offset < length)
        {
            // A torn tail is the expected result of crashing mid-append. Discarding it is safe:
            // an entry that was not fully written was never acknowledged, so no leader can
            // believe it is replicated here.
            BytesDiscardedAtStartup = length - offset;
            _stream.SetLength(offset);
            _stream.Flush(flushToDisk: true);
        }

        _writeOffset = offset;
    }

    private void WriteFileHeader(long snapshotIndex, long snapshotTerm)
    {
        Span<byte> header = stackalloc byte[FileHeaderSize];
        Magic.CopyTo(header);
        BinaryPrimitives.WriteUInt16LittleEndian(header[4..], FormatVersion);
        header[6] = 0;
        header[7] = 0;
        BinaryPrimitives.WriteInt64LittleEndian(header[8..], snapshotIndex);
        BinaryPrimitives.WriteInt64LittleEndian(header[16..], snapshotTerm);

        RandomAccess.Write(_stream.SafeFileHandle, header, 0);
        _stream.Flush(flushToDisk: true);
    }

    // ================================================================ queries

    /// <summary>
    /// The term of the entry at <paramref name="index"/>, or 0 when the log cannot answer.
    /// </summary>
    /// <remarks>
    /// Returns <see cref="SnapshotTerm"/> for exactly <see cref="SnapshotIndex"/>, because that
    /// one boundary entry's term is retained in the header specifically so the consistency check
    /// still works across it. Anything below the boundary has been discarded and cannot be
    /// answered at all.
    /// </remarks>
    public long TermAt(long index)
    {
        lock (_gate)
        {
            if (index == _snapshotIndex) return _snapshotTerm;
            if (index <= _snapshotIndex) return 0;

            long slot = index - _snapshotIndex - 1;
            return slot < _terms.Count ? _terms[(int)slot] : 0;
        }
    }

    /// <summary>Whether the log holds an entry at <paramref name="index"/> with <paramref name="term"/>.</summary>
    /// <remarks>
    /// The explicit range checks matter, and their absence was a real bug. <see cref="TermAt"/>
    /// returns 0 for an index the log does not hold, so <c>Matches(5, 0)</c> on a two-entry log
    /// used to return true — letting a follower pass the consistency check for a position it did
    /// not have, accept entries starting beyond its own end, and so open a gap in its log. Every
    /// index after the gap was then off by one while still carrying a plausible term, which is
    /// the one situation Raft's induction argument cannot recover from.
    /// </remarks>
    public bool Matches(long index, long term)
    {
        lock (_gate)
        {
            if (index == 0) return term == 0; // the empty prefix always matches
            if (index == _snapshotIndex) return term == _snapshotTerm;
            if (index < _snapshotIndex) return false; // discarded; cannot be verified
            if (index > _snapshotIndex + _offsets.Count) return false;

            return _terms[(int)(index - _snapshotIndex - 1)] == term;
        }
    }

    /// <summary>Reads up to <paramref name="maxEntries"/> entries starting at <paramref name="fromIndex"/>.</summary>
    public IReadOnlyList<RaftLogEntry> Read(long fromIndex, int maxEntries)
    {
        var results = new List<RaftLogEntry>();

        lock (_gate)
        {
            long start = Math.Max(fromIndex, _snapshotIndex + 1);
            long end = _snapshotIndex + _offsets.Count;

            for (long index = start; index <= end && results.Count < maxEntries; index++)
            {
                results.Add(ReadAtLocked(index));
            }
        }

        return results;
    }

    /// <summary>Reads the entry at <paramref name="index"/>.</summary>
    public RaftLogEntry ReadAt(long index)
    {
        lock (_gate)
        {
            if (index <= _snapshotIndex || index > _snapshotIndex + _offsets.Count)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(index),
                    index,
                    $"The log holds entries {_snapshotIndex + 1}..{_snapshotIndex + _offsets.Count}.");
            }

            return ReadAtLocked(index);
        }
    }

    private RaftLogEntry ReadAtLocked(long index)
    {
        long offset = _offsets[(int)(index - _snapshotIndex - 1)];
        var handle = _stream.SafeFileHandle;

        Span<byte> header = stackalloc byte[EntryHeaderSize];
        PositionalIo.ReadExactly(handle, header, offset);

        long entryIndex = BinaryPrimitives.ReadInt64LittleEndian(header[4..]);
        long entryTerm = BinaryPrimitives.ReadInt64LittleEndian(header[12..]);
        var kind = (RaftEntryKind)header[20];
        int commandLength = BinaryPrimitives.ReadInt32LittleEndian(header[21..]);

        var command = new byte[commandLength];
        if (commandLength > 0)
        {
            PositionalIo.ReadExactly(handle, command, offset + EntryHeaderSize);
        }

        return new RaftLogEntry(entryIndex, entryTerm, kind, command);
    }

    /// <summary>
    /// The newest configuration entry at or below <paramref name="upToIndex"/>, if the log still
    /// holds one.
    /// </summary>
    /// <remarks>
    /// Used to recover the active configuration after a tail truncation discarded the entry that
    /// established it. Returns null when no configuration entry survives, in which case the
    /// caller falls back to the snapshot's configuration.
    /// </remarks>
    public (RaftConfiguration Configuration, long Index)? FindLatestConfiguration(long upToIndex)
    {
        lock (_gate)
        {
            long end = Math.Min(upToIndex, _snapshotIndex + _offsets.Count);

            for (long index = end; index > _snapshotIndex; index--)
            {
                var entry = ReadAtLocked(index);
                if (entry.Kind == RaftEntryKind.Configuration)
                {
                    return (entry.AsConfiguration(), index);
                }
            }

            return null;
        }
    }

    // ================================================================ appends

    /// <summary>
    /// Appends one entry at the next free index, as a leader, and returns the entry written.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The index is chosen and the entry is written while holding the same lock, and that
    /// atomicity is the whole point. An earlier version computed <c>LastIndex + 1</c> under the
    /// node's state lock, released it, and only then appended — so two concurrent proposals both
    /// read the same <c>LastIndex</c> and both built an entry for the same index. The first was
    /// written; the second matched an existing index with an identical term, was taken for a
    /// duplicate, and was silently discarded. The client was then told its write had committed,
    /// because the waiter was keyed on the index and the <i>other</i> entry committed there.
    /// </para>
    /// <para>
    /// A lost write reported as successful is the worst failure a database can have, and it was
    /// invisible until a test compared every node's log entry by entry.
    /// </para>
    /// </remarks>
    public async ValueTask<RaftLogEntry> AppendAsLeaderAsync(
        long term,
        RaftEntryKind kind,
        byte[] command,
        CancellationToken cancellationToken = default)
    {
        await _appendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            long index;
            long offset;
            lock (_gate)
            {
                index = _snapshotIndex + _offsets.Count + 1;
                offset = _writeOffset;
            }

            var entry = new RaftLogEntry(index, term, kind, command);
            await WriteEntriesAsync([entry], offset, cancellationToken).ConfigureAwait(false);

            lock (_gate)
            {
                _offsets.Add(offset);
                _terms.Add(term);
                _writeOffset = offset + EntryHeaderSize + command.Length;
            }

            return entry;
        }
        finally
        {
            _appendLock.Release();
        }
    }

    /// <summary>
    /// Appends entries received from a leader, discarding any existing suffix that conflicts.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The conflict rule is precise: an existing entry is discarded only when its term differs
    /// from the incoming entry at the same index. An entry that already matches is left alone,
    /// and — importantly — entries <i>after</i> the incoming batch are not discarded when the
    /// batch is merely a repeat of what is already there.
    /// </para>
    /// <para>
    /// That last point is a real trap. A delayed or duplicated AppendEntries can arrive after the
    /// follower has already accepted later entries; truncating on every append would throw away
    /// committed data because an old message turned up late. Only an actual term mismatch
    /// justifies truncation.
    /// </para>
    /// <para>
    /// Contiguity is enforced rather than assumed. The caller is supposed to have verified the
    /// preceding entry, so a batch that would leave a gap means the consistency check was skipped
    /// or wrong — and writing it anyway produces a log whose entries sit at positions that
    /// disagree with their own recorded indices, corrupting every comparison made against it
    /// afterwards.
    /// </para>
    /// </remarks>
    public async ValueTask AppendAsync(
        IReadOnlyList<RaftLogEntry> entries,
        CancellationToken cancellationToken = default)
    {
        if (entries.Count == 0) return;

        await _appendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var toAppend = new List<RaftLogEntry>(entries.Count);
            long offset;

            lock (_gate)
            {
                long truncateFrom = -1;

                foreach (var entry in entries)
                {
                    // Entries at or below the snapshot boundary are already covered by committed
                    // state and are simply redundant.
                    if (entry.Index <= _snapshotIndex) continue;

                    long slot = entry.Index - _snapshotIndex - 1;
                    if (slot < _terms.Count && _terms[(int)slot] != entry.Term)
                    {
                        truncateFrom = entry.Index;
                        break;
                    }
                }

                if (truncateFrom > 0)
                {
                    TruncateLocked(truncateFrom);
                }

                long nextIndex = _snapshotIndex + _offsets.Count + 1;

                foreach (var entry in entries)
                {
                    if (entry.Index < nextIndex) continue; // already present and identical

                    if (entry.Index != nextIndex)
                    {
                        throw new InvalidOperationException(
                            $"Refusing to append entry {entry.Index} when the next index is "
                                + $"{nextIndex}: that would leave a gap. The caller must verify "
                                + "the preceding entry before appending.");
                    }

                    toAppend.Add(entry);
                    nextIndex++;
                }

                offset = _writeOffset;
            }

            if (toAppend.Count == 0) return;

            await WriteEntriesAsync(toAppend, offset, cancellationToken).ConfigureAwait(false);

            lock (_gate)
            {
                long running = offset;
                foreach (var entry in toAppend)
                {
                    _offsets.Add(running);
                    _terms.Add(entry.Term);
                    running += EntryHeaderSize + entry.Command.Length;
                }
                _writeOffset = running;
            }
        }
        finally
        {
            _appendLock.Release();
        }
    }

    private async ValueTask WriteEntriesAsync(
        IReadOnlyList<RaftLogEntry> entries,
        long offset,
        CancellationToken cancellationToken)
    {
        int total = 0;
        foreach (var entry in entries)
        {
            total += EntryHeaderSize + entry.Command.Length;
        }

        byte[] buffer = ArrayPool<byte>.Shared.Rent(total);
        try
        {
            int cursor = 0;
            foreach (var entry in entries)
            {
                cursor += Encode(buffer.AsSpan(cursor), entry);
            }

            await RandomAccess
                .WriteAsync(_stream.SafeFileHandle, buffer.AsMemory(0, cursor), offset, cancellationToken)
                .ConfigureAwait(false);

            // Durable before acknowledged, without exception. Raft's correctness depends on a
            // node never forgetting something it has acknowledged, so this is the one fsync in
            // the system that is not negotiable.
            _stream.Flush(flushToDisk: true);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static int Encode(Span<byte> destination, in RaftLogEntry entry)
    {
        int total = EntryHeaderSize + entry.Command.Length;

        BinaryPrimitives.WriteInt64LittleEndian(destination[4..], entry.Index);
        BinaryPrimitives.WriteInt64LittleEndian(destination[12..], entry.Term);
        destination[20] = (byte)entry.Kind;
        BinaryPrimitives.WriteInt32LittleEndian(destination[21..], entry.Command.Length);
        entry.Command.CopyTo(destination[EntryHeaderSize..]);

        uint crc = Crc32.HashToUInt32(destination[4..total]);
        BinaryPrimitives.WriteUInt32LittleEndian(destination, crc);

        return total;
    }

    // ================================================================ truncation

    /// <summary>Discards every entry from <paramref name="fromIndex"/> onwards.</summary>
    public void Truncate(long fromIndex)
    {
        lock (_gate)
        {
            TruncateLocked(fromIndex);
        }
    }

    private void TruncateLocked(long fromIndex)
    {
        if (fromIndex <= _snapshotIndex)
        {
            // The snapshot covers committed entries, which by Raft's guarantees cannot conflict
            // with a leader's log. Being asked to discard them means something upstream is badly
            // wrong, and quietly complying would destroy committed state.
            throw new InvalidOperationException(
                $"Refusing to truncate from index {fromIndex}, which is at or below the snapshot "
                    + $"boundary {_snapshotIndex}. Committed entries cannot conflict.");
        }

        long slot = fromIndex - _snapshotIndex - 1;
        if (slot >= _offsets.Count) return;

        long offset = _offsets[(int)slot];

        _offsets.RemoveRange((int)slot, _offsets.Count - (int)slot);
        _terms.RemoveRange((int)slot, _terms.Count - (int)slot);

        _stream.SetLength(offset);
        _stream.Flush(flushToDisk: true);
        _writeOffset = offset;
    }

    /// <summary>
    /// Discards every entry at or below <paramref name="upToIndex"/>, which a snapshot now
    /// covers.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Rewritten to a temporary file and renamed rather than compacted in place. Shifting the
    /// surviving entries down within the same file would be simpler and is not crash-safe: a
    /// crash partway through leaves a file that is neither the old log nor the new one, and
    /// replay would find garbage where committed entries used to be. With write-temp, fsync,
    /// rename, every crash point leaves either the complete old log or the complete new one.
    /// </para>
    /// <para>
    /// The caller must have installed the snapshot first. The ordering matters for the same
    /// reason it does in the LSM engine's manifest: discard the history before the state that
    /// replaces it is durable, and a crash in between loses both.
    /// </para>
    /// </remarks>
    public async ValueTask DiscardPrefixAsync(
        long upToIndex,
        CancellationToken cancellationToken = default)
    {
        await _appendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            List<RaftLogEntry> survivors;
            long newSnapshotIndex;
            long newSnapshotTerm;

            lock (_gate)
            {
                if (upToIndex <= _snapshotIndex) return; // already discarded
                if (upToIndex > _snapshotIndex + _offsets.Count)
                {
                    throw new ArgumentOutOfRangeException(
                        nameof(upToIndex),
                        upToIndex,
                        $"Cannot discard past the end of the log ({_snapshotIndex + _offsets.Count}).");
                }

                newSnapshotIndex = upToIndex;
                newSnapshotTerm = _terms[(int)(upToIndex - _snapshotIndex - 1)];

                survivors = [];
                for (long index = upToIndex + 1; index <= _snapshotIndex + _offsets.Count; index++)
                {
                    survivors.Add(ReadAtLocked(index));
                }
            }

            string temporary = _path + ".compacting";
            File.Delete(temporary);

            long writtenOffset;

            using (var rebuilt = new FileStream(
                temporary,
                new FileStreamOptions
                {
                    Mode = FileMode.Create,
                    Access = FileAccess.ReadWrite,
                    Share = FileShare.None,
                    BufferSize = 0,
                }))
            {
                Span<byte> header = stackalloc byte[FileHeaderSize];
                Magic.CopyTo(header);
                BinaryPrimitives.WriteUInt16LittleEndian(header[4..], FormatVersion);
                header[6] = 0;
                header[7] = 0;
                BinaryPrimitives.WriteInt64LittleEndian(header[8..], newSnapshotIndex);
                BinaryPrimitives.WriteInt64LittleEndian(header[16..], newSnapshotTerm);
                RandomAccess.Write(rebuilt.SafeFileHandle, header, 0);

                writtenOffset = FileHeaderSize;

                foreach (var entry in survivors)
                {
                    int size = EntryHeaderSize + entry.Command.Length;
                    byte[] buffer = ArrayPool<byte>.Shared.Rent(size);
                    try
                    {
                        int written = Encode(buffer.AsSpan(0, size), entry);
                        await RandomAccess
                            .WriteAsync(
                                rebuilt.SafeFileHandle,
                                buffer.AsMemory(0, written),
                                writtenOffset,
                                cancellationToken)
                            .ConfigureAwait(false);
                        writtenOffset += written;
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(buffer);
                    }
                }

                rebuilt.Flush(flushToDisk: true);
            }

            // The swap and the bookkeeping that follows it happen under the same lock readers
            // take, because for an instant in the middle of this there is no usable handle at
            // all. Doing it outside the lock -- which an earlier version did -- let a concurrent
            // read observe a disposed SafeFileHandle, which surfaced as an intermittent
            // ObjectDisposedException in roughly one full test run in three.
            //
            // Everything in here is synchronous, so holding the lock across it is safe: the
            // rename is a single syscall and the offsets are recomputed from entries already in
            // memory rather than by re-reading the file.
            lock (_gate)
            {
                _stream.Dispose();

                try
                {
                    File.Move(temporary, _path, overwrite: true);
                }
                finally
                {
                    _stream = OpenFile(_path);
                }

                _offsets.Clear();
                _terms.Clear();

                long running = FileHeaderSize;
                foreach (var entry in survivors)
                {
                    _offsets.Add(running);
                    _terms.Add(entry.Term);
                    running += EntryHeaderSize + entry.Command.Length;
                }

                _snapshotIndex = newSnapshotIndex;
                _snapshotTerm = newSnapshotTerm;
                _writeOffset = writtenOffset;
            }
        }
        finally
        {
            _appendLock.Release();
        }
    }

    /// <summary>
    /// Discards the entire log and resets it to begin after an installed snapshot.
    /// </summary>
    /// <remarks>
    /// Used when a follower installs a snapshot from the leader. Everything is discarded, not
    /// merely the prefix, because a follower far enough behind to need a snapshot may hold
    /// entries that were never committed and have since been overwritten — and keeping them
    /// would leave it permanently divergent.
    /// </remarks>
    public async ValueTask ResetToSnapshotAsync(
        long lastIncludedIndex,
        long lastIncludedTerm,
        CancellationToken cancellationToken = default)
    {
        await _appendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Under the readers' lock, for the same reason as DiscardPrefixAsync: truncating and
            // rewriting the header mutates the file a concurrent read may be part-way through.
            lock (_gate)
            {
                _offsets.Clear();
                _terms.Clear();
                _snapshotIndex = lastIncludedIndex;
                _snapshotTerm = lastIncludedTerm;
                _writeOffset = FileHeaderSize;

                _stream.SetLength(FileHeaderSize);
                WriteFileHeader(lastIncludedIndex, lastIncludedTerm);
            }
        }
        finally
        {
            _appendLock.Release();
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// Takes both locks before closing the handle, and is idempotent. Disposing the stream while
    /// an append was still in flight — or while a reader was mid-<c>pread</c> — surfaced as an
    /// <see cref="ObjectDisposedException"/> on the <c>SafeFileHandle</c>, which is an unhelpful
    /// way to learn that teardown raced an operation. Draining first means it cannot.
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        await _appendLock.WaitAsync().ConfigureAwait(false);
        try
        {
            lock (_gate)
            {
                _stream.Dispose();
            }
        }
        finally
        {
            _appendLock.Release();
            _appendLock.Dispose();
        }
    }
}
