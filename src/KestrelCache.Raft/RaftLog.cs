using System.Buffers;
using System.Buffers.Binary;
using System.IO.Hashing;
using KestrelCache.Internal;

namespace KestrelCache.Raft;

/// <summary>
/// The replicated log, persisted to disk.
/// </summary>
/// <remarks>
/// <para>
/// This has to be durable, and it has to be truncatable. Durable because Raft's safety argument
/// assumes a node that acknowledges an entry still has it after a restart — a node that forgets
/// can vote twice in a term, or lose an entry a leader believes is replicated, and either breaks
/// the protocol. Truncatable because a follower whose log has diverged from the leader's must
/// discard the conflicting suffix, which is the one operation an append-only log is bad at.
/// </para>
/// <para>
/// So the file is append-only with explicit truncation: entries are appended with a checksum,
/// and a truncation seeks to the offset of the first discarded entry and shortens the file. An
/// in-memory index of (index → file offset) makes both cheap, and is rebuilt by replaying the
/// file at startup — the same pattern as the Bitcask engine, for the same reason.
/// </para>
/// <para><b>Record layout</b></para>
/// <code>
/// uint32 crc32        over everything that follows
/// int64  index
/// int64  term
/// uint8  flags        bit 0 = no-op entry
/// int32  commandLength
/// bytes  command
/// </code>
/// </remarks>
public sealed class RaftLog : IAsyncDisposable
{
    private const int HeaderSize = 25; // crc(4) + index(8) + term(8) + flags(1) + length(4)
    private const byte FlagNoOp = 0b0000_0001;

    private readonly FileStream _stream;
    private readonly List<long> _offsets = []; // _offsets[i] is the offset of entry i+1
    private readonly List<long> _terms = [];   // _terms[i] is the term of entry i+1
    private readonly object _gate = new();

    private long _writeOffset;

    private RaftLog(FileStream stream) => _stream = stream;

    /// <summary>Index of the last entry, or 0 when the log is empty.</summary>
    public long LastIndex
    {
        get { lock (_gate) return _offsets.Count; }
    }

    /// <summary>Term of the last entry, or 0 when the log is empty.</summary>
    public long LastTerm
    {
        get { lock (_gate) return _terms.Count > 0 ? _terms[^1] : 0; }
    }

    /// <summary>Bytes the log occupies.</summary>
    public long SizeBytes => Volatile.Read(ref _writeOffset);

    /// <summary>Entries that failed validation and were discarded at startup.</summary>
    public long EntriesDiscardedAtStartup { get; private set; }

    /// <summary>Opens or creates a log file, replaying it to rebuild the index.</summary>
    public static RaftLog Open(string path)
    {
        string? directory = Path.GetDirectoryName(Path.GetFullPath(path));
        if (!string.IsNullOrEmpty(directory)) System.IO.Directory.CreateDirectory(directory);

        // FileShare.None, deliberately. Replay truncates a torn tail, so a second opener would
        // happily shorten a log that a live node is in the middle of appending to -- destroying
        // committed entries. Two owners of one Raft log is always a mistake, and it should fail
        // at open rather than corrupt data later.
        var stream = new FileStream(
            path,
            new FileStreamOptions
            {
                Mode = FileMode.OpenOrCreate,
                Access = FileAccess.ReadWrite,
                Share = FileShare.None,
                BufferSize = 0,
            });

        var log = new RaftLog(stream);
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

    private void Replay()
    {
        var handle = _stream.SafeFileHandle;
        long length = RandomAccess.GetLength(handle);
        long offset = 0;

        var header = new byte[HeaderSize];

        while (offset < length)
        {
            if (length - offset < HeaderSize) break;

            // A looping read, not a bare RandomAccess.Read. pread is explicitly allowed to
            // return fewer bytes than asked for, and treating a short read as end-of-log would
            // silently truncate a perfectly good log -- the worst possible outcome for the one
            // file whose durability the whole protocol depends on.
            int read = PositionalIo.ReadAtMost(handle, header, offset);
            if (read < HeaderSize) break;

            uint storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(header);
            long entryIndex = BinaryPrimitives.ReadInt64LittleEndian(header.AsSpan(4));
            long entryTerm = BinaryPrimitives.ReadInt64LittleEndian(header.AsSpan(12));
            int commandLength = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(21));

            // An implausible length means a torn or corrupt record; stop rather than allocate.
            if (commandLength < 0
                || commandLength > 64 * 1024 * 1024
                || offset + HeaderSize + commandLength > length)
            {
                break;
            }

            int total = HeaderSize + commandLength;
            byte[] record = ArrayPool<byte>.Shared.Rent(total);
            try
            {
                var span = record.AsSpan(0, total);
                if (PositionalIo.ReadAtMost(handle, span, offset) < total) break;

                if (Crc32.HashToUInt32(span[4..]) != storedCrc) break;

                // Indices must be contiguous and start at 1; anything else means the file is not
                // a log this build wrote.
                if (entryIndex != _offsets.Count + 1) break;

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
            EntriesDiscardedAtStartup = 1;
            _stream.SetLength(offset);
            _stream.Flush(flushToDisk: true);
        }

        _writeOffset = offset;
    }

    /// <summary>The term of the entry at <paramref name="index"/>, or 0 if it does not exist.</summary>
    public long TermAt(long index)
    {
        lock (_gate)
        {
            if (index <= 0 || index > _terms.Count) return 0;
            return _terms[(int)(index - 1)];
        }
    }

    /// <summary>Whether the log holds an entry at <paramref name="index"/> with <paramref name="term"/>.</summary>
    /// <remarks>
    /// The explicit length check matters, and its absence was a real bug. <see cref="TermAt"/>
    /// returns 0 for an index the log does not hold, so <c>Matches(5, 0)</c> on a two-entry log
    /// used to return true — letting a follower pass the consistency check for a position it did
    /// not have, accept entries starting beyond its own end, and so open a gap in its log. Every
    /// index after the gap was then off by one while still carrying a plausible term, which is
    /// the one situation Raft's induction argument cannot recover from: two logs agreeing on
    /// (index, term) while holding different entries.
    /// </remarks>
    public bool Matches(long index, long term)
    {
        if (index == 0) return term == 0; // the empty prefix always matches
        if (index > LastIndex) return false;
        return TermAt(index) == term;
    }

    private readonly SemaphoreSlim _appendLock = new(1, 1);

    /// <summary>
    /// Appends one entry at the next free index, as a leader, and returns the entry that was
    /// written.
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
        byte[] command,
        bool isNoOp,
        CancellationToken cancellationToken = default)
    {
        await _appendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            long index;
            long offset;
            lock (_gate)
            {
                index = _offsets.Count + 1;
                offset = _writeOffset;
            }

            var entry = new RaftLogEntry(index, term, command, isNoOp);
            await WriteEntriesAsync([entry], offset, cancellationToken).ConfigureAwait(false);

            lock (_gate)
            {
                _offsets.Add(offset);
                _terms.Add(term);
                _writeOffset = offset + HeaderSize + command.Length;
            }

            return entry;
        }
        finally
        {
            _appendLock.Release();
        }
    }

    /// <summary>
    /// Appends entries received from a leader, discarding any existing suffix that conflicts
    /// with them.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The conflict rule is precise and worth stating: an existing entry is discarded only when
    /// its term differs from the incoming entry at the same index. An entry that already matches
    /// is left alone, and — importantly — entries <i>after</i> the incoming batch are not
    /// discarded when the batch is merely a repeat of what is already there.
    /// </para>
    /// <para>
    /// That last point is a real trap. A delayed or duplicated AppendEntries can arrive after
    /// the follower has already accepted later entries; truncating on every append would throw
    /// away committed data because an old message turned up late. Only an actual term mismatch
    /// justifies truncation.
    /// </para>
    /// <para>
    /// Contiguity is enforced rather than assumed. The caller is supposed to have verified the
    /// preceding entry before calling, so a batch that would leave a gap means the consistency
    /// check was skipped or wrong — and writing it anyway produces a log whose entries sit at
    /// positions that disagree with their own recorded indices, which corrupts every comparison
    /// made against it afterwards. Failing loudly here is the only safe response.
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
            long truncateFrom = -1;

            lock (_gate)
            {
                foreach (var entry in entries)
                {
                    if (entry.Index <= _terms.Count
                        && _terms[(int)(entry.Index - 1)] != entry.Term)
                    {
                        truncateFrom = entry.Index;
                        break;
                    }
                }

                if (truncateFrom > 0)
                {
                    TruncateLocked(truncateFrom);
                }
            }

            var toAppend = new List<RaftLogEntry>(entries.Count);
            long offset;

            lock (_gate)
            {
                long nextIndex = _terms.Count + 1;

                foreach (var entry in entries)
                {
                    if (entry.Index < nextIndex) continue; // already present and identical

                    if (entry.Index != nextIndex)
                    {
                        throw new InvalidOperationException(
                            $"Refusing to append entry {entry.Index} to a log of "
                                + $"{_terms.Count} entries: that would leave a gap at index "
                                + $"{nextIndex}. The caller must verify the preceding entry "
                                + "before appending.");
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
                    running += HeaderSize + entry.Command.Length;
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
            total += HeaderSize + entry.Command.Length;
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
        if (fromIndex <= 0 || fromIndex > _offsets.Count) return;

        long offset = _offsets[(int)(fromIndex - 1)];

        _offsets.RemoveRange((int)(fromIndex - 1), _offsets.Count - (int)(fromIndex - 1));
        _terms.RemoveRange((int)(fromIndex - 1), _terms.Count - (int)(fromIndex - 1));

        _stream.SetLength(offset);
        _stream.Flush(flushToDisk: true);
        _writeOffset = offset;
    }

    /// <summary>Reads up to <paramref name="maxEntries"/> entries starting at <paramref name="fromIndex"/>.</summary>
    public IReadOnlyList<RaftLogEntry> Read(long fromIndex, int maxEntries)
    {
        var results = new List<RaftLogEntry>();

        lock (_gate)
        {
            for (long index = Math.Max(1, fromIndex);
                 index <= _offsets.Count && results.Count < maxEntries;
                 index++)
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
            if (index <= 0 || index > _offsets.Count)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(index), index, $"The log holds entries 1..{_offsets.Count}.");
            }
            return ReadAtLocked(index);
        }
    }

    private RaftLogEntry ReadAtLocked(long index)
    {
        long offset = _offsets[(int)(index - 1)];
        var handle = _stream.SafeFileHandle;

        Span<byte> header = stackalloc byte[HeaderSize];
        PositionalIo.ReadExactly(handle, header, offset);

        long entryIndex = BinaryPrimitives.ReadInt64LittleEndian(header[4..]);
        long entryTerm = BinaryPrimitives.ReadInt64LittleEndian(header[12..]);
        byte flags = header[20];
        int commandLength = BinaryPrimitives.ReadInt32LittleEndian(header[21..]);

        var command = new byte[commandLength];
        if (commandLength > 0)
        {
            PositionalIo.ReadExactly(handle, command, offset + HeaderSize);
        }

        return new RaftLogEntry(entryIndex, entryTerm, command, (flags & FlagNoOp) != 0);
    }

    private static int Encode(Span<byte> destination, in RaftLogEntry entry)
    {
        int total = HeaderSize + entry.Command.Length;

        BinaryPrimitives.WriteInt64LittleEndian(destination[4..], entry.Index);
        BinaryPrimitives.WriteInt64LittleEndian(destination[12..], entry.Term);
        destination[20] = entry.IsNoOp ? FlagNoOp : (byte)0;
        BinaryPrimitives.WriteInt32LittleEndian(destination[21..], entry.Command.Length);
        entry.Command.CopyTo(destination[HeaderSize..]);

        uint crc = Crc32.HashToUInt32(destination[4..total]);
        BinaryPrimitives.WriteUInt32LittleEndian(destination, crc);

        return total;
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _stream.Dispose();
        _appendLock.Dispose();
        return ValueTask.CompletedTask;
    }
}
