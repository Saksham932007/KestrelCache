using System.Buffers;
using System.Collections.Concurrent;
using KestrelCache.Internal;

namespace KestrelCache.Bitcask;

/// <summary>
/// A Bitcask-style storage engine: one append-only log on disk, one hash index in memory.
/// </summary>
/// <remarks>
/// <para>
/// Every write is an append, so writes cost one sequential I/O regardless of key distribution.
/// Every read is a hash lookup followed by exactly one positional read, so reads cost one seek
/// — never more, which is a guarantee a B-tree cannot make. The price is paid in two places:
/// the index must fit in RAM (it holds every live key), and the keyspace is unordered, so
/// range scans are impossible. Those two costs are precisely why the LSM engine exists
/// alongside this one, and why this type implements <see cref="IStorageEngine"/> but not
/// <see cref="IScannableStorageEngine"/>.
/// </para>
/// <para>
/// Concurrency model: reads take no locks at all. Writes serialise behind a single
/// <see cref="SemaphoreSlim"/>, which is not a bottleneck because they are appends to one file
/// and would be serialised by the disk anyway.
/// </para>
/// </remarks>
public sealed class BitcaskEngine : IStorageEngine
{
    private readonly DatabaseOptions _options;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly CancellationTokenSource _shutdown = new();
    private readonly Task? _syncLoop;

    private BitcaskStore _store;
    private volatile bool _disposed;
    private int _compactionScheduled;
    private long _unsyncedWrites;

    private long _reads;
    private long _writes;
    private long _deletes;
    private long _syncs;
    private long _compactions;
    private long _compactionBytesWritten;

    private BitcaskEngine(DatabaseOptions options, BitcaskStore store, RecoveryReport recovery)
    {
        _options = options;
        _store = store;
        Recovery = recovery;

        if (options.SyncPolicy == SyncPolicy.Interval)
        {
            _syncLoop = Task.Run(() => SyncLoopAsync(_shutdown.Token));
        }
    }

    /// <inheritdoc />
    public string Name => "bitcask";

    /// <summary>What startup recovery found and did. Useful for crash-consistency assertions.</summary>
    public RecoveryReport Recovery { get; }

    /// <summary>Opens (or creates) a Bitcask database, replaying the log to rebuild the index.</summary>
    public static ValueTask<BitcaskEngine> OpenAsync(string path) =>
        OpenAsync(new DatabaseOptions { Path = path });

    /// <summary>Opens (or creates) a Bitcask database with explicit options.</summary>
    public static ValueTask<BitcaskEngine> OpenAsync(DatabaseOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        string? directory = Path.GetDirectoryName(Path.GetFullPath(options.Path));
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        var stream = OpenLog(options.Path);
        try
        {
            var (index, writeOffset, liveBytes, report) = Replay(stream, options);
            var store = BitcaskStore.Create(options.Path, stream, index, writeOffset, liveBytes);
            return ValueTask.FromResult(new BitcaskEngine(options, store, report));
        }
        catch
        {
            stream.Dispose();
            throw;
        }
    }

    private static FileStream OpenLog(string path) =>
        new(
            path,
            new FileStreamOptions
            {
                Mode = FileMode.OpenOrCreate,
                Access = FileAccess.ReadWrite,
                Share = FileShare.Read,
                // Buffering is disabled because all I/O is positional: a FileStream buffer would
                // sit between RandomAccess writes and RandomAccess reads of the same handle and
                // make them incoherent. The stream exists only to provide fsync.
                BufferSize = 0,
                Options = FileOptions.Asynchronous,
            });

    // ---------------------------------------------------------------- recovery

    private static (ConcurrentDictionary<byte[], IndexEntry> Index, long WriteOffset, long LiveBytes, RecoveryReport Report)
        Replay(FileStream stream, DatabaseOptions options)
    {
        var handle = stream.SafeFileHandle;
        string path = options.Path;
        var index = new ConcurrentDictionary<byte[], IndexEntry>(ByteKeyComparer.Instance);
        long fileLength = RandomAccess.GetLength(handle);

        if (fileLength == 0)
        {
            Span<byte> fileHeader = stackalloc byte[RecordFormat.FileHeaderSize];
            RecordFormat.WriteFileHeader(fileHeader);
            RandomAccess.Write(handle, fileHeader, 0);
            stream.Flush(flushToDisk: true);
            return (index, RecordFormat.FileHeaderSize, 0, new RecoveryReport());
        }

        if (fileLength < RecordFormat.FileHeaderSize)
        {
            throw new CorruptRecordException(
                $"File is only {fileLength} bytes, shorter than the {RecordFormat.FileHeaderSize}-byte file header.",
                0,
                path);
        }

        Span<byte> headerScratch = stackalloc byte[RecordFormat.FileHeaderSize];
        PositionalIo.ReadExactly(handle, headerScratch, 0);
        RecordFormat.ValidateFileHeader(headerScratch, path);

        long liveBytes = 0;
        long replayed = 0;
        string? truncationReason = null;
        long offset = RecordFormat.FileHeaderSize;

        // Records belonging to a batch whose terminator has not been seen yet. They are applied
        // as a unit, or discarded entirely if the log ends before the terminator arrives.
        var pending = new List<(byte[] Key, IndexEntry Entry, bool IsTombstone)>();

        var recordHeader = new byte[RecordFormat.HeaderSize];

        void Apply(byte[] key, IndexEntry entry, bool isTombstone)
        {
            if (isTombstone)
            {
                if (index.TryRemove(key, out var removed))
                {
                    liveBytes -= removed.PayloadLength;
                }
            }
            else
            {
                if (index.TryGetValue(key, out var existing))
                {
                    liveBytes -= existing.PayloadLength;
                }
                index[key] = entry;
                liveBytes += entry.PayloadLength;
            }
        }

        while (offset < fileLength)
        {
            long remaining = fileLength - offset;

            if (remaining < RecordFormat.HeaderSize)
            {
                truncationReason = $"partial record header: {remaining} byte(s) left";
                break;
            }

            PositionalIo.ReadExactly(handle, recordHeader, offset);

            if (!RecordFormat.TryParseHeader(
                    recordHeader,
                    options.MaxKeySize,
                    options.MaxValueSize,
                    out var header,
                    out string? parseError))
            {
                // A garbage header gives us no length to reason about, so we cannot prove whether
                // this is an interrupted append or deeper damage. Treating it as tail damage is
                // the recoverable reading; callers who would rather fail loudly set
                // TruncateCorruptTail = false.
                truncationReason = parseError;
                break;
            }

            int recordSize = header.RecordSize;
            if (offset + recordSize > fileLength)
            {
                truncationReason =
                    $"record declares {recordSize} bytes but only {remaining} remain";
                break;
            }

            byte[] rented = ArrayPool<byte>.Shared.Rent(recordSize);
            try
            {
                var record = rented.AsSpan(0, recordSize);
                PositionalIo.ReadExactly(handle, record, offset);

                if (!RecordFormat.VerifyChecksum(record, header))
                {
                    bool isFinalRecord = offset + recordSize == fileLength;
                    if (!isFinalRecord)
                    {
                        // An interrupted append can only ever damage the tail of the log. A bad
                        // checksum with valid records after it therefore means genuine corruption
                        // — bit rot, a bad disk, an external writer — and silently dropping the
                        // rest of the database would be far worse than refusing to open.
                        throw new CorruptRecordException(
                            "Checksum mismatch on a record that is not at the end of the log; "
                                + "this is corruption rather than an interrupted write.",
                            offset,
                            path);
                    }

                    truncationReason = "checksum mismatch on the final record";
                    break;
                }

                byte[] key = RecordFormat.KeyOf(record, header).ToArray();
                var entry = new IndexEntry(offset, recordSize);

                if (header.ContinuesBatch)
                {
                    pending.Add((key, entry, header.IsTombstone));
                }
                else
                {
                    foreach (var (pendingKey, pendingEntry, pendingTombstone) in pending)
                    {
                        Apply(pendingKey, pendingEntry, pendingTombstone);
                    }
                    pending.Clear();
                    Apply(key, entry, header.IsTombstone);
                }

                replayed++;
                offset += recordSize;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        long discarded = pending.Count;
        if (discarded > 0)
        {
            // Roll the partial batch back by rewinding to where it began.
            offset = pending[0].Entry.Offset;
            replayed -= discarded;
            truncationReason ??= "batch terminator missing";
        }

        long truncatedBytes = 0;
        if (offset < fileLength)
        {
            truncatedBytes = fileLength - offset;
            if (!options.TruncateCorruptTail)
            {
                throw new CorruptRecordException(
                    $"Log tail is damaged ({truncationReason}) and TruncateCorruptTail is disabled.",
                    offset,
                    path);
            }

            stream.SetLength(offset);
            stream.Flush(flushToDisk: true);
        }

        var finalReport = new RecoveryReport
        {
            RecordsReplayed = replayed,
            LiveKeys = index.Count,
            BytesTruncated = truncatedBytes,
            UncommittedBatchRecordsDiscarded = discarded,
            TruncationReason = truncatedBytes > 0 ? truncationReason : null,
        };

        return (index, offset, liveBytes, finalReport);
    }

    // ---------------------------------------------------------------- reads

    /// <inheritdoc />
    public async ValueTask<byte[]?> GetAsync(byte[] key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ObjectDisposedException.ThrowIf(_disposed, this);

        Interlocked.Increment(ref _reads);

        while (true)
        {
            var store = Volatile.Read(ref _store);
            if (!store.TryAddRef())
            {
                // Retired by a compaction that completed between the read and the AddRef.
                // Re-read the field and work against the new generation.
                continue;
            }

            try
            {
                if (!store.Index.TryGetValue(key, out var entry))
                {
                    return null;
                }

                byte[] rented = ArrayPool<byte>.Shared.Rent(entry.RecordLength);
                try
                {
                    var buffer = rented.AsMemory(0, entry.RecordLength);
                    await PositionalIo
                        .ReadExactlyAsync(store.Handle, buffer, entry.Offset, cancellationToken)
                        .ConfigureAwait(false);

                    return DecodeValue(buffer.Span, key, entry.Offset, store.Path);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(rented);
                }
            }
            finally
            {
                store.Release();
            }
        }
    }

    private byte[] DecodeValue(ReadOnlySpan<byte> record, byte[] expectedKey, long offset, string path)
    {
        if (!RecordFormat.TryParseHeader(
                record,
                _options.MaxKeySize,
                _options.MaxValueSize,
                out var header,
                out string? error))
        {
            throw new CorruptRecordException($"Unreadable record header: {error}", offset, path);
        }

        if (!RecordFormat.VerifyChecksum(record, header))
        {
            throw new CorruptRecordException("Checksum mismatch.", offset, path);
        }

        // The index said this offset holds this key. Verifying it catches an index that has
        // drifted out of step with the file — the exact failure the original off-by-one in
        // tombstone replay produced, where a lookup would land one byte short of its record and
        // happily return whatever it found there.
        if (!RecordFormat.KeyOf(record, header).SequenceEqual(expectedKey))
        {
            throw new CorruptRecordException(
                "Record at the indexed offset holds a different key; the index and the log disagree.",
                offset,
                path);
        }

        if (header.IsTombstone)
        {
            throw new CorruptRecordException(
                "Index points at a tombstone, which should never be reachable.",
                offset,
                path);
        }

        return RecordFormat.ValueOf(record, header).ToArray();
    }

    // ---------------------------------------------------------------- writes

    /// <inheritdoc />
    public ValueTask PutAsync(byte[] key, byte[] value, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        return AppendSingleAsync(key, value, isTombstone: false, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask DeleteAsync(byte[] key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        return AppendSingleAsync(key, [], isTombstone: true, cancellationToken);
    }

    private async ValueTask AppendSingleAsync(
        byte[] key,
        byte[] value,
        bool isTombstone,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ValidateSizes(key, value);

        int size = RecordFormat.RecordSize(key.Length, value.Length);
        byte[] rented = ArrayPool<byte>.Shared.Rent(size);

        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var store = Volatile.Read(ref _store);
            RecordFormat.Encode(rented.AsSpan(0, size), key, value, isTombstone);

            long offset = store.WriteOffset;
            await PositionalIo
                .WriteAllAsync(store.Handle, rented.AsMemory(0, size), offset, cancellationToken)
                .ConfigureAwait(false);

            // fsync before the record becomes visible, never after. A record the index already
            // points at but that is not yet durable is a record a reader can observe and a crash
            // can then erase — a lost acknowledged write.
            await SyncIfRequiredAsync().ConfigureAwait(false);

            store.WriteOffset = offset + size;
            ApplyToIndex(store, key, new IndexEntry(offset, size), isTombstone);
        }
        finally
        {
            _writeLock.Release();
            ArrayPool<byte>.Shared.Return(rented);
        }

        if (isTombstone)
        {
            Interlocked.Increment(ref _deletes);
        }
        else
        {
            Interlocked.Increment(ref _writes);
        }

        MaybeScheduleCompaction();
    }

    /// <inheritdoc />
    public async ValueTask WriteAsync(WriteBatch batch, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (batch.Count == 0) return;

        int total = 0;
        foreach (var op in batch.Ops)
        {
            ValidateSizes(op.Key, op.Value ?? []);
            total += RecordFormat.RecordSize(op.Key.Length, op.Value?.Length ?? 0);
        }

        byte[] rented = ArrayPool<byte>.Shared.Rent(total);
        var entries = new List<(byte[] Key, IndexEntry Entry, bool IsTombstone)>(batch.Count);

        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var store = Volatile.Read(ref _store);
            long baseOffset = store.WriteOffset;
            int cursor = 0;

            for (int i = 0; i < batch.Count; i++)
            {
                var op = batch.Ops[i];
                bool last = i == batch.Count - 1;
                int written = RecordFormat.Encode(
                    rented.AsSpan(cursor, total - cursor),
                    op.Key,
                    op.Value ?? [],
                    op.IsDelete,
                    continuesBatch: !last);

                entries.Add((op.Key, new IndexEntry(baseOffset + cursor, written), op.IsDelete));
                cursor += written;
            }

            // One positional write and one fsync for the whole batch. This is where batching pays
            // for itself: the fsync, not the append, is what costs milliseconds.
            await PositionalIo
                .WriteAllAsync(store.Handle, rented.AsMemory(0, cursor), baseOffset, cancellationToken)
                .ConfigureAwait(false);
            await SyncIfRequiredAsync().ConfigureAwait(false);

            store.WriteOffset = baseOffset + cursor;
            foreach (var (key, entry, isTombstone) in entries)
            {
                ApplyToIndex(store, key, entry, isTombstone);
            }
        }
        finally
        {
            _writeLock.Release();
            ArrayPool<byte>.Shared.Return(rented);
        }

        foreach (var op in batch.Ops)
        {
            if (op.IsDelete) Interlocked.Increment(ref _deletes);
            else Interlocked.Increment(ref _writes);
        }

        MaybeScheduleCompaction();
    }

    private static void ApplyToIndex(BitcaskStore store, byte[] key, IndexEntry entry, bool isTombstone)
    {
        if (isTombstone)
        {
            if (store.Index.TryRemove(key, out var removed))
            {
                store.AddLiveBytes(-removed.PayloadLength);
            }
        }
        else
        {
            if (store.Index.TryGetValue(key, out var existing))
            {
                store.AddLiveBytes(-existing.PayloadLength);
            }
            store.Index[key] = entry;
            store.AddLiveBytes(entry.PayloadLength);
        }
    }

    private void ValidateSizes(byte[] key, byte[] value)
    {
        if (key.Length == 0)
        {
            throw new ArgumentException("Key must not be empty.", nameof(key));
        }
        if (key.Length > _options.MaxKeySize)
        {
            throw new KeyOrValueTooLargeException(
                $"Key is {key.Length} bytes, above the {_options.MaxKeySize}-byte limit.");
        }
        if (value.Length > _options.MaxValueSize)
        {
            throw new KeyOrValueTooLargeException(
                $"Value is {value.Length} bytes, above the {_options.MaxValueSize}-byte limit.");
        }
    }

    // ---------------------------------------------------------------- durability

    private ValueTask SyncIfRequiredAsync()
    {
        if (_options.SyncPolicy == SyncPolicy.EveryWrite)
        {
            return ForceSyncAsync();
        }

        Interlocked.Increment(ref _unsyncedWrites);
        return ValueTask.CompletedTask;
    }

    private ValueTask ForceSyncAsync()
    {
        var store = Volatile.Read(ref _store);

        // Flush(flushToDisk: true) is the only managed API that issues a real fsync /
        // FlushFileBuffers. FlushAsync() and Flush() do not: they stop at the OS page cache,
        // which survives a process crash but not a power cut.
        store.Stream.Flush(flushToDisk: true);

        Interlocked.Increment(ref _syncs);
        Interlocked.Exchange(ref _unsyncedWrites, 0);
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await ForceSyncAsync().ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private async Task SyncLoopAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(_options.SyncInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                if (Interlocked.Read(ref _unsyncedWrites) == 0) continue;

                await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    await ForceSyncAsync().ConfigureAwait(false);
                }
                finally
                {
                    _writeLock.Release();
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Shutting down.
        }
    }

    // ---------------------------------------------------------------- compaction

    private void MaybeScheduleCompaction()
    {
        if (_options.AutoCompactStaleRatio <= 0 || _disposed) return;

        var store = Volatile.Read(ref _store);
        long diskSize = store.WriteOffset;
        if (diskSize < _options.AutoCompactMinBytes) return;

        double stale = 1.0 - ((double)store.LiveBytes / diskSize);
        if (stale < _options.AutoCompactStaleRatio) return;

        if (Interlocked.CompareExchange(ref _compactionScheduled, 1, 0) != 0) return;

        _ = Task.Run(async () =>
        {
            try
            {
                await CompactAsync(_shutdown.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Shutting down.
            }
            catch (ObjectDisposedException)
            {
                // Raced with disposal.
            }
            finally
            {
                Interlocked.Exchange(ref _compactionScheduled, 0);
            }
        });
    }

    /// <summary>
    /// Rewrites the log containing only live records, reclaiming the space held by superseded
    /// values and tombstones.
    /// </summary>
    /// <remarks>
    /// The new log is built under a temporary name and installed with a single
    /// <see cref="File.Move(string, string, bool)"/>, which is <c>rename(2)</c> underneath and
    /// therefore atomic: a crash at any point leaves either the complete old log or the complete
    /// new one, never a half-built file. Readers already working against the old generation keep
    /// their file descriptor and finish normally.
    /// </remarks>
    public async ValueTask CompactAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        BitcaskStore? previous = null;
        string? tempPath = null;

        try
        {
            var store = Volatile.Read(ref _store);
            tempPath = store.Path + ".compacting";
            File.Delete(tempPath);

            var newStream = OpenLog(tempPath);
            ConcurrentDictionary<byte[], IndexEntry> newIndex;
            long newOffset;
            long newLiveBytes = 0;
            long bytesWritten;

            try
            {
                var handle = newStream.SafeFileHandle;
                Span<byte> fileHeader = stackalloc byte[RecordFormat.FileHeaderSize];
                RecordFormat.WriteFileHeader(fileHeader);
                RandomAccess.Write(handle, fileHeader, 0);

                newIndex = new ConcurrentDictionary<byte[], IndexEntry>(ByteKeyComparer.Instance);
                newOffset = RecordFormat.FileHeaderSize;

                foreach (var (key, entry) in store.Index)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    byte[] source = ArrayPool<byte>.Shared.Rent(entry.RecordLength);
                    byte[] destination = ArrayPool<byte>.Shared.Rent(entry.RecordLength);
                    try
                    {
                        var record = source.AsMemory(0, entry.RecordLength);
                        await PositionalIo
                            .ReadExactlyAsync(store.Handle, record, entry.Offset, cancellationToken)
                            .ConfigureAwait(false);

                        if (!RecordFormat.TryParseHeader(
                                record.Span,
                                _options.MaxKeySize,
                                _options.MaxValueSize,
                                out var header,
                                out string? error))
                        {
                            throw new CorruptRecordException(
                                $"Unreadable record during compaction: {error}",
                                entry.Offset,
                                store.Path);
                        }

                        if (!RecordFormat.VerifyChecksum(record.Span, header))
                        {
                            throw new CorruptRecordException(
                                "Checksum mismatch during compaction.", entry.Offset, store.Path);
                        }

                        // Records must be re-encoded rather than copied byte for byte, because
                        // batch framing does not survive compaction. A record carrying
                        // FlagBatchContinues is only meaningful next to the terminator record
                        // that closes its batch, and compaction keeps only the live records --
                        // so the terminator is frequently dropped while a flagged record is
                        // kept. Replaying that log afterwards finds a batch that never ends and
                        // correctly discards it, silently losing live data.
                        //
                        // (An earlier version of this loop did copy verbatim, with a comment
                        // claiming re-encoding could only introduce mistakes. The model-based
                        // fuzzer in ModelBasedFuzzTests disagreed within a few hundred
                        // operations.)
                        int rewritten = RecordFormat.Encode(
                            destination.AsSpan(0, entry.RecordLength),
                            RecordFormat.KeyOf(record.Span, header),
                            RecordFormat.ValueOf(record.Span, header),
                            isTombstone: false,
                            continuesBatch: false);

                        await PositionalIo
                            .WriteAllAsync(
                                handle,
                                destination.AsMemory(0, rewritten),
                                newOffset,
                                cancellationToken)
                            .ConfigureAwait(false);

                        newIndex[key] = new IndexEntry(newOffset, rewritten);
                        newLiveBytes += entry.PayloadLength;
                        newOffset += rewritten;
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(source);
                        ArrayPool<byte>.Shared.Return(destination);
                    }
                }

                // The replacement log must be durable before it is installed, or a crash during
                // the rename could leave a named-but-empty file in place of real data.
                newStream.Flush(flushToDisk: true);
                bytesWritten = newOffset;
            }
            catch
            {
                newStream.Dispose();
                TryDelete(tempPath);
                throw;
            }

            try
            {
                File.Move(tempPath, store.Path, overwrite: true);
            }
            catch
            {
                newStream.Dispose();
                TryDelete(tempPath);
                throw;
            }

            var replacement = BitcaskStore.Create(store.Path, newStream, newIndex, newOffset, newLiveBytes);
            previous = Interlocked.Exchange(ref _store, replacement);

            Interlocked.Increment(ref _compactions);
            Interlocked.Add(ref _compactionBytesWritten, bytesWritten);
            Interlocked.Increment(ref _syncs);
        }
        finally
        {
            _writeLock.Release();

            // Dropping the engine's reference last means in-flight readers on the old generation
            // keep a valid descriptor until they finish.
            previous?.Release();
        }
    }

    private static void TryDelete(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch (IOException)
        {
            // Best effort; a stale .compacting file is harmless and will be overwritten.
        }
    }

    // ---------------------------------------------------------------- stats & teardown

    /// <inheritdoc />
    public EngineStats GetStats()
    {
        var store = Volatile.Read(ref _store);
        return new EngineStats
        {
            Engine = Name,
            KeyCount = store.Index.Count,
            DiskSizeBytes = store.WriteOffset,
            LiveDataBytes = store.LiveBytes,
            Reads = Interlocked.Read(ref _reads),
            Writes = Interlocked.Read(ref _writes),
            Deletes = Interlocked.Read(ref _deletes),
            Syncs = Interlocked.Read(ref _syncs),
            Compactions = Interlocked.Read(ref _compactions),
            CompactionBytesWritten = Interlocked.Read(ref _compactionBytesWritten),
        };
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        await _shutdown.CancelAsync().ConfigureAwait(false);

        if (_syncLoop is not null)
        {
            try
            {
                await _syncLoop.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown.
            }
        }

        var store = Volatile.Read(ref _store);
        try
        {
            store.Stream.Flush(flushToDisk: true);
            Interlocked.Increment(ref _syncs);
        }
        catch (ObjectDisposedException)
        {
            // Already closed.
        }

        store.Release();
        _shutdown.Dispose();
        _writeLock.Dispose();
    }
}
