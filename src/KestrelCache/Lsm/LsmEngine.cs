using System.Runtime.CompilerServices;

namespace KestrelCache.Lsm;

/// <summary>
/// A log-structured merge-tree storage engine: write-ahead log, in-memory memtable, and levelled
/// immutable SSTables with Bloom filters and a block cache.
/// </summary>
/// <remarks>
/// <para>
/// Everything this engine does follows from one decision: never modify data in place. A write is
/// appended to a log and buffered in memory; when the buffer fills it is written out as a new
/// sorted file; when files accumulate they are merged into new files and the old ones deleted.
/// No byte on disk is ever rewritten where it sits.
/// </para>
/// <para>
/// What that buys is a write path made entirely of sequential I/O, which is the one thing both
/// spinning disks and SSDs are dramatically better at than random I/O. What it costs is that a
/// single key may now exist in several places at once, so a read has to look in each of them
/// until it finds the newest version — and old versions pile up until compaction removes them.
/// The rest of the engine is machinery for containing those two costs: sequence numbers so the
/// newest version is always found first, Bloom filters so absent keys cost no I/O, a block cache
/// so hot blocks cost no I/O either, and levelled compaction so the number of places to look
/// stays logarithmic in the data size.
/// </para>
/// <para><b>Compared with the Bitcask engine in this repository</b></para>
/// <list type="bullet">
/// <item>
/// Bitcask keeps every key in a RAM index, so its memory use is proportional to the key count
/// and its startup time to the record count. This engine keeps only per-table metadata in
/// memory, so it scales past RAM and starts in constant time.
/// </item>
/// <item>
/// Bitcask reads cost exactly one seek. This engine's cost is one seek per level consulted,
/// which Bloom filters usually reduce to one in total — slightly worse in the worst case, and
/// the price of everything above.
/// </item>
/// <item>
/// Bitcask cannot range scan at all, because its index is a hash table. This engine keeps
/// everything sorted at every level, so ordered iteration is a merge of a handful of cursors.
/// </item>
/// </list>
/// </remarks>
public sealed class LsmEngine : IScannableStorageEngine
{
    private readonly DatabaseOptions _options;
    private readonly string _directory;
    private readonly BlockCache _blockCache;
    private readonly TableCache _tableCache;

    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly SemaphoreSlim _maintenanceLock = new(1, 1);
    private readonly SemaphoreSlim _maintenanceSignal = new(0);
    private readonly CancellationTokenSource _shutdown = new();
    private readonly Task? _maintenanceLoop;

    private readonly System.Collections.Concurrent.ConcurrentQueue<PendingWrite> _writeQueue = new();
    private readonly object _snapshotGate = new();
    private readonly SortedSet<ulong> _snapshotSequences = [];
    private readonly List<LsmVersion> _liveVersions = [];

    private MemState _mem;
    private WriteAheadLog _wal;
    private LsmVersion _version;

    private ulong _nextFileNumber;
    private ulong _lastSequence;
    private ulong _publishedSequence;
    private long _unsyncedWrites;
    private volatile bool _disposed;
    private volatile Exception? _backgroundFailure;

    private long _reads;
    private long _writes;
    private long _deletes;
    private long _syncs;
    private long _compactions;
    private long _compactionBytesWritten;
    private long _flushes;
    private long _bloomNegatives;
    private long _bloomFalsePositives;

    /// <summary>
    /// The set of memtables a read must consult, swapped as one unit so a reader can never
    /// observe a memtable that has been frozen but whose replacement is not yet in place.
    /// </summary>
    private sealed record MemState(Memtable Active, IReadOnlyList<Memtable> Immutable);

    /// <summary>One caller's batch, waiting to be committed by whichever writer holds the lock.</summary>
    private sealed class PendingWrite(WriteBatch batch)
    {
        internal WriteBatch Batch { get; } = batch;

        internal TaskCompletionSource Completion { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    /// <summary>Most batches one group commit will absorb, so a burst cannot build an unbounded buffer.</summary>
    private const int MaxGroupBatches = 1024;

    /// <summary>Most individual mutations one group commit will absorb.</summary>
    private const int MaxGroupOperations = 16 * 1024;

    private LsmEngine(
        DatabaseOptions options,
        string directory,
        MemState mem,
        WriteAheadLog wal,
        LsmVersion version,
        ulong nextFileNumber,
        ulong lastSequence,
        BlockCache blockCache,
        TableCache tableCache,
        LsmRecoveryReport recovery)
    {
        _options = options;
        _directory = directory;
        _mem = mem;
        _wal = wal;
        _version = version;
        _nextFileNumber = nextFileNumber;
        _lastSequence = lastSequence;
        _publishedSequence = lastSequence;
        _blockCache = blockCache;
        _tableCache = tableCache;
        Recovery = recovery;

        _liveVersions.Add(version);

        if (options.EnableBackgroundCompaction)
        {
            _maintenanceLoop = Task.Run(() => MaintenanceLoopAsync(_shutdown.Token));
        }
    }

    /// <inheritdoc />
    public string Name => "lsm";

    /// <summary>What startup recovery found and did.</summary>
    public LsmRecoveryReport Recovery { get; }

    /// <summary>The directory holding the database's files.</summary>
    public string Directory => _directory;

    // ================================================================ open

    /// <summary>Opens (or creates) an LSM database in the given directory.</summary>
    public static ValueTask<LsmEngine> OpenAsync(string path) =>
        OpenAsync(new DatabaseOptions { Path = path });

    /// <summary>Opens (or creates) an LSM database with explicit options.</summary>
    public static async ValueTask<LsmEngine> OpenAsync(DatabaseOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        string directory = Path.GetFullPath(options.Path);
        System.IO.Directory.CreateDirectory(directory);

        var manifest = Manifest.Load(directory);
        var version = LsmVersion.FromManifest(manifest);

        ulong nextFileNumber = Math.Max(1, manifest.NextFileNumber);
        ulong lastSequence = manifest.LastSequence;

        var blockCache = new BlockCache(options.BlockCacheBytes);
        var tableCache = new TableCache(directory, blockCache);

        try
        {
            // Any .sst on disk that the manifest does not name is an orphan from a crash between
            // writing a table and installing the manifest that referenced it. It holds no
            // reachable data, so it is garbage.
            var referenced = version.FileNumbers.ToHashSet();
            long orphansRemoved = RemoveOrphanTables(directory, referenced, out long orphanBytes);

            // Replay every log the manifest still considers live, in order.
            var recovered = new Memtable(logNumber: 0);
            var walNumbers = FindLogNumbers(directory)
                .Where(number => number >= manifest.LogNumber)
                .OrderBy(number => number)
                .ToList();

            long replayedRecords = 0;
            long replayedEntries = 0;
            long truncatedBytes = 0;
            string? truncationReason = null;
            ulong sequence = lastSequence;

            foreach (ulong number in walNumbers)
            {
                string walPath = Path.Combine(directory, WriteAheadLog.FileNameFor(number));

                var result = WriteAheadLog.Replay(
                    walPath,
                    entry =>
                    {
                        recovered.Add(entry.Sequence, entry.Kind, entry.Key, entry.Value);
                        sequence = Math.Max(sequence, entry.Sequence);
                    },
                    options);

                replayedRecords += result.Records;
                replayedEntries += result.Entries;
                truncatedBytes += result.BytesTruncated;
                truncationReason ??= result.StopReason;
                nextFileNumber = Math.Max(nextFileNumber, number + 1);
            }

            lastSequence = sequence;

            // A fresh log for the new active memtable. Taking a new number rather than appending
            // to the last one keeps the invariant that a log maps to exactly one memtable.
            ulong newLogNumber = nextFileNumber++;
            var wal = WriteAheadLog.Create(directory, newLogNumber);

            long flushedEntries = 0;

            // Anything recovered from the logs is written out as a level-0 table immediately, so
            // that the logs it came from can be retired and recovery is not repeated on the next
            // restart.
            if (recovered.Count > 0)
            {
                ulong tableNumber = nextFileNumber++;
                var meta = await WriteTableFromIteratorAsync(
                        directory,
                        options,
                        tableNumber,
                        recovered.CreateIterator(),
                        CancellationToken.None)
                    .ConfigureAwait(false);

                if (meta is not null)
                {
                    version = version.With(level: 0, added: [meta], removed: []);
                    flushedEntries = meta.EntryCount;
                }
            }

            Manifest.Save(
                directory,
                Manifest.From(version, nextFileNumber, lastSequence, newLogNumber));

            // Only now that the manifest is durable may the old logs go.
            foreach (ulong number in walNumbers)
            {
                TryDelete(Path.Combine(directory, WriteAheadLog.FileNameFor(number)));
            }

            var recovery = new LsmRecoveryReport
            {
                LogsReplayed = walNumbers.Count,
                RecordsReplayed = replayedRecords,
                EntriesReplayed = replayedEntries,
                EntriesFlushedToTable = flushedEntries,
                BytesTruncated = truncatedBytes,
                TruncationReason = truncatedBytes > 0 ? truncationReason : null,
                OrphanTablesRemoved = orphansRemoved,
                OrphanBytesReclaimed = orphanBytes,
                TablesOpened = version.FileNumbers.Count(),
                LastSequence = lastSequence,
            };

            var mem = new MemState(new Memtable(newLogNumber), []);

            return new LsmEngine(
                options,
                directory,
                mem,
                wal,
                version,
                nextFileNumber,
                lastSequence,
                blockCache,
                tableCache,
                recovery);
        }
        catch
        {
            tableCache.Dispose();
            throw;
        }
    }

    private static IEnumerable<ulong> FindLogNumbers(string directory)
    {
        foreach (string path in System.IO.Directory.EnumerateFiles(directory, "*.wal"))
        {
            if (ulong.TryParse(Path.GetFileNameWithoutExtension(path), out ulong number))
            {
                yield return number;
            }
        }
    }

    private static long RemoveOrphanTables(
        string directory,
        HashSet<ulong> referenced,
        out long bytesReclaimed)
    {
        long removed = 0;
        bytesReclaimed = 0;

        foreach (string path in System.IO.Directory.EnumerateFiles(directory, "*.sst"))
        {
            if (!ulong.TryParse(Path.GetFileNameWithoutExtension(path), out ulong number)) continue;
            if (referenced.Contains(number)) continue;

            try
            {
                bytesReclaimed += new FileInfo(path).Length;
                File.Delete(path);
                removed++;
            }
            catch (IOException)
            {
                // Leaving an orphan behind wastes space but is not a correctness problem.
            }
        }

        return removed;
    }

    // ================================================================ reads

    /// <inheritdoc />
    public ValueTask<byte[]?> GetAsync(byte[] key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ObjectDisposedException.ThrowIf(_disposed, this);
        ThrowIfBackgroundFailed();

        // Reading the published sequence, rather than the in-progress one, is what makes a batch
        // atomic to readers: a batch's sequences are all above the published value until the
        // whole batch has been inserted, at which point the new value is published in one store.
        ulong snapshot = Volatile.Read(ref _publishedSequence);
        return GetAtAsync(key, snapshot, cancellationToken);
    }

    /// <summary>Reads a key as of <paramref name="snapshot"/>.</summary>
    public ValueTask<byte[]?> GetAsync(
        byte[] key,
        Snapshot snapshot,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(snapshot);
        ObjectDisposedException.ThrowIf(_disposed, this);
        return GetAtAsync(key, snapshot.Sequence, cancellationToken);
    }

    private ValueTask<byte[]?> GetAtAsync(
        byte[] key,
        ulong snapshot,
        CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _reads);

        var mem = Volatile.Read(ref _mem);

        // Newest data first. The active memtable holds the most recent writes, then each frozen
        // memtable awaiting flush, then level 0, then deeper levels.
        var result = mem.Active.TryGet(key, snapshot, out byte[]? value);
        if (result != LookupResult.NotFound)
        {
            return ValueTask.FromResult(result == LookupResult.Found ? value : null);
        }

        foreach (var frozen in mem.Immutable)
        {
            result = frozen.TryGet(key, snapshot, out value);
            if (result != LookupResult.NotFound)
            {
                return ValueTask.FromResult(result == LookupResult.Found ? value : null);
            }
        }

        return ValueTask.FromResult(SearchTables(key, snapshot, cancellationToken));
    }

    private byte[]? SearchTables(byte[] key, ulong snapshot, CancellationToken cancellationToken)
    {
        var version = AcquireVersion();
        try
        {
            foreach (var table in version.TablesFor(key))
            {
                cancellationToken.ThrowIfCancellationRequested();

                var reader = _tableCache.Get(table.FileNumber);

                if (!reader.MayContain(key))
                {
                    // Ruled out in memory. This is the Bloom filter paying for itself.
                    Interlocked.Increment(ref _bloomNegatives);
                    continue;
                }

                var result = reader.TryGet(key, snapshot, out byte[]? value);

                switch (result)
                {
                    case LookupResult.Found:
                        return value;

                    case LookupResult.Deleted:
                        // A tombstone shadows everything older, so the search stops here.
                        return null;

                    default:
                        // The filter said "maybe" and the table did not have it: a false
                        // positive, and one disk read wasted.
                        Interlocked.Increment(ref _bloomFalsePositives);
                        break;
                }
            }

            return null;
        }
        finally
        {
            ReleaseVersion(version);
        }
    }

    // ================================================================ writes

    /// <inheritdoc />
    public ValueTask PutAsync(byte[] key, byte[] value, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        return WriteAsync(new WriteBatch().Put(key, value), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask DeleteAsync(byte[] key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        return WriteAsync(new WriteBatch().Delete(key), cancellationToken);
    }

    /// <summary>
    /// Applies a batch atomically, joining a group commit with any other writes in flight.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Every write still has to be serialised — the log is one file and the memtable takes one
    /// writer — but serialising them one at a time means each pays for its own lock acquisition,
    /// its own <c>pwrite</c> and, under a strict durability policy, its own <c>fsync</c>. Under
    /// concurrent load that made the lock the queue: measured throughput was flat at roughly
    /// 49,000 writes/sec whether fsync was enabled or disabled, and client-side pipelining
    /// lifted it by only 20% while lifting reads by 4.7x.
    /// </para>
    /// <para>
    /// Group commit fixes the shape rather than the constant. A caller enqueues its batch and
    /// then tries for the lock; whoever gets it becomes the leader and commits <i>everything</i>
    /// queued — one lock acquisition, one write, one fsync for the whole group — then wakes the
    /// others. The busier the server, the larger the groups and the better the amortisation,
    /// which is the opposite of how the one-at-a-time path degraded.
    /// </para>
    /// <para>
    /// Each batch keeps its own log record, so this changes nothing about atomicity: a batch is
    /// still all-or-nothing, and a torn record still loses exactly one.
    /// </para>
    /// </remarks>
    public async ValueTask WriteAsync(WriteBatch batch, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ObjectDisposedException.ThrowIf(_disposed, this);
        ThrowIfBackgroundFailed();

        if (batch.Count == 0) return;

        foreach (var op in batch.Ops)
        {
            ValidateSizes(op.Key, op.Value);
        }

        var pending = new PendingWrite(batch);
        _writeQueue.Enqueue(pending);

        bool frozeMemtable = false;

        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // A leader that arrived earlier may already have committed this batch, in which case
            // there is nothing to do but release the lock.
            if (!pending.Completion.Task.IsCompleted)
            {
                frozeMemtable = await CommitGroupAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _writeLock.Release();
        }

        // Rethrows if the group this batch belonged to failed.
        await pending.Completion.Task.ConfigureAwait(false);

        if (frozeMemtable)
        {
            if (_options.EnableBackgroundCompaction)
            {
                SignalMaintenance();
            }
            else
            {
                // With background work disabled the flush happens inline, so tests see a
                // deterministic tree after every write.
                await RunMaintenanceAsync(compact: false, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Commits every batch currently queued. Called holding the write lock. Returns whether the
    /// active memtable was frozen and so needs flushing.
    /// </summary>
    private async Task<bool> CommitGroupAsync(CancellationToken cancellationToken)
    {
        var group = new List<PendingWrite>();
        int operationCount = 0;

        while (group.Count < MaxGroupBatches
            && operationCount < MaxGroupOperations
            && _writeQueue.TryDequeue(out var queued))
        {
            group.Add(queued);
            operationCount += queued.Batch.Count;
        }

        if (group.Count == 0) return false;

        try
        {
            ulong firstSequence = _lastSequence + 1;

            var records = new List<(ulong FirstSequence, IReadOnlyList<WriteOp> Operations)>(group.Count);
            ulong sequence = firstSequence;
            foreach (var queued in group)
            {
                records.Add((sequence, queued.Batch.Ops));
                sequence += (ulong)queued.Batch.Count;
            }

            // Log first, then memtable. The reverse order would make a write visible before it
            // was recoverable, so a crash could lose a read that had already been served.
            await _wal.AppendGroupAsync(records, cancellationToken).ConfigureAwait(false);
            SyncIfRequired();

            var mem = Volatile.Read(ref _mem);
            ulong cursor = firstSequence;

            foreach (var queued in group)
            {
                foreach (var op in queued.Batch.Ops)
                {
                    mem.Active.Add(
                        cursor++,
                        op.IsDelete ? ValueKind.Deletion : ValueKind.Value,
                        op.Key,
                        op.Value ?? []);
                }
            }

            _lastSequence = cursor - 1;

            // Publishing last is what gives every batch in the group atomic visibility: a reader
            // works at the previously published sequence until the whole group is in place.
            Volatile.Write(ref _publishedSequence, _lastSequence);

            foreach (var queued in group)
            {
                foreach (var op in queued.Batch.Ops)
                {
                    if (op.IsDelete) Interlocked.Increment(ref _deletes);
                    else Interlocked.Increment(ref _writes);
                }
            }

            foreach (var queued in group)
            {
                queued.Completion.TrySetResult();
            }

            if (mem.Active.ApproximateBytes >= _options.MemtableSizeBytes)
            {
                FreezeActiveMemtable();
                return true;
            }

            return false;
        }
        catch (Exception exception)
        {
            // Every batch in the group shares the fate of the single append, so every caller is
            // told the same thing: the write did not happen. The exception is delivered through
            // the completions rather than thrown, so the leader learns of it the same way a
            // follower does and no caller is left waiting.
            foreach (var queued in group)
            {
                queued.Completion.TrySetException(exception);
            }

            return false;
        }
    }

    /// <summary>
    /// Retires the active memtable and installs a fresh one with a new log. Called under the
    /// write lock.
    /// </summary>
    private Memtable FreezeActiveMemtable()
    {
        var current = Volatile.Read(ref _mem);
        ulong newLogNumber = _nextFileNumber++;

        var previousWal = _wal;
        _wal = WriteAheadLog.Create(_directory, newLogNumber);
        _ = previousWal.DisposeAsync();

        var replacement = new Memtable(newLogNumber);
        Volatile.Write(ref _mem, new MemState(replacement, [current.Active, .. current.Immutable]));

        return current.Active;
    }

    private void ValidateSizes(byte[] key, byte[]? value)
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
        if (value is not null && value.Length > _options.MaxValueSize)
        {
            throw new KeyOrValueTooLargeException(
                $"Value is {value.Length} bytes, above the {_options.MaxValueSize}-byte limit.");
        }
    }

    // ================================================================ durability

    private void SyncIfRequired()
    {
        if (_options.SyncPolicy == SyncPolicy.EveryWrite)
        {
            _wal.Sync();
            Interlocked.Increment(ref _syncs);
            Interlocked.Exchange(ref _unsyncedWrites, 0);
            return;
        }

        Interlocked.Increment(ref _unsyncedWrites);
    }

    /// <inheritdoc />
    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _wal.Sync();
            Interlocked.Increment(ref _syncs);
            Interlocked.Exchange(ref _unsyncedWrites, 0);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    /// <summary>
    /// Freezes the active memtable and writes it out as an SSTable, then waits for that to
    /// finish. Mostly useful in tests and before taking a backup.
    /// </summary>
    public async ValueTask FlushMemtableAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (Volatile.Read(ref _mem).Active.Count > 0)
            {
                FreezeActiveMemtable();
            }
        }
        finally
        {
            _writeLock.Release();
        }

        await RunMaintenanceAsync(compact: false, cancellationToken).ConfigureAwait(false);
    }

    // ================================================================ snapshots

    /// <summary>
    /// Takes a snapshot of the current state. Dispose it promptly: an open snapshot prevents
    /// compaction from reclaiming the versions it pins.
    /// </summary>
    public Snapshot CreateSnapshot()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        ulong sequence = Volatile.Read(ref _publishedSequence);

        lock (_snapshotGate)
        {
            _snapshotSequences.Add(sequence);
        }

        return new Snapshot(sequence, Unregister);

        void Unregister(Snapshot snapshot)
        {
            lock (_snapshotGate)
            {
                _snapshotSequences.Remove(snapshot.Sequence);
            }
        }
    }

    /// <summary>
    /// The oldest sequence any live snapshot needs. Compaction may not discard versions at or
    /// above it.
    /// </summary>
    private ulong OldestVisibleSequence()
    {
        lock (_snapshotGate)
        {
            return _snapshotSequences.Count == 0
                ? Volatile.Read(ref _publishedSequence)
                : _snapshotSequences.Min;
        }
    }

    // ================================================================ scans

    /// <inheritdoc />
    public IAsyncEnumerable<KeyValuePair<byte[], byte[]>> ScanAsync(
        byte[]? start = null,
        byte[]? end = null,
        CancellationToken cancellationToken = default) =>
        ScanAtAsync(start, end, Volatile.Read(ref _publishedSequence), cancellationToken);

    /// <summary>Streams a key range as of <paramref name="snapshot"/>.</summary>
    public IAsyncEnumerable<KeyValuePair<byte[], byte[]>> ScanAsync(
        Snapshot snapshot,
        byte[]? start = null,
        byte[]? end = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        return ScanAtAsync(start, end, snapshot.Sequence, cancellationToken);
    }

#pragma warning disable CS1998 // the enumeration itself is synchronous; the async iterator is
                              // what lets the caller cancel between items and lets the version
                              // reference be released in a finally that spans the whole scan
    private async IAsyncEnumerable<KeyValuePair<byte[], byte[]>> ScanAtAsync(
        byte[]? start,
        byte[]? end,
        ulong snapshot,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ThrowIfBackgroundFailed();

        var mem = Volatile.Read(ref _mem);
        var version = AcquireVersion();

        try
        {
            var inputs = new List<IEntryIterator> { mem.Active.CreateIterator() };

            foreach (var frozen in mem.Immutable)
            {
                inputs.Add(frozen.CreateIterator());
            }

            // Only the tables whose key ranges intersect the requested range are opened. For a
            // narrow scan over a large database this is what keeps the cost proportional to the
            // range rather than to the data volume.
            for (int level = 0; level < version.LevelCount; level++)
            {
                foreach (var table in version.Level(level))
                {
                    if (!table.OverlapsRange(start, end)) continue;
                    inputs.Add(_tableCache.Get(table.FileNumber).CreateIterator());
                }
            }

            using var merged = new MergingIterator(inputs);

            foreach (var pair in VisibleEntries.Enumerate(merged, snapshot, start, end))
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return pair;
            }
        }
        finally
        {
            ReleaseVersion(version);
        }
    }
#pragma warning restore CS1998

    // ================================================================ maintenance

    private void SignalMaintenance()
    {
        try
        {
            _maintenanceSignal.Release();
        }
        catch (SemaphoreFullException)
        {
            // Already signalled; the loop will pick the work up on its current pass.
        }
        catch (ObjectDisposedException)
        {
            // Shutting down.
        }
    }

    private async Task MaintenanceLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await _maintenanceSignal.WaitAsync(cancellationToken).ConfigureAwait(false);
                await RunMaintenanceAsync(compact: true, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception exception)
            {
                // A failed flush or compaction means the tree is no longer being maintained.
                // Swallowing it would leave the engine quietly degrading, so it is recorded and
                // surfaced on the next public call instead.
                _backgroundFailure = exception;
                return;
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask CompactAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ThrowIfBackgroundFailed();

        await FlushMemtableAsync(cancellationToken).ConfigureAwait(false);
        await RunMaintenanceAsync(compact: true, cancellationToken).ConfigureAwait(false);
    }

    private async Task RunMaintenanceAsync(bool compact, CancellationToken cancellationToken)
    {
        await _maintenanceLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            while (await FlushOneMemtableAsync(cancellationToken).ConfigureAwait(false))
            {
                // Keep going while memtables are queued.
            }

            if (!compact) return;

            // Compaction is iterative: each job can push a level over its budget, so the planner
            // is re-run until the tree is in shape. The bound stops a pathological
            // configuration from looping forever.
            for (int pass = 0; pass < 64; pass++)
            {
                if (!await CompactOnceAsync(cancellationToken).ConfigureAwait(false)) break;
            }
        }
        finally
        {
            _maintenanceLock.Release();
        }
    }

    private async Task<bool> FlushOneMemtableAsync(CancellationToken cancellationToken)
    {
        var mem = Volatile.Read(ref _mem);
        if (mem.Immutable.Count == 0) return false;

        // Oldest first, so level-0 file numbers increase with age and the newest-first search
        // order at level 0 stays correct.
        var oldest = mem.Immutable[^1];

        ulong fileNumber;
        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            fileNumber = _nextFileNumber++;
        }
        finally
        {
            _writeLock.Release();
        }

        var meta = await WriteTableFromIteratorAsync(
                _directory,
                _options,
                fileNumber,
                oldest.CreateIterator(),
                cancellationToken)
            .ConfigureAwait(false);

        var updated = meta is null
            ? Volatile.Read(ref _version)
            : Volatile.Read(ref _version).With(level: 0, added: [meta], removed: []);

        await InstallVersionAsync(
                updated,
                retiredMemtable: oldest,
                cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        Interlocked.Increment(ref _flushes);
        if (meta is not null)
        {
            Interlocked.Add(ref _compactionBytesWritten, meta.FileSizeBytes);
        }

        return true;
    }

    private static async Task<SsTableMeta?> WriteTableFromIteratorAsync(
        string directory,
        DatabaseOptions options,
        ulong fileNumber,
        IEntryIterator source,
        CancellationToken cancellationToken)
    {
        string path = Path.Combine(directory, SsTableMeta.FileNameFor(fileNumber));

        using (source)
        {
            await using var writer = new SsTableWriter(path, options);

            source.SeekToFirst();
            while (source.IsValid)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await writer.AddAsync(source.Key.ToArray(), source.Value.ToArray(), cancellationToken)
                    .ConfigureAwait(false);
                if (!source.MoveNext()) break;
            }

            if (writer.EntryCount == 0)
            {
                await writer.DisposeAsync().ConfigureAwait(false);
                TryDelete(path);
                return null;
            }

            return await writer.FinishAsync(fileNumber, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<bool> CompactOnceAsync(CancellationToken cancellationToken)
    {
        var version = Volatile.Read(ref _version);
        var job = CompactionPlanner.Plan(version, _options);
        if (job is null) return false;

        ulong oldestVisible = OldestVisibleSequence();

        var inputs = new List<IEntryIterator>();
        foreach (var table in job.AllInputs)
        {
            inputs.Add(_tableCache.Get(table.FileNumber).CreateIterator());
        }

        var outputs = new List<SsTableMeta>();
        long bytesWritten = 0;

        using (var merged = new MergingIterator(inputs))
        {
            merged.SeekToFirst();

            SsTableWriter? writer = null;
            ulong writerFileNumber = 0;
            byte[]? lastUserKey = null;
            bool resolvedForOldestSnapshot = false;

            // A span into the merging iterator cannot be held across an await, and the writer is
            // async. Copying into reusable scratch buffers keeps the merge to zero steady-state
            // allocations while still letting the writer be awaited -- materialising a fresh
            // array per entry would make a compaction of a few million entries allocate a few
            // million arrays, and the GC, rather than the disk, would set the pace.
            byte[] keyScratch = new byte[256];
            byte[] valueScratch = new byte[1024];

            try
            {
                while (merged.IsValid)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    int keyLength;
                    int valueLength;
                    ulong sequence;
                    ValueKind kind;
                    bool firstVersionOfKey;
                    bool wellFormed;

                    // Everything that touches the iterator's spans happens in this block, with no
                    // await inside it.
                    {
                        var internalKey = merged.Key;
                        wellFormed = InternalKey.IsWellFormed(internalKey);

                        if (!wellFormed)
                        {
                            keyLength = 0;
                            valueLength = 0;
                            sequence = 0;
                            kind = ValueKind.Value;
                            firstVersionOfKey = false;
                        }
                        else
                        {
                            var userKey = InternalKey.UserKey(internalKey);
                            sequence = InternalKey.Sequence(internalKey);
                            kind = InternalKey.Kind(internalKey);
                            firstVersionOfKey =
                                lastUserKey is null || !userKey.SequenceEqual(lastUserKey);

                            if (firstVersionOfKey) lastUserKey = userKey.ToArray();

                            var value = merged.Value;

                            if (internalKey.Length > keyScratch.Length)
                            {
                                keyScratch = new byte[Math.Max(internalKey.Length, keyScratch.Length * 2)];
                            }
                            if (value.Length > valueScratch.Length)
                            {
                                valueScratch = new byte[Math.Max(value.Length, valueScratch.Length * 2)];
                            }

                            internalKey.CopyTo(keyScratch);
                            value.CopyTo(valueScratch);
                            keyLength = internalKey.Length;
                            valueLength = value.Length;
                        }
                    }

                    if (!wellFormed)
                    {
                        if (!merged.MoveNext()) break;
                        continue;
                    }

                    bool keep;

                    // Deciding which versions of a key may be discarded is the subtlest rule in
                    // the engine, and getting it wrong does not crash anything -- it silently
                    // returns the wrong answer to a snapshot read, or resurrects deleted data.
                    //
                    // Versions arrive newest-first. The newest is the current value and is always
                    // kept. Older ones may only go once some *surviving* version is already
                    // visible to the oldest live snapshot, because until then one of them is
                    // still the answer that snapshot must see. Concretely: with versions at
                    // sequences 100, 50 and 10 and an open snapshot at 20, the snapshot can see
                    // neither 100 nor 50, so version 10 is the one it reads -- and discarding it
                    // for being "old" would make the snapshot read return nothing at all.
                    if (firstVersionOfKey)
                    {
                        resolvedForOldestSnapshot = sequence <= oldestVisible;

                        // The newest version is kept, with one exception: a tombstone that no
                        // deeper level could still be shadowing, and that no snapshot needs, has
                        // finished its job and can finally be dropped.
                        keep = kind != ValueKind.Deletion
                            || !CompactionPlanner.CanDropTombstone(
                                version,
                                job.TargetLevel,
                                InternalKey.UserKey(keyScratch.AsSpan(0, keyLength)),
                                oldestVisible,
                                sequence);
                    }
                    else if (resolvedForOldestSnapshot)
                    {
                        // A version at or below the oldest snapshot has already survived, so
                        // nothing can observe this one.
                        keep = false;
                    }
                    else
                    {
                        keep = true;
                        resolvedForOldestSnapshot = sequence <= oldestVisible;
                    }

                    if (keep)
                    {
                        // Roll to a new output file once the current one reaches its target size,
                        // so that compacting a large level still produces independently
                        // compactable files rather than one enormous one.
                        //
                        // The roll happens only at a user-key boundary. Splitting a key's
                        // versions across two files would put them in two tables at the same
                        // level, which breaks that level's disjoint-range invariant and with it
                        // the binary search that assumes at most one table can hold a key.
                        if (writer is not null
                            && firstVersionOfKey
                            && writer.FileSize >= _options.TargetFileSizeBytes)
                        {
                            var rolled = await writer.FinishAsync(writerFileNumber, cancellationToken)
                                .ConfigureAwait(false);
                            outputs.Add(rolled);
                            bytesWritten += rolled.FileSizeBytes;

                            await writer.DisposeAsync().ConfigureAwait(false);
                            writer = null;
                        }

                        if (writer is null)
                        {
                            writerFileNumber = await NextFileNumberAsync(cancellationToken)
                                .ConfigureAwait(false);
                            writer = new SsTableWriter(
                                Path.Combine(_directory, SsTableMeta.FileNameFor(writerFileNumber)),
                                _options);
                        }

                        await writer.AddAsync(
                                keyScratch.AsMemory(0, keyLength),
                                valueScratch.AsMemory(0, valueLength),
                                cancellationToken)
                            .ConfigureAwait(false);
                    }

                    if (!merged.MoveNext()) break;
                }

                if (writer is not null)
                {
                    var finished = await writer.FinishAsync(writerFileNumber, cancellationToken)
                        .ConfigureAwait(false);
                    outputs.Add(finished);
                    bytesWritten += finished.FileSizeBytes;
                }
            }
            finally
            {
                if (writer is not null) await writer.DisposeAsync().ConfigureAwait(false);
            }
        }

        var updated = version.WithCompaction(
            job.SourceLevel,
            job.SourceTables,
            job.TargetLevel,
            job.TargetTables,
            outputs);

        await InstallVersionAsync(updated, retiredMemtable: null, cancellationToken)
            .ConfigureAwait(false);

        Interlocked.Increment(ref _compactions);
        Interlocked.Add(ref _compactionBytesWritten, bytesWritten);

        return true;
    }

    private async Task<ulong> NextFileNumberAsync(CancellationToken cancellationToken)
    {
        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return _nextFileNumber++;
        }
        finally
        {
            _writeLock.Release();
        }
    }

    /// <summary>
    /// Makes a new version current: persist the manifest, publish the version, then clean up
    /// whatever is no longer reachable.
    /// </summary>
    /// <remarks>
    /// The order is the crash-safety argument in three lines. The manifest goes first because it
    /// is the only durable record of what the database contains; publishing the in-memory version
    /// before it would mean a crash could leave the engine serving tables the manifest does not
    /// name. Deletion goes last because a file may only be removed once nothing — no manifest, no
    /// live version, no in-flight reader — can still reach it.
    /// </remarks>
    private async Task InstallVersionAsync(
        LsmVersion updated,
        Memtable? retiredMemtable,
        CancellationToken cancellationToken)
    {
        LsmVersion previous;
        ulong logNumber;

        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var mem = Volatile.Read(ref _mem);

            if (retiredMemtable is not null)
            {
                var remaining = mem.Immutable.Where(m => !ReferenceEquals(m, retiredMemtable)).ToList();
                Volatile.Write(ref _mem, new MemState(mem.Active, remaining));
                mem = Volatile.Read(ref _mem);
            }

            // The oldest log still backing a live memtable. Everything below it has been
            // durably flushed into SSTables and is now redundant.
            logNumber = mem.Immutable.Count > 0
                ? mem.Immutable.Min(m => m.LogNumber)
                : mem.Active.LogNumber;

            Manifest.Save(
                _directory,
                Manifest.From(updated, _nextFileNumber, _lastSequence, logNumber));

            previous = Interlocked.Exchange(ref _version, updated);

            lock (_snapshotGate)
            {
                _liveVersions.Add(updated);
            }
        }
        finally
        {
            _writeLock.Release();
        }

        if (previous.Release())
        {
            lock (_snapshotGate)
            {
                _liveVersions.Remove(previous);
            }
        }

        RemoveObsoleteFiles(logNumber);
    }

    /// <summary>
    /// Deletes tables no live version references, and logs no live memtable needs.
    /// </summary>
    private void RemoveObsoleteFiles(ulong logNumber)
    {
        HashSet<ulong> reachable;

        lock (_snapshotGate)
        {
            reachable = _liveVersions
                .Where(version => version.IsLive)
                .SelectMany(version => version.FileNumbers)
                .ToHashSet();
        }

        foreach (ulong number in Volatile.Read(ref _version).FileNumbers)
        {
            reachable.Add(number);
        }

        foreach (string path in System.IO.Directory.EnumerateFiles(_directory, "*.sst"))
        {
            if (!ulong.TryParse(Path.GetFileNameWithoutExtension(path), out ulong number)) continue;
            if (reachable.Contains(number)) continue;

            // Close the reader before unlinking. On Unix the delete would succeed either way,
            // but Windows refuses to remove an open file, and a storage engine that only works
            // on one platform is a worse engine.
            _tableCache.Evict(number);
            TryDelete(path);
        }

        foreach (ulong number in FindLogNumbers(_directory).ToList())
        {
            if (number >= logNumber) continue;
            TryDelete(Path.Combine(_directory, WriteAheadLog.FileNameFor(number)));
        }
    }

    private LsmVersion AcquireVersion()
    {
        while (true)
        {
            var version = Volatile.Read(ref _version);
            if (version.TryAddRef()) return version;
        }
    }

    private void ReleaseVersion(LsmVersion version)
    {
        if (!version.Release()) return;

        lock (_snapshotGate)
        {
            _liveVersions.Remove(version);
        }
    }

    private void ThrowIfBackgroundFailed()
    {
        if (_backgroundFailure is { } failure)
        {
            throw new KestrelCacheException(
                "Background flush or compaction failed; the database is no longer being "
                    + "maintained. See the inner exception.",
                failure);
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
            // Best effort; an undeleted obsolete file wastes space but breaks nothing.
        }
        catch (UnauthorizedAccessException)
        {
            // Same.
        }
    }

    // ================================================================ stats & teardown

    /// <inheritdoc />
    public EngineStats GetStats()
    {
        var version = Volatile.Read(ref _version);
        var mem = Volatile.Read(ref _mem);

        long tableBytes = version.TotalSizeBytes;
        long walBytes = _wal.SizeBytes;

        return new EngineStats
        {
            Engine = Name,
            // An upper bound: every version of every key, tombstones included.
            KeyCount = version.TotalEntryCount + mem.Active.Count
                + mem.Immutable.Sum(m => (long)m.Count),
            KeyCountIsExact = false,
            DiskSizeBytes = tableBytes + walBytes,
            DataFileBytes = tableBytes,
            WriteAheadLogBytes = walBytes,

            // Bytes resident in SSTables, stale versions included. Marked inexact by
            // KeyCountIsExact above, which is what keeps StaleRatio from reporting a number it
            // cannot actually compute.
            LiveDataBytes = tableBytes,
            Reads = Interlocked.Read(ref _reads),
            Writes = Interlocked.Read(ref _writes),
            Deletes = Interlocked.Read(ref _deletes),
            Syncs = Interlocked.Read(ref _syncs) + _wal.Syncs,
            Compactions = Interlocked.Read(ref _compactions),
            CompactionBytesWritten = Interlocked.Read(ref _compactionBytesWritten),
            BloomFilterNegatives = Interlocked.Read(ref _bloomNegatives),
            BloomFilterFalsePositives = Interlocked.Read(ref _bloomFalsePositives),
            BlockCacheHits = _blockCache.Hits,
            BlockCacheMisses = _blockCache.Misses,
            SsTablesPerLevel = version.TableCountsPerLevel,
        };
    }

    /// <summary>Memtable flushes completed since open.</summary>
    public long FlushCount => Interlocked.Read(ref _flushes);

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        await _shutdown.CancelAsync().ConfigureAwait(false);

        if (_maintenanceLoop is not null)
        {
            try
            {
                await _maintenanceLoop.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown.
            }
        }

        try
        {
            _wal.Sync();
        }
        catch (ObjectDisposedException)
        {
            // Already closed.
        }

        await _wal.DisposeAsync().ConfigureAwait(false);
        _tableCache.Dispose();
        _blockCache.Clear();

        _shutdown.Dispose();
        _writeLock.Dispose();
        _maintenanceLock.Dispose();
        _maintenanceSignal.Dispose();
    }
}

/// <summary>What LSM startup recovery found and did.</summary>
public sealed record LsmRecoveryReport
{
    /// <summary>Write-ahead log files replayed.</summary>
    public int LogsReplayed { get; init; }

    /// <summary>Batch records replayed across all logs.</summary>
    public long RecordsReplayed { get; init; }

    /// <summary>Individual mutations replayed.</summary>
    public long EntriesReplayed { get; init; }

    /// <summary>Entries written out as a level-0 table during recovery.</summary>
    public long EntriesFlushedToTable { get; init; }

    /// <summary>Bytes of torn log tail discarded.</summary>
    public long BytesTruncated { get; init; }

    /// <summary>Why log replay stopped early, when it did.</summary>
    public string? TruncationReason { get; init; }

    /// <summary>
    /// Tables found on disk that the manifest did not reference, left behind by a crash between
    /// writing a table and installing the manifest that named it.
    /// </summary>
    public long OrphanTablesRemoved { get; init; }

    /// <summary>Bytes reclaimed from orphan tables.</summary>
    public long OrphanBytesReclaimed { get; init; }

    /// <summary>Tables the manifest referenced.</summary>
    public int TablesOpened { get; init; }

    /// <summary>Highest sequence number recovered.</summary>
    public ulong LastSequence { get; init; }

    /// <summary>True when nothing had to be discarded or reclaimed.</summary>
    public bool Clean => BytesTruncated == 0 && OrphanTablesRemoved == 0;
}
