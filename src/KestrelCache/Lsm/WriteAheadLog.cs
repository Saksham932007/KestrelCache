using System.Buffers;
using System.Buffers.Binary;
using System.IO.Hashing;
using KestrelCache.Internal;
using Microsoft.Win32.SafeHandles;

namespace KestrelCache.Lsm;

/// <summary>One mutation recovered from the log.</summary>
internal readonly record struct LogEntry(ulong Sequence, ValueKind Kind, byte[] Key, byte[] Value);

/// <summary>
/// The write-ahead log: the only thing that makes a memtable's contents survive a crash.
/// </summary>
/// <remarks>
/// <para>
/// A write is acknowledged once it is in the memtable, and the memtable is in RAM. The log is
/// what bridges that gap: every mutation is appended here first, so a crash loses nothing that
/// was acknowledged — recovery replays the log back into a fresh memtable and carries on. This
/// is the entire reason an LSM tree can buffer writes in memory at all, and it is why "write-
/// ahead" is the operative word: the log entry must be durable <i>before</i> the write is
/// visible, never after.
/// </para>
/// <para>
/// The log pays for itself by being purely sequential. One append per batch, one fsync, no
/// seeks — which is why the write path is fast despite doing the work twice (once to the log,
/// later to an SSTable). Sequential writes are orders of magnitude cheaper than the random ones
/// an in-place B-tree would need, so doing them twice still wins.
/// </para>
/// <para><b>Record layout</b></para>
/// <code>
/// uint32 crc32        over the length field and the payload
/// uint32 payloadSize
/// bytes  payload      one whole batch:
///                       varint firstSequence
///                       varint operationCount
///                       per operation:
///                         byte   kind (0 = deletion, 1 = value)
///                         varint keyLength,   bytes key
///                         varint valueLength, bytes value   (absent for a deletion)
/// </code>
/// <para>
/// A whole batch occupies exactly one record, which is what makes batches atomic. There is no
/// partially-applied state to reason about: either the record's checksum validates and the batch
/// is applied in full, or it does not and the batch never happened. A torn final record — the
/// expected result of crashing mid-append — simply fails its checksum and is discarded.
/// </para>
/// </remarks>
internal sealed class WriteAheadLog : IAsyncDisposable
{
    private const int HeaderSize = 8;

    private readonly FileStream _stream;
    private readonly SafeFileHandle _handle;
    private long _offset;
    private long _syncs;

    private WriteAheadLog(ulong logNumber, string path, FileStream stream, long offset)
    {
        LogNumber = logNumber;
        Path = path;
        _stream = stream;
        _handle = stream.SafeFileHandle;
        _offset = offset;
    }

    /// <summary>The log's file number, which ties it to the memtable it backs.</summary>
    internal ulong LogNumber { get; }

    /// <summary>Path of the log file.</summary>
    internal string Path { get; }

    /// <summary>Bytes written so far.</summary>
    internal long SizeBytes => Volatile.Read(ref _offset);

    /// <summary>fsync calls issued against this log.</summary>
    internal long Syncs => Interlocked.Read(ref _syncs);

    /// <summary>Conventional filename for a log number.</summary>
    internal static string FileNameFor(ulong logNumber) => $"{logNumber:D6}.wal";

    /// <summary>Creates a fresh log, truncating anything already at that path.</summary>
    internal static WriteAheadLog Create(string directory, ulong logNumber)
    {
        string path = System.IO.Path.Combine(directory, FileNameFor(logNumber));

        var stream = new FileStream(
            path,
            new FileStreamOptions
            {
                Mode = FileMode.Create,
                Access = FileAccess.ReadWrite,
                Share = FileShare.Read,
                BufferSize = 0,
                Options = FileOptions.Asynchronous,
            });

        return new WriteAheadLog(logNumber, path, stream, offset: 0);
    }

    /// <summary>
    /// Appends a whole batch as one record and returns the sequence number assigned to the last
    /// operation in it.
    /// </summary>
    internal async ValueTask AppendAsync(
        ulong firstSequence,
        IReadOnlyList<WriteOp> operations,
        CancellationToken cancellationToken = default)
    {
        int total = HeaderSize + PayloadSizeOf(firstSequence, operations);
        byte[] rented = ArrayPool<byte>.Shared.Rent(total);

        try
        {
            int written = EncodeRecord(rented.AsSpan(0, total), firstSequence, operations);

            long offset = _offset;
            await PositionalIo
                .WriteAllAsync(_handle, rented.AsMemory(0, written), offset, cancellationToken)
                .ConfigureAwait(false);

            _offset = offset + written;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>Encoded payload size of one batch record, excluding its header.</summary>
    private static int PayloadSizeOf(ulong firstSequence, IReadOnlyList<WriteOp> operations)
    {
        int size = Varint.SizeOf(firstSequence) + Varint.SizeOf((uint)operations.Count);

        foreach (var op in operations)
        {
            size += 1 + Varint.SizeOf((uint)op.Key.Length) + op.Key.Length;
            if (!op.IsDelete)
            {
                size += Varint.SizeOf((uint)op.Value!.Length) + op.Value.Length;
            }
        }

        return size;
    }

    /// <summary>Writes one complete record and returns its total length.</summary>
    private static int EncodeRecord(
        Span<byte> destination,
        ulong firstSequence,
        IReadOnlyList<WriteOp> operations)
    {
        var payload = destination[HeaderSize..];
        int cursor = 0;

        cursor += Varint.Write(payload[cursor..], firstSequence);
        cursor += Varint.Write(payload[cursor..], (uint)operations.Count);

        foreach (var op in operations)
        {
            payload[cursor++] = (byte)(op.IsDelete ? ValueKind.Deletion : ValueKind.Value);
            cursor += Varint.Write(payload[cursor..], (uint)op.Key.Length);
            op.Key.CopyTo(payload[cursor..]);
            cursor += op.Key.Length;

            if (!op.IsDelete)
            {
                cursor += Varint.Write(payload[cursor..], (uint)op.Value!.Length);
                op.Value.CopyTo(payload[cursor..]);
                cursor += op.Value.Length;
            }
        }

        BinaryPrimitives.WriteUInt32LittleEndian(destination[4..], (uint)cursor);

        // The checksum spans the length field as well as the payload, so a corrupt length cannot
        // send the reader off to parse garbage.
        uint crc = Crc32.HashToUInt32(destination[4..(HeaderSize + cursor)]);
        BinaryPrimitives.WriteUInt32LittleEndian(destination, crc);

        return HeaderSize + cursor;
    }

    /// <summary>
    /// Appends several batches as separate records in a single write.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the log-side half of group commit. Each batch keeps its own checksummed record,
    /// so per-batch atomicity across recovery is unchanged — a torn record still loses exactly
    /// one batch and never half of one. What changes is that the whole group leaves the process
    /// in one <c>pwrite</c> and is made durable by one <c>fsync</c>, instead of one of each per
    /// batch.
    /// </para>
    /// <para>
    /// The measurement that motivated it: write throughput sat at roughly 49,000 ops/sec with
    /// fsync disabled entirely, and client-side pipelining lifted it only to 49,000 from 40,000
    /// while lifting reads from 59,000 to 281,000. Reads scaled with concurrency and writes did
    /// not, which is the signature of a serialised path rather than a slow device — every writer
    /// was taking the same lock and issuing its own syscall, so the lock <i>was</i> the queue and
    /// no amount of client pipelining could widen it.
    /// </para>
    /// </remarks>
    internal async ValueTask AppendGroupAsync(
        IReadOnlyList<(ulong FirstSequence, IReadOnlyList<WriteOp> Operations)> group,
        CancellationToken cancellationToken = default)
    {
        if (group.Count == 0) return;
        if (group.Count == 1)
        {
            await AppendAsync(group[0].FirstSequence, group[0].Operations, cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        int total = 0;
        foreach (var (firstSequence, operations) in group)
        {
            total += HeaderSize + PayloadSizeOf(firstSequence, operations);
        }

        byte[] rented = ArrayPool<byte>.Shared.Rent(total);
        try
        {
            int cursor = 0;
            foreach (var (firstSequence, operations) in group)
            {
                cursor += EncodeRecord(rented.AsSpan(cursor), firstSequence, operations);
            }

            long offset = _offset;
            await PositionalIo
                .WriteAllAsync(_handle, rented.AsMemory(0, cursor), offset, cancellationToken)
                .ConfigureAwait(false);

            // Advanced only after the write succeeds, so a failure leaves the log exactly as it
            // was and the bytes that did land are simply overwritten by the next append.
            _offset = offset + cursor;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>Forces the log to stable storage.</summary>
    internal void Sync()
    {
        _stream.Flush(flushToDisk: true);
        Interlocked.Increment(ref _syncs);
    }

    /// <summary>
    /// Replays a log file, invoking <paramref name="apply"/> once per mutation in order, and
    /// reports how it ended.
    /// </summary>
    /// <remarks>
    /// Damage is always treated as a torn tail, because that is the only thing an interrupted
    /// append can produce: the log is written strictly front to back and never revisited, so a
    /// record that fails its checksum cannot have valid records after it. Replay stops there and
    /// the file is truncated back to the last good record, which is also what lets the log be
    /// reused for further writes instead of being abandoned.
    /// </remarks>
    internal static WalReplayResult Replay(string path, Action<LogEntry> apply, DatabaseOptions options)
    {
        if (!File.Exists(path))
        {
            return new WalReplayResult();
        }

        using var stream = new FileStream(
            path,
            new FileStreamOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.ReadWrite,
                Share = FileShare.Read,
                BufferSize = 0,
            });

        var handle = stream.SafeFileHandle;
        long length = RandomAccess.GetLength(handle);
        long offset = 0;
        long records = 0;
        long entries = 0;
        ulong lastSequence = 0;
        string? stopReason = null;

        Span<byte> header = stackalloc byte[HeaderSize];

        while (offset < length)
        {
            if (length - offset < HeaderSize)
            {
                stopReason = $"partial record header ({length - offset} byte(s) left)";
                break;
            }

            PositionalIo.ReadExactly(handle, header, offset);
            uint storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(header);
            uint payloadSize = BinaryPrimitives.ReadUInt32LittleEndian(header[4..]);

            if (payloadSize == 0 || offset + HeaderSize + (long)payloadSize > length)
            {
                stopReason = $"record declares a {payloadSize}-byte payload that does not fit";
                break;
            }

            byte[] rented = ArrayPool<byte>.Shared.Rent((int)payloadSize + 4);
            try
            {
                // Read the length field together with the payload so the checksum can be
                // verified over exactly the bytes it covers.
                var checked_ = rented.AsSpan(0, (int)payloadSize + 4);
                PositionalIo.ReadExactly(handle, checked_, offset + 4);

                if (Crc32.HashToUInt32(checked_) != storedCrc)
                {
                    stopReason = "checksum mismatch";
                    break;
                }

                var payload = checked_[4..];
                int cursor = 0;

                if (!Varint.TryRead(payload, ref cursor, out ulong firstSequence)
                    || !Varint.TryRead(payload, ref cursor, out uint count))
                {
                    stopReason = "malformed batch header";
                    break;
                }

                var batch = new List<LogEntry>((int)count);
                bool malformed = false;

                for (uint i = 0; i < count && !malformed; i++)
                {
                    if (cursor >= payload.Length)
                    {
                        malformed = true;
                        break;
                    }

                    var kind = (ValueKind)payload[cursor++];
                    if (kind is not (ValueKind.Value or ValueKind.Deletion))
                    {
                        malformed = true;
                        break;
                    }

                    if (!Varint.TryRead(payload, ref cursor, out uint keyLength)
                        || keyLength == 0
                        || keyLength > options.MaxKeySize
                        || cursor + keyLength > payload.Length)
                    {
                        malformed = true;
                        break;
                    }

                    byte[] key = payload.Slice(cursor, (int)keyLength).ToArray();
                    cursor += (int)keyLength;

                    byte[] value = [];
                    if (kind == ValueKind.Value)
                    {
                        if (!Varint.TryRead(payload, ref cursor, out uint valueLength)
                            || valueLength > options.MaxValueSize
                            || cursor + valueLength > payload.Length)
                        {
                            malformed = true;
                            break;
                        }

                        value = payload.Slice(cursor, (int)valueLength).ToArray();
                        cursor += (int)valueLength;
                    }

                    batch.Add(new LogEntry(firstSequence + i, kind, key, value));
                }

                if (malformed)
                {
                    stopReason = "malformed batch body";
                    break;
                }

                // Apply only after the whole batch has parsed. This is what makes the batch
                // atomic across recovery: a half-decodable record contributes nothing.
                foreach (var entry in batch)
                {
                    apply(entry);
                    lastSequence = Math.Max(lastSequence, entry.Sequence);
                    entries++;
                }

                records++;
                offset += HeaderSize + payloadSize;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        long truncated = 0;
        if (offset < length)
        {
            truncated = length - offset;
            stream.SetLength(offset);
            stream.Flush(flushToDisk: true);
        }

        return new WalReplayResult
        {
            Records = records,
            Entries = entries,
            LastSequence = lastSequence,
            BytesTruncated = truncated,
            StopReason = truncated > 0 ? stopReason : null,
        };
    }

    /// <summary>Reopens an existing log for appending, positioned at its end.</summary>
    internal static WriteAheadLog OpenForAppend(string directory, ulong logNumber)
    {
        string path = System.IO.Path.Combine(directory, FileNameFor(logNumber));

        var stream = new FileStream(
            path,
            new FileStreamOptions
            {
                Mode = FileMode.OpenOrCreate,
                Access = FileAccess.ReadWrite,
                Share = FileShare.Read,
                BufferSize = 0,
                Options = FileOptions.Asynchronous,
            });

        long length = RandomAccess.GetLength(stream.SafeFileHandle);
        return new WriteAheadLog(logNumber, path, stream, length);
    }

    public ValueTask DisposeAsync()
    {
        _stream.Dispose();
        return ValueTask.CompletedTask;
    }
}

/// <summary>What replaying one log file found.</summary>
internal sealed record WalReplayResult
{
    /// <summary>Batch records applied.</summary>
    public long Records { get; init; }

    /// <summary>Individual mutations applied.</summary>
    public long Entries { get; init; }

    /// <summary>Highest sequence number seen.</summary>
    public ulong LastSequence { get; init; }

    /// <summary>Bytes of torn tail discarded.</summary>
    public long BytesTruncated { get; init; }

    /// <summary>Why replay stopped early, when it did.</summary>
    public string? StopReason { get; init; }
}
