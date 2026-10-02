using System.Buffers;
using KestrelCache.Internal;
using Microsoft.Win32.SafeHandles;

namespace KestrelCache.Lsm;

/// <summary>
/// Writes one immutable, sorted table file in a single forward pass.
/// </summary>
/// <remarks>
/// <para>
/// Everything about the output is append-only and written once: data blocks in key order, then
/// the Bloom filter, then the block index, then the footer. Nothing is ever revisited, which is
/// why producing an SSTable costs one sequential write of its whole size regardless of the key
/// distribution — the property that makes an LSM tree's writes cheap in the first place.
/// </para>
/// <para>
/// Immutability is doing a lot of quiet work here. Because a finished table never changes, it
/// needs no locking to read, its blocks can be cached indefinitely without invalidation, and its
/// index and filter can be loaded once and shared by every reader. Compaction replaces tables
/// rather than editing them, so "update in place" never has to be made concurrency-safe.
/// </para>
/// </remarks>
internal sealed class SsTableWriter : IAsyncDisposable
{
    private readonly FileStream _stream;
    private readonly SafeFileHandle _handle;
    private readonly DatabaseOptions _options;
    private readonly BlockBuilder _dataBlock = new();
    private readonly BlockBuilder _indexBlock = new();
    private readonly List<byte[]> _filterKeys = [];

    private long _offset;
    private byte[]? _smallestKey;
    private byte[]? _largestKey;
    private ulong _smallestSequence = ulong.MaxValue;
    private ulong _largestSequence;
    private long _entryCount;
    private long _uncompressedBytes;

    // An index entry can only be written once the *next* block's first key is known, because the
    // separator between two blocks has to be a key that is >= everything in the earlier block
    // and < everything in the later one. So a finished block waits here until then.
    private BlockHandle? _pendingIndexEntry;

    internal SsTableWriter(string path, DatabaseOptions options)
    {
        _options = options;
        _stream = new FileStream(
            path,
            new FileStreamOptions
            {
                Mode = FileMode.Create,
                Access = FileAccess.ReadWrite,
                Share = FileShare.Read,
                BufferSize = 0,
                Options = FileOptions.Asynchronous,
            });
        _handle = _stream.SafeFileHandle;
        Path = path;
    }

    /// <summary>Where the table is being written.</summary>
    internal string Path { get; }

    /// <summary>Entries written so far.</summary>
    internal long EntryCount => _entryCount;

    /// <summary>Bytes written to the file so far.</summary>
    internal long FileSize => _offset;

    /// <summary>
    /// Appends an entry. Internal keys must arrive in ascending
    /// <see cref="InternalKey.Comparer"/> order.
    /// </summary>
    internal async ValueTask AddAsync(
        ReadOnlyMemory<byte> internalKey,
        ReadOnlyMemory<byte> value,
        CancellationToken cancellationToken = default)
    {
        if (!InternalKey.IsWellFormed(internalKey.Span))
        {
            throw new ArgumentException("Not a well-formed internal key.", nameof(internalKey));
        }

        _smallestKey ??= internalKey.ToArray();
        _largestKey = internalKey.ToArray();

        ulong sequence = InternalKey.Sequence(internalKey.Span);
        _smallestSequence = Math.Min(_smallestSequence, sequence);
        _largestSequence = Math.Max(_largestSequence, sequence);

        _filterKeys.Add(InternalKey.UserKey(internalKey.Span).ToArray());

        _dataBlock.Add(internalKey.Span, value.Span);
        _entryCount++;
        _uncompressedBytes += internalKey.Length + value.Length;

        if (_dataBlock.EstimatedSize >= _options.BlockSizeBytes)
        {
            await FlushDataBlockAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask FlushDataBlockAsync(CancellationToken cancellationToken)
    {
        if (_dataBlock.IsEmpty) return;

        byte[] separator = _dataBlock.LastKey.ToArray();
        var handle = await WriteBlockAsync(_dataBlock.Finish(), compress: true, cancellationToken)
            .ConfigureAwait(false);

        // The previous block's index entry can now be emitted: this block's last key separates
        // it from whatever comes next.
        if (_pendingIndexEntry is { } pending)
        {
            _indexBlock.Add(_pendingSeparator!, pending.Encode());
        }

        _pendingIndexEntry = handle;
        _pendingSeparator = separator;

        _dataBlock.Reset();
    }

    private byte[]? _pendingSeparator;

    private async ValueTask<BlockHandle> WriteBlockAsync(
        ReadOnlyMemory<byte> block,
        bool compress,
        CancellationToken cancellationToken)
    {
        var stored = BlockCompression.Compress(
            block,
            enabled: compress && _options.CompressBlocks,
            out var kind);

        byte[] trailer = ArrayPool<byte>.Shared.Rent(BlockHandle.TrailerSize);
        try
        {
            BlockHandle.WriteTrailer(
                trailer.AsSpan(0, BlockHandle.TrailerSize), stored.Span, kind);

            long blockOffset = _offset;
            await PositionalIo.WriteAllAsync(_handle, stored, blockOffset, cancellationToken)
                .ConfigureAwait(false);
            await PositionalIo.WriteAllAsync(
                    _handle,
                    trailer.AsMemory(0, BlockHandle.TrailerSize),
                    blockOffset + stored.Length,
                    cancellationToken)
                .ConfigureAwait(false);

            _offset = blockOffset + stored.Length + BlockHandle.TrailerSize;
            return new BlockHandle(blockOffset, stored.Length);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(trailer);
        }
    }

    /// <summary>
    /// Writes the filter, index and footer, forces the whole file to disk, and returns its
    /// metadata.
    /// </summary>
    internal async ValueTask<SsTableMeta> FinishAsync(
        ulong fileNumber,
        CancellationToken cancellationToken = default)
    {
        await FlushDataBlockAsync(cancellationToken).ConfigureAwait(false);

        if (_pendingIndexEntry is { } pending)
        {
            _indexBlock.Add(_pendingSeparator!, pending.Encode());
            _pendingIndexEntry = null;
        }

        // The filter is never compressed: it is already a dense bit array, so a compressor can
        // only make it bigger, and it is read on every lookup that touches this table.
        byte[] filter = BloomFilter.Build(_filterKeys, _options.BloomBitsPerKey);
        var filterHandle = await WriteBlockAsync(filter, compress: false, cancellationToken)
            .ConfigureAwait(false);

        var indexHandle = await WriteBlockAsync(_indexBlock.Finish(), compress: false, cancellationToken)
            .ConfigureAwait(false);

        byte[] footer = new byte[SsTableFooter.Size];
        new SsTableFooter(filterHandle, indexHandle).Encode(footer);
        await PositionalIo.WriteAllAsync(_handle, footer, _offset, cancellationToken)
            .ConfigureAwait(false);
        _offset += SsTableFooter.Size;

        // The table must be durable before anything references it. The manifest naming this file
        // is written afterwards, so the ordering guarantees a crash can never leave a manifest
        // pointing at a file whose contents did not survive.
        _stream.Flush(flushToDisk: true);

        return new SsTableMeta
        {
            FileNumber = fileNumber,
            FileSizeBytes = _offset,
            SmallestKey = _smallestKey ?? [],
            LargestKey = _largestKey ?? [],
            SmallestSequence = _smallestSequence == ulong.MaxValue ? 0 : _smallestSequence,
            LargestSequence = _largestSequence,
            EntryCount = _entryCount,
        };
    }

    public ValueTask DisposeAsync()
    {
        _stream.Dispose();
        return ValueTask.CompletedTask;
    }
}
