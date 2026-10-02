using System.Buffers;
using KestrelCache.Internal;
using Microsoft.Win32.SafeHandles;

namespace KestrelCache.Lsm;

/// <summary>
/// Reads one immutable SSTable: Bloom filter, block index, then at most one data block per
/// lookup.
/// </summary>
/// <remarks>
/// <para>
/// A point lookup costs, in the best case, nothing at all: the Bloom filter rules the key out in
/// memory. Otherwise it is a binary search of the in-memory index to find the one block that
/// could hold the key, a single read of that block (often served by the block cache), and a
/// binary search within it over the restart points. One disk read, bounded and predictable.
/// </para>
/// <para>
/// The filter and index are loaded once at open and held for the reader's lifetime. They are
/// small — around 1.25 bytes per key for the filter and one entry per 4 KiB block for the index
/// — and they are consulted on every single lookup, so paying for them repeatedly would be the
/// worst possible place to economise.
/// </para>
/// <para>
/// Because the file never changes, none of this needs synchronisation. Readers share one
/// instance and one file descriptor, and all I/O is positional, so any number of them can read
/// concurrently without a lock.
/// </para>
/// </remarks>
internal sealed class SsTableReader : IDisposable
{
    private readonly FileStream _stream;
    private readonly SafeFileHandle _handle;
    private readonly BlockCache? _cache;
    private readonly byte[] _filter;
    private readonly BlockReader _index;
    private readonly long _fileLength;

    private SsTableReader(
        ulong fileNumber,
        string path,
        FileStream stream,
        byte[] filter,
        BlockReader index,
        long fileLength,
        BlockCache? cache)
    {
        FileNumber = fileNumber;
        Path = path;
        _stream = stream;
        _handle = stream.SafeFileHandle;
        _filter = filter;
        _index = index;
        _fileLength = fileLength;
        _cache = cache;
    }

    /// <summary>The table's file number.</summary>
    internal ulong FileNumber { get; }

    /// <summary>Path of the table file.</summary>
    internal string Path { get; }

    /// <summary>Opens a table, validating its footer and loading its filter and index.</summary>
    internal static SsTableReader Open(string path, ulong fileNumber, BlockCache? cache)
    {
        var stream = new FileStream(
            path,
            new FileStreamOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.Read,
                BufferSize = 0,
                Options = FileOptions.Asynchronous | FileOptions.RandomAccess,
            });

        try
        {
            var handle = stream.SafeFileHandle;
            long length = RandomAccess.GetLength(handle);

            if (length < SsTableFooter.Size)
            {
                throw new CorruptRecordException(
                    $"SSTable is {length} bytes, too short to hold a footer.", 0, path);
            }

            Span<byte> footerBytes = stackalloc byte[SsTableFooter.Size];
            PositionalIo.ReadExactly(handle, footerBytes, length - SsTableFooter.Size);
            var footer = SsTableFooter.Decode(footerBytes, path, length);

            byte[] filter = ReadBlockUncached(handle, footer.Filter, path);
            byte[] indexBytes = ReadBlockUncached(handle, footer.Index, path);
            var index = BlockReader.Open(indexBytes, path, footer.Index.Offset);

            return new SsTableReader(fileNumber, path, stream, filter, index, length, cache);
        }
        catch
        {
            stream.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Whether the Bloom filter can rule this key out without any I/O.
    /// </summary>
    internal bool MayContain(ReadOnlySpan<byte> userKey) =>
        BloomFilter.MayContain(_filter, userKey);

    /// <summary>
    /// Looks up the newest entry for <paramref name="userKey"/> at or below
    /// <paramref name="snapshotSequence"/>.
    /// </summary>
    internal LookupResult TryGet(
        ReadOnlySpan<byte> userKey,
        ulong snapshotSequence,
        out byte[]? value)
    {
        value = null;

        byte[] probe = InternalKey.Encode(
            userKey, InternalKey.ClampSnapshot(snapshotSequence), ValueKind.Value);

        // Find the one data block that could hold the key. Index entries are the last key of
        // each block, so the first entry at or after the probe names the right block.
        var indexCursor = _index.CreateCursor();
        if (!indexCursor.Seek(probe))
        {
            return LookupResult.NotFound;
        }

        var handle = BlockHandle.Decode(indexCursor.Value);
        byte[] block = ReadBlock(handle);

        var cursor = BlockReader.Open(block, Path, handle.Offset).CreateCursor();
        if (!cursor.Seek(probe))
        {
            return LookupResult.NotFound;
        }

        if (!InternalKey.IsWellFormed(cursor.Key)
            || !InternalKey.UserKey(cursor.Key).SequenceEqual(userKey))
        {
            // The block's first candidate belongs to a different user key, so this table holds
            // no visible entry for the one requested. On a Bloom-filter false positive this is
            // the path taken.
            return LookupResult.NotFound;
        }

        if (InternalKey.Kind(cursor.Key) == ValueKind.Deletion)
        {
            return LookupResult.Deleted;
        }

        value = cursor.Value.ToArray();
        return LookupResult.Found;
    }

    /// <summary>Creates a cursor over the whole table, for scans and compaction.</summary>
    internal IEntryIterator CreateIterator() => new TableIterator(this);

    private byte[] ReadBlock(BlockHandle handle)
    {
        if (_cache is not null && _cache.TryGet(FileNumber, handle.Offset, out byte[]? cached))
        {
            return cached!;
        }

        byte[] block = ReadBlockUncached(_handle, handle, Path);
        _cache?.Put(FileNumber, handle.Offset, block);
        return block;
    }

    private static byte[] ReadBlockUncached(SafeFileHandle handle, BlockHandle block, string path)
    {
        int total = block.Size + BlockHandle.TrailerSize;
        byte[] rented = ArrayPool<byte>.Shared.Rent(total);

        try
        {
            var buffer = rented.AsSpan(0, total);
            PositionalIo.ReadExactly(handle, buffer, block.Offset);

            var stored = buffer[..block.Size];
            var trailer = buffer[block.Size..];

            var kind = BlockHandle.ValidateTrailer(stored, trailer, block.Offset, path);
            return BlockCompression.Decompress(stored, kind, block.Offset);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    public void Dispose() => _stream.Dispose();

    /// <summary>
    /// A two-level cursor: one over the index block, and one over the data block it currently
    /// points at.
    /// </summary>
    /// <remarks>
    /// Compaction and range scans both stream whole tables through this, which is why the inner
    /// cursor is advanced block by block rather than by loading the file. A 64 MB table is
    /// traversed with one 4 KiB block resident at a time.
    /// </remarks>
    private sealed class TableIterator(SsTableReader reader) : IEntryIterator
    {
        private BlockReader.Cursor? _indexCursor;
        private BlockReader.Cursor? _dataCursor;
        private bool _valid;

        public bool IsValid => _valid;

        public ReadOnlySpan<byte> Key => _dataCursor!.Key;

        public ReadOnlySpan<byte> Value => _dataCursor!.Value;

        public void SeekToFirst()
        {
            _indexCursor = reader._index.CreateCursor();
            _indexCursor.SeekToFirst();
            _valid = LoadCurrentBlock(seekTo: null);
            AdvancePastEmptyBlocks();
        }

        public void Seek(ReadOnlySpan<byte> internalKey)
        {
            _indexCursor = reader._index.CreateCursor();
            if (!_indexCursor.Seek(internalKey))
            {
                _valid = false;
                return;
            }

            _valid = LoadCurrentBlock(internalKey.ToArray());
            AdvancePastEmptyBlocks();
        }

        public bool MoveNext()
        {
            if (_indexCursor is null)
            {
                SeekToFirst();
                return _valid;
            }

            if (!_valid) return false;

            if (_dataCursor is not null && _dataCursor.MoveNext())
            {
                return true;
            }

            // Exhausted this block; step the index to the next one.
            if (!_indexCursor.MoveNext())
            {
                _valid = false;
                return false;
            }

            _valid = LoadCurrentBlock(seekTo: null);
            AdvancePastEmptyBlocks();
            return _valid;
        }

        private void AdvancePastEmptyBlocks()
        {
            while (_valid && _dataCursor is { IsValid: false })
            {
                if (!_indexCursor!.MoveNext())
                {
                    _valid = false;
                    return;
                }
                _valid = LoadCurrentBlock(seekTo: null);
            }
        }

        private bool LoadCurrentBlock(byte[]? seekTo)
        {
            if (_indexCursor is not { IsValid: true }) return false;

            var handle = BlockHandle.Decode(_indexCursor.Value);
            byte[] block = reader.ReadBlock(handle);
            _dataCursor = BlockReader.Open(block, reader.Path, handle.Offset).CreateCursor();

            if (seekTo is null)
            {
                _dataCursor.SeekToFirst();
            }
            else
            {
                _dataCursor.Seek(seekTo);
            }

            return _dataCursor.IsValid || true; // AdvancePastEmptyBlocks handles an empty block
        }

        public void Dispose()
        {
        }
    }
}
