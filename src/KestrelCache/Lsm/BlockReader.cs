namespace KestrelCache.Lsm;

/// <summary>
/// Reads a block written by <see cref="BlockBuilder"/>, with binary search over its restart
/// points.
/// </summary>
internal sealed class BlockReader
{
    private readonly byte[] _block;
    private readonly int _restartTableOffset;
    private readonly int _restartCount;

    private BlockReader(byte[] block, int restartTableOffset, int restartCount)
    {
        _block = block;
        _restartTableOffset = restartTableOffset;
        _restartCount = restartCount;
    }

    /// <summary>Entries' total encoded region, excluding the restart table.</summary>
    private int EntriesEnd => _restartTableOffset;

    /// <summary>Parses a block's framing, rejecting one whose trailer does not make sense.</summary>
    internal static BlockReader Open(byte[] block, string? file = null, long offset = 0)
    {
        if (block.Length < sizeof(uint))
        {
            throw new CorruptRecordException("Block is too short to hold a restart count.", offset, file);
        }

        int countOffset = block.Length - sizeof(uint);
        uint restartCount = Varint.ReadFixed32(block.AsSpan(countOffset));

        // A corrupt restart count would otherwise index far outside the block.
        long tableBytes = (long)restartCount * sizeof(uint);
        if (restartCount == 0 || tableBytes + sizeof(uint) > block.Length)
        {
            throw new CorruptRecordException(
                $"Block declares {restartCount} restart point(s), which does not fit in {block.Length} bytes.",
                offset,
                file);
        }

        int restartTableOffset = countOffset - (int)tableBytes;

        return new BlockReader(block, restartTableOffset, (int)restartCount);
    }

    private uint RestartOffset(int index) =>
        Varint.ReadFixed32(_block.AsSpan(_restartTableOffset + (index * sizeof(uint))));

    /// <summary>Creates a cursor over this block.</summary>
    internal Cursor CreateCursor() => new(this);

    /// <summary>
    /// A forward cursor over one block's entries, reconstructing each key from its shared prefix.
    /// </summary>
    internal sealed class Cursor
    {
        private readonly BlockReader _reader;

        // The key is rebuilt in place: a restart entry overwrites it entirely, and every
        // following entry replaces only the bytes after its shared prefix. Growing the buffer
        // rather than allocating per entry is what keeps a full-table scan allocation-free.
        private byte[] _key = new byte[64];
        private int _keyLength;

        private int _valueOffset;
        private int _valueLength;
        private bool _valid;

        internal Cursor(BlockReader reader) => _reader = reader;

        /// <summary>True when positioned on an entry.</summary>
        internal bool IsValid => _valid;

        /// <summary>The current entry's full key.</summary>
        internal ReadOnlySpan<byte> Key => _key.AsSpan(0, _keyLength);

        /// <summary>The current entry's value.</summary>
        internal ReadOnlySpan<byte> Value => _reader._block.AsSpan(_valueOffset, _valueLength);

        /// <summary>Positions on the first entry.</summary>
        internal void SeekToFirst()
        {
            _keyLength = 0;
            _valid = ParseAt(0);
        }

        /// <summary>
        /// Positions on the first entry whose key is greater than or equal to
        /// <paramref name="target"/>.
        /// </summary>
        internal bool Seek(ReadOnlySpan<byte> target, IComparer<byte[]>? comparer = null)
        {
            // Binary search the restart table for the last restart point whose key is strictly
            // below the target. Restart keys are stored in full, so they can be compared without
            // decoding anything before them.
            int low = 0;
            int high = _reader._restartCount - 1;

            while (low < high)
            {
                int mid = (low + high + 1) / 2;
                if (!ParseAt((int)_reader.RestartOffset(mid)))
                {
                    high = mid - 1;
                    continue;
                }

                if (CompareCurrentKeyTo(target) < 0)
                {
                    low = mid;
                }
                else
                {
                    high = mid - 1;
                }
            }

            // Then scan forward from that restart point: at most RestartInterval entries.
            _keyLength = 0;
            if (!ParseAt((int)_reader.RestartOffset(low)))
            {
                _valid = false;
                return false;
            }

            _valid = true;
            while (CompareCurrentKeyTo(target) < 0)
            {
                if (!MoveNext()) return false;
            }

            return _valid;
        }

        /// <summary>Advances to the next entry.</summary>
        internal bool MoveNext()
        {
            if (!_valid) return false;
            _valid = ParseAt(_valueOffset + _valueLength);
            return _valid;
        }

        private int CompareCurrentKeyTo(ReadOnlySpan<byte> target) =>
            InternalKey.IsWellFormed(Key) && InternalKey.IsWellFormed(target)
                ? InternalKey.Comparer.Compare(Key, target)
                : Key.SequenceCompareTo(target);

        private bool ParseAt(int offset)
        {
            if (offset >= _reader.EntriesEnd)
            {
                _valid = false;
                return false;
            }

            var block = _reader._block.AsSpan(0, _reader.EntriesEnd);
            int cursor = offset;

            if (!Varint.TryRead(block, ref cursor, out uint shared)
                || !Varint.TryRead(block, ref cursor, out uint suffix)
                || !Varint.TryRead(block, ref cursor, out uint valueLength))
            {
                throw new CorruptRecordException("Malformed block entry header.", offset);
            }

            if (shared > _keyLength)
            {
                throw new CorruptRecordException(
                    $"Block entry claims a {shared}-byte shared prefix but only {_keyLength} bytes are known.",
                    offset);
            }

            long keyLength = shared + (long)suffix;
            if (cursor + suffix + valueLength > block.Length || keyLength > int.MaxValue)
            {
                throw new CorruptRecordException("Block entry runs past the end of its block.", offset);
            }

            if (keyLength > _key.Length)
            {
                Array.Resize(ref _key, Math.Max((int)keyLength, _key.Length * 2));
            }

            block.Slice(cursor, (int)suffix).CopyTo(_key.AsSpan((int)shared));
            _keyLength = (int)keyLength;
            cursor += (int)suffix;

            _valueOffset = cursor;
            _valueLength = (int)valueLength;
            _valid = true;
            return true;
        }
    }
}
