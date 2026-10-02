using System.Buffers;

namespace KestrelCache.Lsm;

/// <summary>
/// Builds one prefix-compressed data or index block.
/// </summary>
/// <remarks>
/// <para>
/// Keys within a block arrive in sorted order, which means consecutive keys usually share a long
/// prefix — <c>user:0000001234</c> and <c>user:0000001235</c> differ in one byte out of sixteen.
/// Each entry therefore stores only how many bytes it shares with its predecessor plus the
/// bytes that differ. On realistic key shapes this is a large saving: structured keys with a
/// common namespace compress to a few bytes each, and smaller blocks mean fewer disk reads and
/// more of the table resident in the block cache.
/// </para>
/// <para>
/// The catch is that an entry can only be decoded by walking from a known-complete key, which
/// would make a block a linked list and destroy random access. Restart points solve it: every
/// <c>RestartInterval</c> entries, one key is stored in full and its offset recorded in a table
/// at the end of the block. A seek then binary-searches that table and scans forward at most
/// <c>RestartInterval</c> entries, so lookup stays logarithmic in block count and bounded within
/// a block.
/// </para>
/// <para>
/// The interval is the usual trade: 16 keeps the scan short while amortising the cost of a full
/// key over sixteen entries. A smaller value buys faster seeks at the cost of compression; a
/// larger one the reverse.
/// </para>
/// <para><b>Layout</b></para>
/// <code>
/// entries:
///   varint sharedPrefixLength     (0 at a restart point)
///   varint suffixLength
///   varint valueLength
///   bytes  keySuffix[suffixLength]
///   bytes  value[valueLength]
/// trailer:
///   uint32 restartOffset[restartCount]
///   uint32 restartCount
/// </code>
/// </remarks>
internal sealed class BlockBuilder
{
    /// <summary>Entries between stored-in-full keys.</summary>
    internal const int RestartInterval = 16;

    private readonly ArrayBufferWriter<byte> _buffer = new(4096);
    private readonly List<uint> _restarts = [0];

    private byte[] _lastKey = [];
    private int _entriesSinceRestart;
    private bool _finished;

    /// <summary>Entries added so far.</summary>
    internal int Count { get; private set; }

    /// <summary>True when nothing has been added.</summary>
    internal bool IsEmpty => Count == 0;

    /// <summary>The last key added, which a table writer needs for its index entry.</summary>
    internal ReadOnlySpan<byte> LastKey => _lastKey;

    /// <summary>
    /// Approximate finished size, used to decide when a block is full. Includes the trailer so
    /// the estimate does not drift as restart points accumulate.
    /// </summary>
    internal int EstimatedSize =>
        _buffer.WrittenCount + (_restarts.Count * sizeof(uint)) + sizeof(uint);

    /// <summary>
    /// Appends an entry. Keys must arrive in ascending order.
    /// </summary>
    internal void Add(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        if (_finished) throw new InvalidOperationException("Block already finished.");

        int shared = 0;

        if (_entriesSinceRestart < RestartInterval)
        {
            // Measure the shared prefix against the previous key.
            int limit = Math.Min(_lastKey.Length, key.Length);
            while (shared < limit && _lastKey[shared] == key[shared])
            {
                shared++;
            }
        }
        else
        {
            // Start a new restart point: store this key in full and record where it begins.
            _restarts.Add((uint)_buffer.WrittenCount);
            _entriesSinceRestart = 0;
        }

        int suffix = key.Length - shared;

        var span = _buffer.GetSpan(
            (3 * Varint.MaxSize32) + suffix + value.Length);
        int written = 0;

        written += Varint.Write(span[written..], (uint)shared);
        written += Varint.Write(span[written..], (uint)suffix);
        written += Varint.Write(span[written..], (uint)value.Length);

        key[shared..].CopyTo(span[written..]);
        written += suffix;

        value.CopyTo(span[written..]);
        written += value.Length;

        _buffer.Advance(written);

        _lastKey = key.ToArray();
        _entriesSinceRestart++;
        Count++;
    }

    /// <summary>Appends the restart table and returns the finished block.</summary>
    internal ReadOnlyMemory<byte> Finish()
    {
        if (_finished) throw new InvalidOperationException("Block already finished.");

        var trailer = _buffer.GetSpan((_restarts.Count + 1) * sizeof(uint));
        int offset = 0;

        foreach (uint restart in _restarts)
        {
            Varint.WriteFixed32(trailer[offset..], restart);
            offset += sizeof(uint);
        }

        Varint.WriteFixed32(trailer[offset..], (uint)_restarts.Count);
        offset += sizeof(uint);

        _buffer.Advance(offset);
        _finished = true;

        return _buffer.WrittenMemory;
    }

    /// <summary>Resets the builder so the same instance can build the next block.</summary>
    internal void Reset()
    {
        _buffer.Clear();
        _restarts.Clear();
        _restarts.Add(0);
        _lastKey = [];
        _entriesSinceRestart = 0;
        Count = 0;
        _finished = false;
    }
}
