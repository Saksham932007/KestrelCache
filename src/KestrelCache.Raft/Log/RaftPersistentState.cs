using System.Buffers.Binary;
using System.IO.Hashing;
using System.Text;

namespace KestrelCache.Raft;

/// <summary>
/// The two pieces of state Raft requires to survive a restart: the current term, and who this
/// node voted for in it.
/// </summary>
/// <remarks>
/// <para>
/// These are tiny and they are load-bearing. A node that forgets its term can be talked into
/// accepting an old leader; a node that forgets its vote can vote twice in the same term, which
/// allows two leaders to be elected simultaneously and lets them accept conflicting writes. So
/// both are written and fsynced <i>before</i> the node acts on them — before the vote reply is
/// sent, before the candidacy is announced.
/// </para>
/// <para>
/// The file is rewritten whole each time rather than appended to. It is under fifty bytes, so
/// an incremental format would save nothing and add a replay path; and the write-to-temp,
/// fsync, rename sequence gives atomicity for free, so a crash mid-update leaves the previous
/// state rather than a half-written one.
/// </para>
/// </remarks>
public sealed class RaftPersistentState
{
    private const int MaxVotedForLength = 256;

    private readonly string _path;
    private readonly object _gate = new();

    private long _currentTerm;
    private string? _votedFor;

    private RaftPersistentState(string path, long currentTerm, string? votedFor)
    {
        _path = path;
        _currentTerm = currentTerm;
        _votedFor = votedFor;
    }

    /// <summary>The term this node believes it is in.</summary>
    public long CurrentTerm
    {
        get { lock (_gate) return _currentTerm; }
    }

    /// <summary>Who this node voted for in <see cref="CurrentTerm"/>, or null if it has not voted.</summary>
    public string? VotedFor
    {
        get { lock (_gate) return _votedFor; }
    }

    /// <summary>True when the state file was missing or unreadable and defaults were used.</summary>
    public bool StartedFresh { get; private init; }

    /// <summary>Loads persisted state, or starts at term 0 with no vote.</summary>
    public static RaftPersistentState Open(string path)
    {
        string? directory = Path.GetDirectoryName(Path.GetFullPath(path));
        if (!string.IsNullOrEmpty(directory)) Directory.CreateDirectory(directory);

        if (!File.Exists(path))
        {
            return new RaftPersistentState(path, 0, null) { StartedFresh = true };
        }

        byte[] bytes;
        try
        {
            bytes = File.ReadAllBytes(path);
        }
        catch (IOException)
        {
            return new RaftPersistentState(path, 0, null) { StartedFresh = true };
        }

        if (!TryDecode(bytes, out long term, out string? votedFor))
        {
            // A corrupt state file cannot be repaired, and guessing would be unsafe. Starting at
            // term 0 is the conservative choice: the node will learn the real term from the
            // first message it receives, and until then it cannot win an election against peers
            // whose terms are higher.
            return new RaftPersistentState(path, 0, null) { StartedFresh = true };
        }

        return new RaftPersistentState(path, term, votedFor);
    }

    /// <summary>
    /// Records a new term and vote, returning only once both are on stable storage.
    /// </summary>
    public void Save(long currentTerm, string? votedFor)
    {
        lock (_gate)
        {
            _currentTerm = currentTerm;
            _votedFor = votedFor;
            Write(_path, currentTerm, votedFor);
        }
    }

    /// <summary>Advances to a later term, clearing the vote, and persists it.</summary>
    public void AdvanceTerm(long newTerm)
    {
        lock (_gate)
        {
            if (newTerm <= _currentTerm) return;
            _currentTerm = newTerm;
            _votedFor = null;
            Write(_path, _currentTerm, _votedFor);
        }
    }

    /// <summary>Records a vote in the current term and persists it.</summary>
    public void RecordVote(string candidateId)
    {
        lock (_gate)
        {
            _votedFor = candidateId;
            Write(_path, _currentTerm, _votedFor);
        }
    }

    private static void Write(string path, long term, string? votedFor)
    {
        byte[] encoded = Encode(term, votedFor);
        string temporary = path + ".tmp";

        using (var stream = new FileStream(
            temporary,
            new FileStreamOptions
            {
                Mode = FileMode.Create,
                Access = FileAccess.Write,
                Share = FileShare.None,
                BufferSize = 0,
            }))
        {
            stream.Write(encoded);
            stream.Flush(flushToDisk: true);
        }

        File.Move(temporary, path, overwrite: true);
    }

    private static byte[] Encode(long term, string? votedFor)
    {
        byte[] voted = votedFor is null ? [] : Encoding.UTF8.GetBytes(votedFor);
        var buffer = new byte[4 + 8 + 4 + voted.Length];

        BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(4), term);
        BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(12), voted.Length);
        voted.CopyTo(buffer, 16);

        uint crc = Crc32.HashToUInt32(buffer.AsSpan(4));
        BinaryPrimitives.WriteUInt32LittleEndian(buffer, crc);

        return buffer;
    }

    private static bool TryDecode(ReadOnlySpan<byte> bytes, out long term, out string? votedFor)
    {
        term = 0;
        votedFor = null;

        if (bytes.Length < 16) return false;
        if (Crc32.HashToUInt32(bytes[4..]) != BinaryPrimitives.ReadUInt32LittleEndian(bytes)) return false;

        term = BinaryPrimitives.ReadInt64LittleEndian(bytes[4..]);
        int length = BinaryPrimitives.ReadInt32LittleEndian(bytes[12..]);

        if (length < 0 || length > MaxVotedForLength || 16 + length > bytes.Length) return false;

        votedFor = length == 0 ? null : Encoding.UTF8.GetString(bytes.Slice(16, length));
        return true;
    }
}
