using System.Buffers.Binary;
using System.IO.Hashing;
using System.Text;

namespace KestrelCache.Raft;

/// <summary>Which message a frame carries.</summary>
internal enum RaftMessageKind : byte
{
    RequestVoteRequest = 1,
    RequestVoteResponse = 2,
    AppendEntriesRequest = 3,
    AppendEntriesResponse = 4,
}

/// <summary>
/// The on-wire encoding for Raft RPCs between nodes.
/// </summary>
/// <remarks>
/// <para>
/// Length-prefixed and checksummed, for the same reason every other framed format in this
/// project is: TCP is a byte stream with no concept of a message, so a reader that does not know
/// how long a message is cannot know when it has one. The length prefix provides that, and the
/// checksum catches a frame that was truncated or mangled in a way TCP's own checksum missed.
/// </para>
/// <para>
/// A compact binary encoding rather than JSON, because AppendEntries carries the actual log
/// entries and is the hot path of the whole protocol — a leader sends one to every follower
/// several times a second forever. Base64-encoding every value into JSON would inflate the
/// replication traffic by a third for no benefit, since nothing human ever reads these frames.
/// </para>
/// <code>
/// uint32 frameLength   (everything after this field)
/// uint32 crc32         (over everything after this field)
/// uint8  kind
/// ...    body, per kind
/// </code>
/// </remarks>
internal static class RaftWire
{
    /// <summary>Bytes of frame header: length and checksum.</summary>
    internal const int FrameHeaderSize = 8;

    /// <summary>Largest frame accepted, as a guard against a corrupt or hostile length.</summary>
    internal const int MaxFrameSize = 64 * 1024 * 1024;

    // ---------------------------------------------------------------- framing

    internal static byte[] Frame(RaftMessageKind kind, Action<BinaryWriter> writeBody)
    {
        using var body = new MemoryStream(256);
        using (var writer = new BinaryWriter(body, Encoding.UTF8, leaveOpen: true))
        {
            writer.Write((byte)kind);
            writeBody(writer);
        }

        byte[] payload = body.ToArray();
        var frame = new byte[FrameHeaderSize + payload.Length];

        BinaryPrimitives.WriteUInt32LittleEndian(frame, (uint)(payload.Length + 4));
        payload.CopyTo(frame, FrameHeaderSize);

        uint crc = Crc32.HashToUInt32(frame.AsSpan(FrameHeaderSize));
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(4), crc);

        return frame;
    }

    internal static (RaftMessageKind Kind, BinaryReader Reader) Unframe(byte[] payload, string? peer)
    {
        if (payload.Length < 1)
        {
            throw new CorruptRecordException("Raft frame carries no message kind.", 0, peer);
        }

        var stream = new MemoryStream(payload, writable: false);
        var reader = new BinaryReader(stream, Encoding.UTF8);
        var kind = (RaftMessageKind)reader.ReadByte();

        if (!Enum.IsDefined(kind))
        {
            throw new CorruptRecordException($"Unknown Raft message kind {(byte)kind}.", 0, peer);
        }

        return (kind, reader);
    }

    internal static void ValidateChecksum(ReadOnlySpan<byte> checksummed, uint expected, string? peer)
    {
        if (Crc32.HashToUInt32(checksummed) != expected)
        {
            throw new CorruptRecordException("Raft frame checksum mismatch.", 0, peer);
        }
    }

    // ---------------------------------------------------------------- bodies

    internal static byte[] Encode(RequestVoteRequest request) =>
        Frame(RaftMessageKind.RequestVoteRequest, writer =>
        {
            writer.Write(request.Term);
            writer.Write(request.CandidateId);
            writer.Write(request.LastLogIndex);
            writer.Write(request.LastLogTerm);
        });

    internal static RequestVoteRequest DecodeRequestVoteRequest(BinaryReader reader) =>
        new(reader.ReadInt64(), reader.ReadString(), reader.ReadInt64(), reader.ReadInt64());

    internal static byte[] Encode(RequestVoteResponse response) =>
        Frame(RaftMessageKind.RequestVoteResponse, writer =>
        {
            writer.Write(response.Term);
            writer.Write(response.VoteGranted);
            writer.Write(response.VoterId);
        });

    internal static RequestVoteResponse DecodeRequestVoteResponse(BinaryReader reader) =>
        new(reader.ReadInt64(), reader.ReadBoolean(), reader.ReadString());

    internal static byte[] Encode(AppendEntriesRequest request)
    {
        var entries = request.Entries;
        long term = request.Term;
        string leaderId = request.LeaderId;
        long previousIndex = request.PreviousLogIndex;
        long previousTerm = request.PreviousLogTerm;
        long leaderCommit = request.LeaderCommit;

        return Frame(RaftMessageKind.AppendEntriesRequest, writer =>
        {
            writer.Write(term);
            writer.Write(leaderId);
            writer.Write(previousIndex);
            writer.Write(previousTerm);
            writer.Write(leaderCommit);
            writer.Write(entries.Count);

            foreach (var entry in entries)
            {
                writer.Write(entry.Index);
                writer.Write(entry.Term);
                writer.Write(entry.IsNoOp);
                writer.Write(entry.Command.Length);
                writer.Write(entry.Command);
            }
        });
    }

    internal static AppendEntriesRequest DecodeAppendEntriesRequest(BinaryReader reader, string? peer)
    {
        long term = reader.ReadInt64();
        string leaderId = reader.ReadString();
        long previousIndex = reader.ReadInt64();
        long previousTerm = reader.ReadInt64();
        long leaderCommit = reader.ReadInt64();
        int count = reader.ReadInt32();

        if (count < 0 || count > 1_000_000)
        {
            throw new CorruptRecordException($"Implausible entry count {count}.", 0, peer);
        }

        var entries = new List<RaftLogEntry>(Math.Min(count, 1024));
        for (int i = 0; i < count; i++)
        {
            long index = reader.ReadInt64();
            long entryTerm = reader.ReadInt64();
            bool isNoOp = reader.ReadBoolean();
            int length = reader.ReadInt32();

            if (length < 0 || length > MaxFrameSize)
            {
                throw new CorruptRecordException($"Implausible command length {length}.", 0, peer);
            }

            entries.Add(new RaftLogEntry(index, entryTerm, reader.ReadBytes(length), isNoOp));
        }

        return new AppendEntriesRequest(
            term, leaderId, previousIndex, previousTerm, entries, leaderCommit);
    }

    internal static byte[] Encode(AppendEntriesResponse response) =>
        Frame(RaftMessageKind.AppendEntriesResponse, writer =>
        {
            writer.Write(response.Term);
            writer.Write(response.Success);
            writer.Write(response.FollowerId);
            writer.Write(response.MatchIndex);
            writer.Write(response.ConflictTerm);
            writer.Write(response.ConflictIndex);
        });

    internal static AppendEntriesResponse DecodeAppendEntriesResponse(BinaryReader reader) =>
        new(
            reader.ReadInt64(),
            reader.ReadBoolean(),
            reader.ReadString(),
            reader.ReadInt64(),
            reader.ReadInt64(),
            reader.ReadInt64());
}
