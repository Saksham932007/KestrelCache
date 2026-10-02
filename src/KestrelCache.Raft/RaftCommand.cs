using System.Buffers.Binary;

namespace KestrelCache.Raft;

/// <summary>
/// Encodes a <see cref="WriteBatch"/> as the opaque byte string Raft replicates.
/// </summary>
/// <remarks>
/// <para>
/// Raft agrees on an ordered sequence of opaque commands and knows nothing about their contents;
/// this is the boundary where a database operation becomes one of those commands. Encoding a
/// whole batch as a single command is what preserves the batch's atomicity through replication:
/// one command is either committed or not, so every follower applies the entire batch or none of
/// it, in the same order as every other follower.
/// </para>
/// <para>
/// A hand-rolled binary format rather than JSON, because this is written to the Raft log on
/// every node for every write. Deterministic encoding also matters: two nodes that encoded the
/// same batch differently would compute different log contents for the same command.
/// </para>
/// <code>
/// uint16 formatVersion
/// int32  operationCount
/// per operation:
///   uint8 kind          (0 = delete, 1 = put)
///   int32 keyLength,   bytes key
///   int32 valueLength, bytes value   (absent for a delete)
/// </code>
/// </remarks>
public static class RaftCommand
{
    private const ushort FormatVersion = 1;
    private const byte KindDelete = 0;
    private const byte KindPut = 1;

    /// <summary>Largest command this codec will decode, as a guard against a corrupt log.</summary>
    private const int MaxOperations = 1_000_000;

    /// <summary>Encodes a batch.</summary>
    public static byte[] Encode(WriteBatch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);

        int size = 2 + 4;
        foreach (var op in batch.Ops)
        {
            size += 1 + 4 + op.Key.Length;
            if (!op.IsDelete) size += 4 + op.Value!.Length;
        }

        var buffer = new byte[size];
        BinaryPrimitives.WriteUInt16LittleEndian(buffer, FormatVersion);
        BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(2), batch.Count);

        int cursor = 6;
        foreach (var op in batch.Ops)
        {
            buffer[cursor++] = op.IsDelete ? KindDelete : KindPut;

            BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(cursor), op.Key.Length);
            cursor += 4;
            op.Key.CopyTo(buffer, cursor);
            cursor += op.Key.Length;

            if (!op.IsDelete)
            {
                BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(cursor), op.Value!.Length);
                cursor += 4;
                op.Value.CopyTo(buffer, cursor);
                cursor += op.Value.Length;
            }
        }

        return buffer;
    }

    /// <summary>Decodes a batch, rejecting anything malformed.</summary>
    public static WriteBatch Decode(ReadOnlySpan<byte> command)
    {
        if (command.Length < 6)
        {
            throw new CorruptRecordException("Raft command is too short to hold a header.", 0);
        }

        ushort version = BinaryPrimitives.ReadUInt16LittleEndian(command);
        if (version != FormatVersion)
        {
            throw new CorruptRecordException(
                $"Raft command format version {version} is not supported by this build "
                    + $"(expected {FormatVersion}).",
                0);
        }

        int count = BinaryPrimitives.ReadInt32LittleEndian(command[2..]);
        if (count < 0 || count > MaxOperations)
        {
            throw new CorruptRecordException($"Implausible operation count {count}.", 0);
        }

        var batch = new WriteBatch();
        int cursor = 6;

        for (int i = 0; i < count; i++)
        {
            if (cursor >= command.Length)
            {
                throw new CorruptRecordException("Raft command ends mid-operation.", cursor);
            }

            byte kind = command[cursor++];
            if (kind is not (KindDelete or KindPut))
            {
                throw new CorruptRecordException($"Unknown operation kind {kind}.", cursor - 1);
            }

            int keyLength = ReadLength(command, ref cursor, "key");
            byte[] key = command.Slice(cursor, keyLength).ToArray();
            cursor += keyLength;

            if (kind == KindDelete)
            {
                batch.Delete(key);
                continue;
            }

            int valueLength = ReadLength(command, ref cursor, "value");
            byte[] value = command.Slice(cursor, valueLength).ToArray();
            cursor += valueLength;

            batch.Put(key, value);
        }

        return batch;
    }

    private static int ReadLength(ReadOnlySpan<byte> command, ref int cursor, string what)
    {
        if (cursor + 4 > command.Length)
        {
            throw new CorruptRecordException($"Raft command ends before its {what} length.", cursor);
        }

        int length = BinaryPrimitives.ReadInt32LittleEndian(command[cursor..]);
        cursor += 4;

        if (length < 0 || cursor + length > command.Length)
        {
            throw new CorruptRecordException(
                $"Raft command declares a {length}-byte {what} that does not fit.", cursor);
        }

        return length;
    }
}
