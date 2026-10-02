using System.Buffers.Binary;
using System.Text;

namespace KestrelCache.Raft;

/// <summary>
/// Encodes a <see cref="RaftConfiguration"/> for storage in a log entry and in a snapshot
/// header.
/// </summary>
/// <remarks>
/// A deterministic binary format, for the same reason commands use one: the bytes go into the
/// replicated log, so two nodes encoding the same configuration must produce identical entries
/// or their logs will differ at an index they are supposed to agree on.
/// </remarks>
public static class RaftConfigurationCodec
{
    /// <summary>
    /// Current format version. Version 1 had no learner list; it is still read, with an empty
    /// one, so a log or snapshot written before learners existed still opens.
    /// </summary>
    private const ushort FormatVersion = 2;

    private const ushort FormatVersionWithoutLearners = 1;
    private const int MaxVoters = 1024;
    private const int MaxIdLength = 256;

    /// <summary>Encodes a configuration.</summary>
    public static byte[] Encode(RaftConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        using var stream = new MemoryStream(128);
        using var writer = new BinaryWriter(stream, Encoding.UTF8);

        writer.Write(FormatVersion);
        WriteList(writer, configuration.Voters);
        writer.Write(configuration.OutgoingVoters is not null);
        if (configuration.OutgoingVoters is not null)
        {
            WriteList(writer, configuration.OutgoingVoters);
        }
        WriteList(writer, configuration.Learners);

        writer.Flush();
        return stream.ToArray();
    }

    /// <summary>Decodes a configuration, rejecting anything malformed.</summary>
    public static RaftConfiguration Decode(ReadOnlySpan<byte> encoded)
    {
        if (encoded.Length < 2)
        {
            throw new CorruptRecordException("Configuration is too short to hold a version.", 0);
        }

        ushort version = BinaryPrimitives.ReadUInt16LittleEndian(encoded);
        if (version is not (FormatVersion or FormatVersionWithoutLearners))
        {
            throw new CorruptRecordException(
                $"Configuration format version {version} is not supported by this build "
                    + $"(expected {FormatVersionWithoutLearners} or {FormatVersion}).",
                0);
        }

        using var stream = new MemoryStream(encoded.ToArray(), writable: false);
        using var reader = new BinaryReader(stream, Encoding.UTF8);

        _ = reader.ReadUInt16();
        var voters = ReadList(reader);
        bool hasOutgoing = reader.ReadBoolean();
        var outgoing = hasOutgoing ? ReadList(reader) : null;

        // Absent in version 1, which predates learners.
        var learners = version >= FormatVersion ? ReadList(reader) : [];

        if (voters.Count == 0)
        {
            throw new CorruptRecordException("Configuration contains no voters.", 0);
        }

        return new RaftConfiguration
        {
            Voters = voters,
            OutgoingVoters = outgoing,
            Learners = learners,
        };
    }

    private static void WriteList(BinaryWriter writer, IReadOnlyList<string> values)
    {
        writer.Write(values.Count);
        foreach (string value in values)
        {
            writer.Write(value);
        }
    }

    private static List<string> ReadList(BinaryReader reader)
    {
        int count = reader.ReadInt32();
        if (count < 0 || count > MaxVoters)
        {
            throw new CorruptRecordException($"Implausible voter count {count}.", 0);
        }

        var values = new List<string>(Math.Min(count, 16));
        for (int i = 0; i < count; i++)
        {
            string value = reader.ReadString();
            if (value.Length is 0 or > MaxIdLength)
            {
                throw new CorruptRecordException(
                    $"Implausible node id of {value.Length} character(s).", 0);
            }
            values.Add(value);
        }

        return values;
    }
}
