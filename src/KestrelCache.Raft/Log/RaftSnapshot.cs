using System.Buffers.Binary;
using System.IO.Hashing;

namespace KestrelCache.Raft;

/// <summary>What a snapshot replaces, and the configuration in force when it was taken.</summary>
/// <param name="LastIncludedIndex">The snapshot stands in for every entry up to and including this index.</param>
/// <param name="LastIncludedTerm">Term of the entry at <paramref name="LastIncludedIndex"/>.</param>
/// <param name="Configuration">
/// The cluster configuration as of that index.
/// </param>
/// <remarks>
/// The configuration has to travel with the snapshot. Membership is established by configuration
/// entries in the log, and a snapshot exists precisely to let those entries be discarded — so a
/// node that restored from a snapshot without it would come back up not knowing who its peers
/// are, and a follower receiving one would have to guess.
/// </remarks>
public readonly record struct RaftSnapshotMetadata(
    long LastIncludedIndex,
    long LastIncludedTerm,
    RaftConfiguration Configuration);

/// <summary>
/// Reads and writes the state-machine snapshot that lets the Raft log be truncated.
/// </summary>
/// <remarks>
/// <para>
/// Installed by write-to-temporary, fsync, rename, for the same reason the LSM engine's manifest
/// is: the rename is atomic, so a crash leaves either the complete previous snapshot or the
/// complete new one, never a half-written file that would be mistaken for valid state.
/// </para>
/// <para>
/// The ordering around it is what makes truncation safe. The snapshot must be durable
/// <i>before</i> the log entries it replaces are discarded. Reversing those two steps means a
/// crash in between loses the history and the state that was supposed to replace it, which is
/// unrecoverable — and it is the kind of mistake that only shows up as data loss under a crash,
/// never in ordinary testing.
/// </para>
/// <para><b>File layout</b></para>
/// <code>
/// magic              4   "KCSN"
/// formatVersion      2   currently 1
/// reserved           2   zero
/// lastIncludedIndex  8
/// lastIncludedTerm   8
/// configLength       4
/// payloadLength      8
/// headerCrc32        4   over bytes [0, 36)
/// config             n   encoded RaftConfiguration
/// payloadCrc32       4   over the state-machine payload
/// payload            m   opaque state-machine image
/// </code>
/// <para>
/// Two checksums rather than one, because they answer different questions. The header checksum
/// makes the length fields trustworthy before anything is allocated from them. The payload
/// checksum detects a corrupt image — and is verified after reading, so a damaged snapshot is
/// refused rather than restored into the state machine.
/// </para>
/// </remarks>
public sealed class RaftSnapshotStore
{
    private const int HeaderSize = 40;
    private const int ChecksummedHeaderSize = 36;
    private const ushort FormatVersion = 1;
    private const int MaxConfigLength = 1024 * 1024;
    private const long MaxPayloadLength = 64L * 1024 * 1024 * 1024;

    private static ReadOnlySpan<byte> Magic => "KCSN"u8;

    private readonly string _path;

    /// <summary>Creates a store over <c>snapshot.bin</c> in <paramref name="directory"/>.</summary>
    public RaftSnapshotStore(string directory)
    {
        Directory.CreateDirectory(directory);

        // Fully qualified because this type exposes its own Path property, which would otherwise
        // shadow the static class.
        _path = System.IO.Path.Combine(directory, "snapshot.bin");
    }

    /// <summary>Path of the snapshot file.</summary>
    public string Path => _path;

    /// <summary>True when a snapshot exists.</summary>
    public bool Exists => File.Exists(_path);

    /// <summary>Size of the snapshot file, or zero.</summary>
    public long SizeBytes => Exists ? new FileInfo(_path).Length : 0;

    /// <summary>
    /// Reads the metadata without reading the payload, which is what recovery and replication
    /// decisions need.
    /// </summary>
    public RaftSnapshotMetadata? TryReadMetadata()
    {
        if (!Exists) return null;

        using var stream = File.OpenRead(_path);
        if (stream.Length < HeaderSize) return null;

        Span<byte> header = stackalloc byte[HeaderSize];
        stream.ReadExactly(header);

        var parsed = ParseHeader(header, _path);

        var config = new byte[parsed.ConfigLength];
        stream.ReadExactly(config);

        return new RaftSnapshotMetadata(
            parsed.LastIncludedIndex,
            parsed.LastIncludedTerm,
            RaftConfigurationCodec.Decode(config));
    }

    /// <summary>
    /// Captures the state machine into a new snapshot and installs it atomically.
    /// </summary>
    public async ValueTask<RaftSnapshotMetadata> WriteAsync(
        RaftSnapshotMetadata metadata,
        IRaftStateMachine stateMachine,
        CancellationToken cancellationToken = default)
    {
        string temporary = _path + ".tmp";
        File.Delete(temporary);

        byte[] config = RaftConfigurationCodec.Encode(metadata.Configuration);

        // The payload is captured to a side file first, because its length and checksum are
        // needed in the header and the state machine streams it out without knowing either.
        string payloadPath = _path + ".payload";
        File.Delete(payloadPath);

        try
        {
            long payloadLength;
            uint payloadCrc;

            await using (var payload = new FileStream(
                payloadPath,
                new FileStreamOptions
                {
                    Mode = FileMode.Create,
                    Access = FileAccess.ReadWrite,
                    Share = FileShare.None,
                    BufferSize = 64 * 1024,
                }))
            {
                await stateMachine.CaptureSnapshotAsync(payload, cancellationToken)
                    .ConfigureAwait(false);
                await payload.FlushAsync(cancellationToken).ConfigureAwait(false);

                payloadLength = payload.Length;
                payload.Position = 0;

                var crc = new Crc32();
                byte[] buffer = new byte[64 * 1024];
                int read;
                while ((read = await payload.ReadAsync(buffer, cancellationToken)
                    .ConfigureAwait(false)) > 0)
                {
                    crc.Append(buffer.AsSpan(0, read));
                }
                payloadCrc = crc.GetCurrentHashAsUInt32();
            }

            await using (var output = new FileStream(
                temporary,
                new FileStreamOptions
                {
                    Mode = FileMode.Create,
                    Access = FileAccess.Write,
                    Share = FileShare.None,
                    BufferSize = 64 * 1024,
                }))
            {
                byte[] header = new byte[HeaderSize];
                Magic.CopyTo(header);
                BinaryPrimitives.WriteUInt16LittleEndian(header.AsSpan(4), FormatVersion);
                BinaryPrimitives.WriteInt64LittleEndian(header.AsSpan(8), metadata.LastIncludedIndex);
                BinaryPrimitives.WriteInt64LittleEndian(header.AsSpan(16), metadata.LastIncludedTerm);
                BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(24), config.Length);
                BinaryPrimitives.WriteInt64LittleEndian(header.AsSpan(28), payloadLength);
                BinaryPrimitives.WriteUInt32LittleEndian(
                    header.AsSpan(ChecksummedHeaderSize),
                    Crc32.HashToUInt32(header.AsSpan(0, ChecksummedHeaderSize)));

                await output.WriteAsync(header, cancellationToken).ConfigureAwait(false);
                await output.WriteAsync(config, cancellationToken).ConfigureAwait(false);

                byte[] payloadCrcBytes = new byte[4];
                BinaryPrimitives.WriteUInt32LittleEndian(payloadCrcBytes, payloadCrc);
                await output.WriteAsync(payloadCrcBytes, cancellationToken).ConfigureAwait(false);

                await using (var payload = File.OpenRead(payloadPath))
                {
                    await payload.CopyToAsync(output, 64 * 1024, cancellationToken)
                        .ConfigureAwait(false);
                }

                // Durable before the rename, or the rename could survive a crash while the
                // contents did not -- leaving a snapshot that names nothing.
                output.Flush(flushToDisk: true);
            }

            File.Move(temporary, _path, overwrite: true);
            return metadata;
        }
        finally
        {
            TryDelete(temporary);
            TryDelete(payloadPath);
        }
    }

    /// <summary>
    /// Installs a snapshot received from a leader, already assembled as a complete file.
    /// </summary>
    public void InstallFromFile(string assembledPath)
    {
        // Validated before installation: a snapshot that fails its checksum must never replace
        // good state, and the check is cheap next to a full restore.
        using (var stream = File.OpenRead(assembledPath))
        {
            Span<byte> header = stackalloc byte[HeaderSize];
            stream.ReadExactly(header);
            _ = ParseHeader(header, assembledPath);
        }

        File.Move(assembledPath, _path, overwrite: true);
    }

    /// <summary>
    /// Opens the payload for restoring, after verifying its checksum, and returns it with the
    /// metadata.
    /// </summary>
    public (RaftSnapshotMetadata Metadata, Stream Payload) OpenForRestore()
    {
        var stream = File.OpenRead(_path);

        try
        {
            Span<byte> header = stackalloc byte[HeaderSize];
            stream.ReadExactly(header);
            var parsed = ParseHeader(header, _path);

            var config = new byte[parsed.ConfigLength];
            stream.ReadExactly(config);

            Span<byte> payloadCrcBytes = stackalloc byte[4];
            stream.ReadExactly(payloadCrcBytes);
            uint expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(payloadCrcBytes);

            long payloadStart = stream.Position;

            var crc = new Crc32();
            byte[] buffer = new byte[64 * 1024];
            long remaining = parsed.PayloadLength;
            while (remaining > 0)
            {
                int read = stream.Read(buffer, 0, (int)Math.Min(buffer.Length, remaining));
                if (read == 0)
                {
                    throw new CorruptRecordException(
                        $"Snapshot payload is shorter than its declared {parsed.PayloadLength} bytes.",
                        payloadStart,
                        _path);
                }
                crc.Append(buffer.AsSpan(0, read));
                remaining -= read;
            }

            if (crc.GetCurrentHashAsUInt32() != expectedCrc)
            {
                throw new CorruptRecordException(
                    "Snapshot payload checksum mismatch; refusing to restore it.",
                    payloadStart,
                    _path);
            }

            stream.Position = payloadStart;

            var metadata = new RaftSnapshotMetadata(
                parsed.LastIncludedIndex,
                parsed.LastIncludedTerm,
                RaftConfigurationCodec.Decode(config));

            return (metadata, stream);
        }
        catch
        {
            stream.Dispose();
            throw;
        }
    }

    /// <summary>Reads the whole snapshot file, for sending to a follower.</summary>
    public Stream OpenForSending() => File.OpenRead(_path);

    private readonly record struct ParsedHeader(
        long LastIncludedIndex,
        long LastIncludedTerm,
        int ConfigLength,
        long PayloadLength);

    private static ParsedHeader ParseHeader(ReadOnlySpan<byte> header, string path)
    {
        if (!header[..4].SequenceEqual(Magic))
        {
            throw new CorruptRecordException("Bad magic; not a KestrelCache snapshot.", 0, path);
        }

        ushort version = BinaryPrimitives.ReadUInt16LittleEndian(header[4..]);
        if (version != FormatVersion)
        {
            throw new CorruptRecordException(
                $"Snapshot format version {version} is not supported by this build "
                    + $"(expected {FormatVersion}).",
                0,
                path);
        }

        uint storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(header[ChecksummedHeaderSize..]);
        if (Crc32.HashToUInt32(header[..ChecksummedHeaderSize]) != storedCrc)
        {
            throw new CorruptRecordException("Snapshot header checksum mismatch.", 0, path);
        }

        long lastIncludedIndex = BinaryPrimitives.ReadInt64LittleEndian(header[8..]);
        long lastIncludedTerm = BinaryPrimitives.ReadInt64LittleEndian(header[16..]);
        int configLength = BinaryPrimitives.ReadInt32LittleEndian(header[24..]);
        long payloadLength = BinaryPrimitives.ReadInt64LittleEndian(header[28..]);

        if (lastIncludedIndex < 0
            || lastIncludedTerm < 0
            || configLength is < 0 or > MaxConfigLength
            || payloadLength is < 0 or > MaxPayloadLength)
        {
            throw new CorruptRecordException(
                "Snapshot header holds implausible lengths or positions.", 0, path);
        }

        return new ParsedHeader(lastIncludedIndex, lastIncludedTerm, configLength, payloadLength);
    }

    private static void TryDelete(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch (IOException)
        {
            // A stale temporary is harmless; it is overwritten next time.
        }
    }
}
