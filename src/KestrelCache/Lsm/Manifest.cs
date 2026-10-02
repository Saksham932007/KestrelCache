using System.Text.Json;
using System.Text.Json.Serialization;

namespace KestrelCache.Lsm;

/// <summary>The durable catalogue of which tables exist, and where recovery should resume.</summary>
internal sealed record ManifestState
{
    /// <summary>On-disk manifest format version.</summary>
    [JsonPropertyName("formatVersion")]
    public int FormatVersion { get; init; } = Manifest.FormatVersion;

    /// <summary>Next unused file number. File numbers are never reused.</summary>
    [JsonPropertyName("nextFileNumber")]
    public ulong NextFileNumber { get; init; } = 1;

    /// <summary>Highest sequence number assigned to any write.</summary>
    [JsonPropertyName("lastSequence")]
    public ulong LastSequence { get; init; }

    /// <summary>
    /// The oldest write-ahead log still needed. Logs numbered below this have been flushed into
    /// SSTables and may be deleted.
    /// </summary>
    [JsonPropertyName("logNumber")]
    public ulong LogNumber { get; init; }

    /// <summary>Tables per level, index 0 being level 0.</summary>
    [JsonPropertyName("levels")]
    public IReadOnlyList<IReadOnlyList<TableRecord>> Levels { get; init; } = [];

    /// <summary>One table's metadata as stored in the manifest.</summary>
    internal sealed record TableRecord
    {
        [JsonPropertyName("fileNumber")]
        public required ulong FileNumber { get; init; }

        [JsonPropertyName("fileSize")]
        public required long FileSizeBytes { get; init; }

        [JsonPropertyName("smallestKey")]
        public required byte[] SmallestKey { get; init; }

        [JsonPropertyName("largestKey")]
        public required byte[] LargestKey { get; init; }

        [JsonPropertyName("smallestSequence")]
        public ulong SmallestSequence { get; init; }

        [JsonPropertyName("largestSequence")]
        public ulong LargestSequence { get; init; }

        [JsonPropertyName("entryCount")]
        public long EntryCount { get; init; }

        internal SsTableMeta ToMeta() => new()
        {
            FileNumber = FileNumber,
            FileSizeBytes = FileSizeBytes,
            SmallestKey = SmallestKey,
            LargestKey = LargestKey,
            SmallestSequence = SmallestSequence,
            LargestSequence = LargestSequence,
            EntryCount = EntryCount,
        };

        internal static TableRecord From(SsTableMeta meta) => new()
        {
            FileNumber = meta.FileNumber,
            FileSizeBytes = meta.FileSizeBytes,
            SmallestKey = meta.SmallestKey,
            LargestKey = meta.LargestKey,
            SmallestSequence = meta.SmallestSequence,
            LargestSequence = meta.LargestSequence,
            EntryCount = meta.EntryCount,
        };
    }
}

/// <summary>
/// Reads and writes the manifest, the one file that defines what the database <i>is</i>.
/// </summary>
/// <remarks>
/// <para>
/// SSTables on disk are meaningless on their own; the manifest is what says which of them are
/// live, at which level, and which log files are still needed. So installing a new manifest is
/// the moment a flush or a compaction becomes real, and that moment has to be atomic — a
/// half-written manifest would be a database with no definition.
/// </para>
/// <para>
/// Atomicity comes from write-to-temp, fsync, rename. <c>rename(2)</c> is atomic within a
/// filesystem, so a crash leaves either the complete old manifest or the complete new one. The
/// ordering around it is what makes the whole scheme crash-safe:
/// </para>
/// <list type="number">
/// <item>Write the new SSTable and fsync it, so its contents are durable.</item>
/// <item>Install the manifest naming it, atomically.</item>
/// <item>Only then delete the write-ahead log the data came from.</item>
/// </list>
/// <para>
/// A crash between any two steps is survivable. After step 1 the table exists but is
/// unreferenced, and is garbage-collected as an orphan. After step 2 the data is reachable from
/// the manifest and the log is merely redundant. The one ordering that would lose data —
/// deleting the log before the manifest is durable — is the one the sequence forbids.
/// </para>
/// <para>
/// A real engine logs incremental edits here rather than rewriting the whole file, because at a
/// few hundred thousand tables the rewrite itself becomes expensive. At this scale a full
/// rewrite costs microseconds and happens only on flush or compaction, never per write, so the
/// simpler scheme is the better trade.
/// </para>
/// </remarks>
internal static class Manifest
{
    /// <summary>Current manifest format version.</summary>
    internal const int FormatVersion = 1;

    /// <summary>Name of the manifest file within the database directory.</summary>
    internal const string FileName = "MANIFEST";

    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
    };

    /// <summary>Full path of the manifest in <paramref name="directory"/>.</summary>
    internal static string PathIn(string directory) => Path.Combine(directory, FileName);

    /// <summary>
    /// Loads the manifest, or returns a fresh empty state when the directory holds no database
    /// yet.
    /// </summary>
    internal static ManifestState Load(string directory)
    {
        string path = PathIn(directory);
        if (!File.Exists(path))
        {
            return new ManifestState();
        }

        string json = File.ReadAllText(path);

        ManifestState? state;
        try
        {
            state = JsonSerializer.Deserialize<ManifestState>(json, SerializerOptions);
        }
        catch (JsonException exception)
        {
            throw new CorruptRecordException(
                $"Manifest is not valid JSON: {exception.Message}", 0, path);
        }

        if (state is null)
        {
            throw new CorruptRecordException("Manifest deserialised to null.", 0, path);
        }

        if (state.FormatVersion != FormatVersion)
        {
            throw new CorruptRecordException(
                $"Manifest format version {state.FormatVersion} is not supported by this build "
                    + $"(expected {FormatVersion}).",
                0,
                path);
        }

        return state;
    }

    /// <summary>Installs a new manifest atomically.</summary>
    internal static void Save(string directory, ManifestState state)
    {
        string path = PathIn(directory);
        string temporary = path + ".tmp";

        byte[] json = JsonSerializer.SerializeToUtf8Bytes(state, SerializerOptions);

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
            stream.Write(json);

            // The temporary file must be durable before the rename, otherwise the rename could
            // survive a crash while its contents did not -- leaving a manifest that names
            // nothing.
            stream.Flush(flushToDisk: true);
        }

        File.Move(temporary, path, overwrite: true);
    }

    /// <summary>Builds a manifest state from a version and the engine's counters.</summary>
    internal static ManifestState From(
        LsmVersion version,
        ulong nextFileNumber,
        ulong lastSequence,
        ulong logNumber) => new()
        {
            FormatVersion = FormatVersion,
            NextFileNumber = nextFileNumber,
            LastSequence = lastSequence,
            LogNumber = logNumber,
            Levels = [.. version.Levels.Select(level =>
                (IReadOnlyList<ManifestState.TableRecord>)
                    [.. level.Select(ManifestState.TableRecord.From)])],
        };
}
