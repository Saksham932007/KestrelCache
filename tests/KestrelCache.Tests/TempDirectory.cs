namespace KestrelCache.Tests;

/// <summary>
/// A scratch directory that deletes itself, so engine tests touch real files — the whole point
/// of testing a storage engine — without leaking them across runs.
/// </summary>
/// <remarks>
/// <para>
/// The base directory deliberately defaults to <c>artifacts/test-scratch</c> inside the
/// repository rather than to <see cref="Path.GetTempPath"/>. On most Linux distributions
/// (Fedora among them) <c>/tmp</c> is a <c>tmpfs</c>, where <c>fsync</c> returns immediately
/// without doing anything, because there is no device to write back to. A storage engine test
/// suite running there is testing a RAM disk: torn writes never happen, fsync costs nothing, and
/// a build that had quietly stopped calling fsync at all would pass every test.
/// </para>
/// <para>
/// Set <c>KESTRELCACHE_TEST_DIR</c> to override — useful on CI runners where the workspace is
/// itself a RAM disk, or to point the suite at a specific device.
/// </para>
/// </remarks>
public sealed class TempDirectory : IDisposable
{
    /// <summary>Environment variable that overrides where scratch databases are created.</summary>
    public const string BaseDirectoryVariable = "KESTRELCACHE_TEST_DIR";

    private static readonly string BaseDirectory = ResolveBaseDirectory();

    public TempDirectory(string? label = null)
    {
        Path = System.IO.Path.Combine(
            BaseDirectory,
            $"{label ?? "test"}-{Guid.NewGuid():N}");
        Directory.CreateDirectory(Path);
    }

    /// <summary>Absolute path of the scratch directory.</summary>
    public string Path { get; }

    /// <summary>A path inside the scratch directory.</summary>
    public string File(string fileName) => System.IO.Path.Combine(Path, fileName);

    /// <summary>The conventional database path used by most tests.</summary>
    public string DbPath => File("test.kc");

    /// <summary>
    /// True when the backing filesystem actually honours fsync. Timing-sensitive durability
    /// assertions are meaningless on a RAM disk, so they check this first.
    /// </summary>
    public static bool BackedByRealDevice { get; } = !IsMemoryBacked(BaseDirectory);

    /// <summary>Where scratch databases are being created, for diagnostics.</summary>
    public static string Root => BaseDirectory;

    private static string ResolveBaseDirectory()
    {
        string? configured = Environment.GetEnvironmentVariable(BaseDirectoryVariable);
        string chosen = string.IsNullOrWhiteSpace(configured)
            ? System.IO.Path.Combine(RepoLayout.Root, "artifacts", "test-scratch")
            : configured;

        Directory.CreateDirectory(chosen);
        return chosen;
    }

    private static bool IsMemoryBacked(string path)
    {
        try
        {
            // Path.GetPathRoot is useless here: on Unix it returns "/" for everything, which
            // would report the root filesystem's type no matter which mount the path is really
            // on. The mount that owns a path is the one whose root is its longest prefix.
            string full = System.IO.Path.GetFullPath(path);

            DriveInfo? owner = null;
            foreach (var drive in DriveInfo.GetDrives())
            {
                string root = drive.RootDirectory.FullName;
                if (!full.StartsWith(root, StringComparison.Ordinal)) continue;
                if (owner is null || root.Length > owner.RootDirectory.FullName.Length)
                {
                    owner = drive;
                }
            }

            string? type = owner?.DriveFormat;
            return type is null or "tmpfs" or "ramfs" or "devtmpfs";
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            // Unknown filesystem: assume the worst and skip the timing-sensitive assertions
            // rather than report a spurious failure.
            return true;
        }
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(Path)) Directory.Delete(Path, recursive: true);
        }
        catch (IOException)
        {
            // A leaked handle on a failing test should not mask the real assertion failure.
        }
    }
}
