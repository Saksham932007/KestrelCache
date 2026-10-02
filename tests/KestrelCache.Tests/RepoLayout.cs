using System.Reflection;

namespace KestrelCache.Tests;

/// <summary>Locates build outputs that the tests need to launch as separate processes.</summary>
internal static class RepoLayout
{
    /// <summary>The repository root, found by walking up to the solution file.</summary>
    internal static string Root { get; } = FindRoot();

    /// <summary>Build configuration the tests were compiled in.</summary>
    internal static string Configuration { get; } =
        Assembly.GetExecutingAssembly()
            .GetCustomAttribute<AssemblyConfigurationAttribute>()?.Configuration
        ?? "Debug";

    /// <summary>
    /// Absolute path of the CLI's managed assembly, which the crash tests launch through the
    /// <c>dotnet</c> muxer.
    /// </summary>
    /// <remarks>
    /// The native apphost sitting next to it (<c>kestrel</c>) is deliberately not used. It
    /// resolves the runtime through the machine's registered install location, so it fails
    /// outright when the SDK lives somewhere unregistered such as <c>~/.dotnet</c> — which is
    /// exactly how a developer without root installs it. Going through the muxer that is already
    /// running this test process cannot have that problem.
    /// </remarks>
    internal static string CliAssembly
    {
        get
        {
            string path = Path.Combine(
                Root, "src", "KestrelCache.Cli", "bin", Configuration, "net9.0", "kestrel.dll");

            if (!File.Exists(path))
            {
                throw new FileNotFoundException(
                    $"The CLI assembly the crash tests depend on was not found at '{path}'. "
                        + "Build the solution, not just the test project.",
                    path);
            }

            return path;
        }
    }

    /// <summary>The <c>dotnet</c> executable to launch child processes with.</summary>
    internal static string DotnetMuxer { get; } = FindMuxer();

    private static string FindMuxer()
    {
        string fileName = OperatingSystem.IsWindows() ? "dotnet.exe" : "dotnet";

        // The surest answer: this test host was itself started by the muxer.
        string? current = Environment.ProcessPath;
        if (current is not null
            && Path.GetFileName(current).Equals(fileName, StringComparison.OrdinalIgnoreCase))
        {
            return current;
        }

        if (Environment.GetEnvironmentVariable("DOTNET_ROOT") is { Length: > 0 } dotnetRoot)
        {
            string candidate = Path.Combine(dotnetRoot, fileName);
            if (File.Exists(candidate)) return candidate;
        }

        foreach (string directory in
                 (Environment.GetEnvironmentVariable("PATH") ?? string.Empty)
                 .Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries))
        {
            string candidate = Path.Combine(directory, fileName);
            if (File.Exists(candidate)) return candidate;
        }

        // Last resort: let the OS resolve it.
        return fileName;
    }

    private static string FindRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "KestrelCache.sln")))
            {
                return directory.FullName;
            }
            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException(
            $"Could not locate KestrelCache.sln above '{AppContext.BaseDirectory}'.");
    }
}
