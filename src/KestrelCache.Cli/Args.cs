namespace KestrelCache.Cli;

/// <summary>
/// Minimal <c>--flag value</c> parser.
/// </summary>
/// <remarks>
/// Hand-rolled on purpose: the CLI exists to demo the engine and to host the crash-test
/// harness, and taking a dependency on a full command-line framework for a dozen flags would
/// add version churn to a project whose point is the storage engine.
/// </remarks>
internal sealed class Args
{
    private readonly Dictionary<string, string?> _values = new(StringComparer.OrdinalIgnoreCase);
    private readonly List<string> _positional = [];

    internal Args(IReadOnlyList<string> argv, int skip = 0)
    {
        for (int i = skip; i < argv.Count; i++)
        {
            string token = argv[i];
            if (!token.StartsWith("--", StringComparison.Ordinal))
            {
                _positional.Add(token);
                continue;
            }

            string name = token[2..];
            if (i + 1 < argv.Count && !argv[i + 1].StartsWith("--", StringComparison.Ordinal))
            {
                _values[name] = argv[++i];
            }
            else
            {
                _values[name] = null; // bare switch
            }
        }
    }

    internal IReadOnlyList<string> Positional => _positional;

    internal bool Has(string name) => _values.ContainsKey(name);

    internal string? Value(string name) => _values.GetValueOrDefault(name);

    internal string Require(string name) =>
        Value(name) ?? throw new ArgumentException($"Missing required option --{name}.");

    internal string String(string name, string fallback) => Value(name) ?? fallback;

    internal int Int(string name, int fallback) =>
        Value(name) is { } raw && int.TryParse(raw, out int parsed) ? parsed : fallback;

    internal T Enum<T>(string name, T fallback) where T : struct, System.Enum =>
        Value(name) is { } raw && System.Enum.TryParse<T>(raw, ignoreCase: true, out T parsed)
            ? parsed
            : fallback;
}
