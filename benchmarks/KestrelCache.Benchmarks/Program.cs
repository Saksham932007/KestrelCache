using System.Reflection;
using BenchmarkDotNet.Running;
using KestrelCache.Benchmarks;

if (args.Length == 0 || args[0] is "-h" or "--help")
{
    Console.WriteLine(
        """
        kestrel-bench - KestrelCache performance suite

        USAGE
          kestrel-bench latency [--root DIR] [--ops N] [--value-size N]
          kestrel-bench micro   [BenchmarkDotNet filter...]

        latency   Real workloads against a real file, reporting p50/p95/p99/p99.9 per operation.
                  This is the suite whose numbers appear in the README.

        micro     BenchmarkDotNet microbenchmarks of the hot paths (record encoding, index
                  lookup, allocation counts). Build in Release or BenchmarkDotNet will refuse.

        NOTES
          --root must be on a real device. On a tmpfs, fsync does nothing and every durability
          number is meaningless; the suite prints a warning if it detects one.
        """);
    return 0;
}

if (args[0] == "micro")
{
    BenchmarkSwitcher
        .FromAssembly(Assembly.GetExecutingAssembly())
        .Run(args.Skip(1).ToArray());
    return 0;
}

if (args[0] != "latency")
{
    Console.Error.WriteLine($"error: unknown suite '{args[0]}'");
    return 2;
}

string root = ValueOf("--root")
    ?? Path.Combine(AppContext.BaseDirectory, "bench-scratch");
int operations = int.TryParse(ValueOf("--ops"), out int parsedOps) ? parsedOps : 100_000;
int valueSize = int.TryParse(ValueOf("--value-size"), out int parsedSize) ? parsedSize : 128;

Directory.CreateDirectory(root);

var suite = new LatencySuite(root, operations, valueSize);
await suite.RunAllAsync();
return 0;

string? ValueOf(string name)
{
    int index = Array.IndexOf(args, name);
    return index >= 0 && index + 1 < args.Length ? args[index + 1] : null;
}
