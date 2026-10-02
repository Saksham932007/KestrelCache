using System.Diagnostics;
using KestrelCache;
using KestrelCache.Cli;

if (args.Length == 0 || args[0] is "-h" or "--help" or "help")
{
    PrintUsage();
    return 0;
}

string command = args[0];
var options = new Args(args, skip: 1);

try
{
    return command switch
    {
        "demo" => await Commands.DemoAsync(options),
        "put" => await Commands.PutAsync(options),
        "get" => await Commands.GetAsync(options),
        "delete" => await Commands.DeleteAsync(options),
        "scan" => await Commands.ScanAsync(options),
        "stats" => await Commands.StatsAsync(options),
        "compact" => await Commands.CompactAsync(options),
        "load" => await Commands.LoadAsync(options),
        "crash-writer" => await Commands.CrashWriterAsync(options),
        _ => Unknown(command),
    };
}
catch (ArgumentException ex)
{
    Console.Error.WriteLine($"error: {ex.Message}");
    return 2;
}
catch (KestrelCacheException ex)
{
    Console.Error.WriteLine($"error: {ex.Message}");
    return 3;
}

static int Unknown(string command)
{
    Console.Error.WriteLine($"error: unknown command '{command}'");
    PrintUsage();
    return 2;
}

static void PrintUsage()
{
    Console.WriteLine(
        """
        kestrel - KestrelCache command line

        USAGE
          kestrel <command> [options]

        COMMANDS
          demo              Run a short scripted walkthrough of the API
          put               Write one key          --path P --key K --value V
          get               Read one key           --path P --key K
          delete            Remove one key         --path P --key K
          scan              Iterate a key range    --path P [--prefix S] [--start S] [--end S] [--limit N]
          stats             Print engine counters  --path P
          compact           Force a compaction     --path P
          load              Write N keys and report throughput
                                                   --path P [--count N] [--value-size N] [--batch N]
          crash-writer      Durability test harness; see docs/DESIGN.md

        COMMON OPTIONS
          --path PATH       Database file (bitcask) or directory (lsm). Default: ./kc-data
          --engine NAME     bitcask | lsm. Default: lsm
          --sync POLICY     none | everywrite | interval. Default: interval

        EXAMPLES
          kestrel put --path ./db --key user:1 --value alice
          kestrel scan --path ./db --prefix user:
          kestrel load --path ./db --count 100000 --batch 500
        """);
}
