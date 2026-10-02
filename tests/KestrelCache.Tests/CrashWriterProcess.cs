using System.Collections.Concurrent;
using System.Diagnostics;

namespace KestrelCache.Tests;

/// <summary>
/// Drives the <c>kestrel crash-writer</c> child process and records exactly which writes it
/// acknowledged before being killed.
/// </summary>
internal sealed class CrashWriterProcess : IDisposable
{
    private readonly Process _process;
    private readonly ConcurrentQueue<string> _acknowledged = new();
    private readonly TaskCompletionSource _ready =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Task _reader;

    private CrashWriterProcess(Process process)
    {
        _process = process;
        _reader = Task.Run(PumpStdoutAsync);
    }

    /// <summary>Keys the child printed an ACK for, in order.</summary>
    internal IReadOnlyList<string> Acknowledged => [.. _acknowledged];

    internal static async Task<CrashWriterProcess> StartAsync(
        string path,
        EngineKind engine,
        SyncPolicy syncPolicy,
        int batchSize = 1,
        int valueSize = 64)
    {
        var startInfo = new ProcessStartInfo
        {
            FileName = RepoLayout.DotnetMuxer,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };

        startInfo.ArgumentList.Add(RepoLayout.CliAssembly);
        startInfo.ArgumentList.Add("crash-writer");
        startInfo.ArgumentList.Add("--path");
        startInfo.ArgumentList.Add(path);
        startInfo.ArgumentList.Add("--engine");
        startInfo.ArgumentList.Add(engine.ToString());
        startInfo.ArgumentList.Add("--sync");
        startInfo.ArgumentList.Add(syncPolicy.ToString());
        startInfo.ArgumentList.Add("--batch");
        startInfo.ArgumentList.Add(batchSize.ToString());
        startInfo.ArgumentList.Add("--value-size");
        startInfo.ArgumentList.Add(valueSize.ToString());

        var process = Process.Start(startInfo)
            ?? throw new InvalidOperationException("Failed to start the crash-writer process.");

        var harness = new CrashWriterProcess(process);
        await harness.WaitUntilReadyAsync();
        return harness;
    }

    private async Task WaitUntilReadyAsync()
    {
        var timeout = Task.Delay(TimeSpan.FromSeconds(30));
        if (await Task.WhenAny(_ready.Task, timeout) == timeout)
        {
            throw new TimeoutException("The crash-writer process never reported READY.");
        }
    }

    private async Task PumpStdoutAsync()
    {
        try
        {
            while (await _process.StandardOutput.ReadLineAsync() is { } line)
            {
                if (line == "READY")
                {
                    _ready.TrySetResult();
                }
                else if (line.StartsWith("ACK ", StringComparison.Ordinal))
                {
                    _acknowledged.Enqueue(line[4..]);
                }
            }
        }
        catch (IOException)
        {
            // The pipe breaks when the child is killed. Expected.
        }
        catch (ObjectDisposedException)
        {
            // Same, if disposal races the kill.
        }
        finally
        {
            _ready.TrySetResult();
        }
    }

    /// <summary>Blocks until the child has acknowledged at least <paramref name="count"/> writes.</summary>
    internal async Task WaitForAcknowledgementsAsync(int count, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (_acknowledged.Count < count)
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException(
                    $"Only {_acknowledged.Count} of {count} acknowledgements arrived in {timeout}.");
            }
            if (_process.HasExited)
            {
                throw new InvalidOperationException(
                    $"The crash-writer exited early with code {_process.ExitCode}.");
            }
            await Task.Delay(5);
        }
    }

    /// <summary>
    /// Kills the child with SIGKILL and returns the keys it had acknowledged.
    /// </summary>
    /// <remarks>
    /// SIGKILL specifically, not a graceful shutdown: the whole point is to deny the process any
    /// opportunity to flush, close handles or run finalisers, because a durability guarantee that
    /// only holds when the process exits politely is not a durability guarantee.
    /// </remarks>
    internal async Task<IReadOnlyList<string>> KillAsync()
    {
        if (!_process.HasExited)
        {
            _process.Kill(entireProcessTree: true);
        }

        await _process.WaitForExitAsync();

        // Give the stdout pump a moment to drain whatever the kernel already buffered, so the
        // acknowledged set is as large (and the assertion as strict) as it can honestly be.
        await Task.WhenAny(_reader, Task.Delay(TimeSpan.FromSeconds(5)));

        return Acknowledged;
    }

    public void Dispose()
    {
        try
        {
            if (!_process.HasExited) _process.Kill(entireProcessTree: true);
        }
        catch (InvalidOperationException)
        {
            // Already gone.
        }
        _process.Dispose();
    }
}
