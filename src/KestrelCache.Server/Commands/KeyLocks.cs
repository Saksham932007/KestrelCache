using System.Collections.Concurrent;

namespace KestrelCache.Server.Commands;

/// <summary>
/// Per-key mutual exclusion for the read-modify-write commands.
/// </summary>
/// <remarks>
/// <para>
/// <c>INCR</c> is the motivating case. The engine offers no compare-and-swap, so an increment is
/// a read followed by a write, and two clients incrementing the same key at once would both read
/// the same value and both write the same result — losing one increment. A counter that loses
/// increments is not a counter.
/// </para>
/// <para>
/// A single global lock would fix that and serialise every mutating command in the server, which
/// throws away the concurrency the whole read path was built for. So locks are striped: the key
/// is hashed to one of a fixed number of semaphores, and only keys landing in the same stripe
/// contend. 1024 stripes means two unrelated counters collide about one time in a thousand,
/// which costs nothing, while the memory stays constant no matter how many keys exist.
/// </para>
/// <para>
/// A dictionary of one lock per key would avoid false sharing entirely but introduces a harder
/// problem — knowing when a key's lock can be removed without racing a thread about to take it.
/// Striping sidesteps the lifetime question completely by never allocating or freeing anything.
/// </para>
/// </remarks>
internal static class KeyLocks
{
    /// <summary>Number of stripes. A power of two so the index is a mask rather than a modulo.</summary>
    private const int StripeCount = 1024;

    private static readonly SemaphoreSlim[] Stripes = CreateStripes();

    private static SemaphoreSlim[] CreateStripes()
    {
        var stripes = new SemaphoreSlim[StripeCount];
        for (int i = 0; i < StripeCount; i++)
        {
            stripes[i] = new SemaphoreSlim(1, 1);
        }
        return stripes;
    }

    /// <summary>Takes the lock covering <paramref name="key"/>; dispose the result to release it.</summary>
    internal static async ValueTask<Releaser> AcquireAsync(
        byte[] key,
        CancellationToken cancellationToken)
    {
        var stripe = Stripes[StripeFor(key)];
        await stripe.WaitAsync(cancellationToken).ConfigureAwait(false);
        return new Releaser(stripe);
    }

    private static int StripeFor(byte[] key) =>
        ByteKeyComparer.Instance.GetHashCode(key) & (StripeCount - 1);

    /// <summary>Releases a stripe on dispose.</summary>
    internal readonly struct Releaser(SemaphoreSlim stripe) : IDisposable
    {
        public void Dispose() => stripe.Release();
    }
}
