namespace KestrelCache;

/// <summary>
/// How aggressively the engine forces writes out of the OS page cache and onto stable storage.
/// </summary>
/// <remarks>
/// <para>
/// This is the single most important durability knob in the engine, and the distinction it
/// encodes is one that is easy to get wrong: a plain <c>FileStream.Flush()</c> (or
/// <c>FlushAsync</c>) only pushes bytes from the managed buffer into the operating system's
/// page cache. The data is then visible to other processes and survives a process crash
/// (<c>kill -9</c>), but it is <b>not</b> on the platter. A power cut or kernel panic loses it.
/// Reaching stable storage needs an explicit <c>fsync</c>, which is what
/// <see cref="FileStream.Flush(bool)"/> with <c>flushToDisk: true</c> issues.
/// </para>
/// <para>
/// An <c>fsync</c> costs roughly 0.1-10 ms depending on the device, which is three to five
/// orders of magnitude more than the in-memory work surrounding it. So this is a real
/// trade-off rather than a free safety switch, and the right answer depends on what the caller
/// can tolerate losing.
/// </para>
/// </remarks>
public enum SyncPolicy
{
    /// <summary>
    /// Never force an fsync; rely on the OS to flush the page cache eventually.
    /// Fastest. Survives process crashes but not power loss.
    /// </summary>
    None = 0,

    /// <summary>
    /// fsync before every write is acknowledged. Slowest and strictest: an acknowledged write
    /// is guaranteed durable. This is what a database claiming "durable" should mean.
    /// </summary>
    EveryWrite = 1,

    /// <summary>
    /// fsync on a background interval (see <see cref="DatabaseOptions.SyncInterval"/>).
    /// A bounded-loss compromise: at most one interval's worth of acknowledged writes can be
    /// lost to power failure. This is the policy most real deployments actually run.
    /// </summary>
    Interval = 2,
}
