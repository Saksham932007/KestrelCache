namespace KestrelCache;

/// <summary>Which storage engine backs a <see cref="KestrelDb"/>.</summary>
public enum EngineKind
{
    /// <summary>
    /// Append-only log with a full in-memory hash index. One seek per read, no range scans,
    /// index must fit in RAM.
    /// </summary>
    Bitcask = 0,

    /// <summary>
    /// Log-structured merge tree: write-ahead log, memtable, levelled SSTables with Bloom
    /// filters. Supports ordered scans and a keyspace far larger than RAM.
    /// </summary>
    Lsm = 1,
}
