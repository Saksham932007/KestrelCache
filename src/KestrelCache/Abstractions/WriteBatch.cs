namespace KestrelCache;

/// <summary>A single mutation inside a <see cref="WriteBatch"/>.</summary>
public readonly record struct WriteOp
{
    /// <summary>The key being written or deleted.</summary>
    public byte[] Key { get; init; }

    /// <summary>The new value, or <c>null</c> when this op is a delete.</summary>
    public byte[]? Value { get; init; }

    /// <summary>True when this op removes the key.</summary>
    public bool IsDelete => Value is null;
}

/// <summary>
/// A set of mutations applied as one atomic, durable unit.
/// </summary>
/// <remarks>
/// Batching is not only an ergonomic convenience. It is the mechanism by which the engine
/// amortises its most expensive operation: one batch means one append and one fsync regardless
/// of how many keys it touches, so a 1000-key batch costs roughly one fsync instead of 1000.
/// Atomicity follows from the same property — a reader either sees the whole batch or none of
/// it, because the batch becomes visible only once its log record is fully written.
/// </remarks>
public sealed class WriteBatch
{
    private readonly List<WriteOp> _ops = [];

    /// <summary>The ops in insertion order. Later ops on the same key win.</summary>
    public IReadOnlyList<WriteOp> Ops => _ops;

    /// <summary>Number of mutations queued.</summary>
    public int Count => _ops.Count;

    /// <summary>Approximate encoded size of the batch, used for memtable accounting.</summary>
    public long ApproximateSizeBytes { get; private set; }

    /// <summary>Queues a put.</summary>
    public WriteBatch Put(byte[] key, byte[] value)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        _ops.Add(new WriteOp { Key = key, Value = value });
        ApproximateSizeBytes += key.Length + value.Length + 16;
        return this;
    }

    /// <summary>Queues a put, UTF-8 encoding both arguments.</summary>
    public WriteBatch Put(string key, string value) => Put(ByteKey.From(key), ByteKey.From(value));

    /// <summary>Queues a delete.</summary>
    public WriteBatch Delete(byte[] key)
    {
        ArgumentNullException.ThrowIfNull(key);
        _ops.Add(new WriteOp { Key = key, Value = null });
        ApproximateSizeBytes += key.Length + 16;
        return this;
    }

    /// <summary>Queues a delete, UTF-8 encoding the key.</summary>
    public WriteBatch Delete(string key) => Delete(ByteKey.From(key));

    /// <summary>Empties the batch so it can be reused.</summary>
    public void Clear()
    {
        _ops.Clear();
        ApproximateSizeBytes = 0;
    }
}
