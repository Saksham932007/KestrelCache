namespace KestrelCache.Lsm;

/// <summary>
/// A sorted skip list that one writer may mutate while any number of readers traverse it, with
/// no locks on the read path.
/// </summary>
/// <remarks>
/// <para>
/// This is the memtable's data structure, and the reason it is a skip list rather than a
/// balanced tree is concurrency. A red-black tree rebalances by rotating subtrees, which moves
/// nodes a concurrent reader may be standing on; making that safe needs a lock that every read
/// pays for. A skip list never moves a node. Insertion only ever publishes a new node by
/// redirecting forward pointers, so a reader either sees the pointer before the insert or after
/// it, and both are consistent, complete lists.
/// </para>
/// <para><b>The memory ordering argument</b></para>
/// <para>
/// Correctness rests on one rule: a node is fully initialised before any pointer to it becomes
/// visible. Insertion writes the new node's own forward pointers first, then publishes it with
/// <see cref="Volatile.Write{T}"/>, which is a release store — it cannot be reordered ahead of
/// the initialising writes. Readers load forward pointers with <see cref="Volatile.Read{T}"/>, an
/// acquire load, so a reader that observes the new pointer is guaranteed to observe the
/// initialisation that preceded it. Without the volatile pair the compiler or the CPU would be
/// free to make the publishing store visible first, and a reader could follow a pointer into a
/// node whose key array had not been assigned yet.
/// </para>
/// <para>
/// A reader may miss a concurrent insert entirely, which is fine: it then sees a slightly older
/// but perfectly consistent snapshot. That is the same guarantee the sequence numbers in
/// <see cref="InternalKey"/> provide at a higher level, so the two fit together.
/// </para>
/// <para>
/// <b>Single writer</b> is a precondition, not an implementation detail. The engine satisfies it
/// by serialising all writes behind one lock — which it has to do anyway, because the
/// write-ahead log is an append to a single file. Making the structure accept concurrent writers
/// would add CAS retry loops to pay for a concurrency the layer above cannot use.
/// </para>
/// </remarks>
internal sealed class SkipList
{
    /// <summary>
    /// Maximum tower height. 12 levels with a 1-in-4 promotion rate addresses roughly
    /// 4^12 ≈ 16.7M entries before the expected search cost degrades, which is far above the
    /// size at which a memtable gets flushed.
    /// </summary>
    private const int MaxHeight = 12;

    private const int BranchingFactor = 4;

    private readonly IComparer<byte[]> _comparer;
    private readonly Node _head;
    private readonly Random _random;

    private int _height = 1;
    private int _count;

    internal SkipList(IComparer<byte[]> comparer, int seed = 0x5EED)
    {
        _comparer = comparer;
        _head = new Node(key: null, value: null, MaxHeight);
        _random = new Random(seed);
    }

    /// <summary>Entries currently in the list.</summary>
    internal int Count => Volatile.Read(ref _count);

    /// <summary>
    /// Inserts an entry. The caller must guarantee that no other thread is inserting
    /// concurrently.
    /// </summary>
    internal void Insert(byte[] key, byte[] value)
    {
        var previous = new Node[MaxHeight];
        Node? existing = FindGreaterOrEqual(key, previous);

        // Duplicate internal keys cannot occur: the tag embeds a sequence number, and the engine
        // never issues the same sequence twice. If one shows up, the invariant is broken
        // somewhere upstream and silently overwriting would hide it.
        if (existing is not null
            && existing.Key is not null
            && _comparer.Compare(existing.Key, key) == 0)
        {
            throw new InvalidOperationException(
                "Duplicate internal key inserted into the memtable; sequence numbers must be unique.");
        }

        int height = RandomHeight();
        int currentHeight = Volatile.Read(ref _height);

        if (height > currentHeight)
        {
            // Levels above the current height start out empty, so the head is their predecessor.
            for (int level = currentHeight; level < height; level++)
            {
                previous[level] = _head;
            }

            // Publishing the taller height before the node is linked is safe: a reader that
            // starts at the new height finds null forward pointers there and simply drops to a
            // level that is populated.
            Volatile.Write(ref _height, height);
        }

        var node = new Node(key, value, height);

        for (int level = 0; level < height; level++)
        {
            // Order matters. The new node's own forward pointer is set first, using a plain
            // write because no reader can reach this node yet, and only then is the node
            // published with a release store.
            node.SetNextRelaxed(level, previous[level].Next(level));
            previous[level].SetNext(level, node);
        }

        Interlocked.Increment(ref _count);
    }

    /// <summary>
    /// Returns the first entry whose key is greater than or equal to <paramref name="key"/>, or
    /// <c>null</c> if no such entry exists.
    /// </summary>
    internal Node? Seek(byte[] key) => FindGreaterOrEqual(key, previous: null);

    /// <summary>The first entry in key order.</summary>
    internal Node? First() => _head.Next(0);

    /// <summary>Enumerates entries in ascending key order from <paramref name="start"/>.</summary>
    internal IEnumerable<Node> From(byte[]? start)
    {
        Node? node = start is null ? First() : Seek(start);
        while (node is not null)
        {
            yield return node;
            node = node.Next(0);
        }
    }

    private Node? FindGreaterOrEqual(byte[] key, Node[]? previous)
    {
        Node node = _head;
        int level = Volatile.Read(ref _height) - 1;

        while (true)
        {
            Node? next = node.Next(level);

            if (next is not null && _comparer.Compare(next.Key!, key) < 0)
            {
                // Still below the target at this level, so keep moving right.
                node = next;
                continue;
            }

            if (previous is not null) previous[level] = node;

            if (level == 0) return next;
            level--;
        }
    }

    /// <summary>
    /// Picks a tower height geometrically: 1 with probability 3/4, each additional level a
    /// further 1-in-4. This is what gives the structure its O(log n) search without any
    /// rebalancing — the distribution of heights does the balancing statistically.
    /// </summary>
    private int RandomHeight()
    {
        int height = 1;
        while (height < MaxHeight && _random.Next(BranchingFactor) == 0)
        {
            height++;
        }
        return height;
    }

    /// <summary>One entry, with a tower of forward pointers.</summary>
    internal sealed class Node
    {
        private readonly Node?[] _next;

        internal Node(byte[]? key, byte[]? value, int height)
        {
            Key = key;
            Value = value;
            _next = new Node?[height];
        }

        /// <summary>The internal key. Null only for the sentinel head node.</summary>
        internal byte[]? Key { get; }

        /// <summary>The stored value. Empty for a tombstone.</summary>
        internal byte[]? Value { get; }

        /// <summary>Acquire-load of the forward pointer at <paramref name="level"/>.</summary>
        internal Node? Next(int level) =>
            level < _next.Length ? Volatile.Read(ref _next[level]) : null;

        /// <summary>Release-store of the forward pointer at <paramref name="level"/>.</summary>
        internal void SetNext(int level, Node? node) => Volatile.Write(ref _next[level], node);

        /// <summary>
        /// Plain store, for initialising a node that no reader can reach yet. Using a relaxed
        /// write here rather than a volatile one is the whole reason the insert path is cheap.
        /// </summary>
        internal void SetNextRelaxed(int level, Node? node) => _next[level] = node;
    }
}
