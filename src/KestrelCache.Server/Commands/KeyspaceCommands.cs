using System.Text;
using KestrelCache.Server.Resp;

namespace KestrelCache.Server.Commands;

/// <summary>Commands that read or write the keyspace.</summary>
internal static class KeyspaceCommands
{
    private static ReadOnlySpan<byte> Pong => "PONG"u8;

    internal static ValueTask PingAsync(CommandContext context)
    {
        if (context.ArgumentCount == 0)
        {
            RespWriter.WriteSimpleString(context.Output, Pong);
        }
        else
        {
            // PING with an argument echoes it, which is how clients implement round-trip probes.
            RespWriter.WriteBulkString(context.Output, context.Argument(1));
        }
        return ValueTask.CompletedTask;
    }

    internal static ValueTask EchoAsync(CommandContext context)
    {
        RespWriter.WriteBulkString(context.Output, context.Argument(1));
        return ValueTask.CompletedTask;
    }

    // ---------------------------------------------------------------- strings

    internal static async ValueTask GetAsync(CommandContext context)
    {
        byte[]? value = await context.Database.Engine
            .GetAsync(context.Argument(1), context.CancellationToken)
            .ConfigureAwait(false);

        if (value is null)
        {
            context.Metrics.KeyspaceMiss();
            RespWriter.WriteNull(context.Output);
            return;
        }

        context.Metrics.KeyspaceHit();
        RespWriter.WriteBulkString(context.Output, value);
    }

    /// <summary>
    /// <c>SET key value [NX | XX] [GET]</c>.
    /// </summary>
    /// <remarks>
    /// The <c>NX</c> and <c>XX</c> conditions are a read followed by a write and are therefore
    /// not atomic against a concurrent writer here. Making them atomic needs a per-key lock,
    /// which <see cref="KeyLocks"/> provides for the counter commands — the difference is that a
    /// counter is useless without atomicity, while conditional SET is usually a convenience. The
    /// unsupported options are rejected rather than ignored: silently dropping <c>EX</c> would
    /// hand back <c>+OK</c> for a key that then never expires, which is worse than an error.
    /// </remarks>
    internal static async ValueTask SetAsync(CommandContext context)
    {
        byte[] key = context.Argument(1);
        byte[] value = context.Argument(2);

        bool onlyIfAbsent = false;
        bool onlyIfPresent = false;
        bool returnOld = false;

        for (int i = 3; i < context.Arguments.Count; i++)
        {
            switch (context.Keyword(i))
            {
                case "NX":
                    onlyIfAbsent = true;
                    break;
                case "XX":
                    onlyIfPresent = true;
                    break;
                case "GET":
                    returnOld = true;
                    break;
                case "EX" or "PX" or "EXAT" or "PXAT" or "KEEPTTL":
                    RespWriter.WriteError(
                        context.Output,
                        "ERR expiry is not supported: no record in the storage format carries a "
                            + "timestamp");
                    return;
                default:
                    RespWriter.WriteError(context.Output, "ERR syntax error");
                    return;
            }
        }

        if (onlyIfAbsent && onlyIfPresent)
        {
            RespWriter.WriteError(context.Output, "ERR syntax error");
            return;
        }

        byte[]? existing = null;
        if (onlyIfAbsent || onlyIfPresent || returnOld)
        {
            existing = await context.Database.Engine
                .GetAsync(key, context.CancellationToken)
                .ConfigureAwait(false);
        }

        bool allowed = (!onlyIfAbsent || existing is null) && (!onlyIfPresent || existing is not null);

        if (allowed)
        {
            await context.Database.Engine
                .PutAsync(key, value, context.CancellationToken)
                .ConfigureAwait(false);
        }

        if (returnOld)
        {
            if (existing is null) RespWriter.WriteNull(context.Output);
            else RespWriter.WriteBulkString(context.Output, existing);
        }
        else if (allowed)
        {
            RespWriter.WriteOk(context.Output);
        }
        else
        {
            RespWriter.WriteNull(context.Output);
        }
    }

    internal static async ValueTask SetNxAsync(CommandContext context)
    {
        byte[] key = context.Argument(1);

        bool exists = await context.Database.Engine
            .GetAsync(key, context.CancellationToken).ConfigureAwait(false) is not null;

        if (!exists)
        {
            await context.Database.Engine
                .PutAsync(key, context.Argument(2), context.CancellationToken)
                .ConfigureAwait(false);
        }

        RespWriter.WriteInteger(context.Output, exists ? 0 : 1);
    }

    internal static async ValueTask GetSetAsync(CommandContext context)
    {
        byte[] key = context.Argument(1);

        byte[]? previous = await context.Database.Engine
            .GetAsync(key, context.CancellationToken).ConfigureAwait(false);

        await context.Database.Engine
            .PutAsync(key, context.Argument(2), context.CancellationToken)
            .ConfigureAwait(false);

        if (previous is null) RespWriter.WriteNull(context.Output);
        else RespWriter.WriteBulkString(context.Output, previous);
    }

    internal static async ValueTask AppendAsync(CommandContext context)
    {
        byte[] key = context.Argument(1);
        byte[] suffix = context.Argument(2);

        // Read-modify-write, so it is serialised per key for the same reason the counters are.
        using var _ = await KeyLocks.AcquireAsync(key, context.CancellationToken).ConfigureAwait(false);

        byte[]? existing = await context.Database.Engine
            .GetAsync(key, context.CancellationToken).ConfigureAwait(false);

        byte[] combined;
        if (existing is null || existing.Length == 0)
        {
            combined = suffix;
        }
        else
        {
            combined = new byte[existing.Length + suffix.Length];
            existing.CopyTo(combined, 0);
            suffix.CopyTo(combined, existing.Length);
        }

        await context.Database.Engine
            .PutAsync(key, combined, context.CancellationToken)
            .ConfigureAwait(false);

        RespWriter.WriteInteger(context.Output, combined.Length);
    }

    internal static async ValueTask StrLenAsync(CommandContext context)
    {
        byte[]? value = await context.Database.Engine
            .GetAsync(context.Argument(1), context.CancellationToken).ConfigureAwait(false);

        RespWriter.WriteInteger(context.Output, value?.Length ?? 0);
    }

    internal static async ValueTask MGetAsync(CommandContext context)
    {
        RespWriter.WriteArrayHeader(context.Output, context.ArgumentCount);

        for (int i = 1; i < context.Arguments.Count; i++)
        {
            byte[]? value = await context.Database.Engine
                .GetAsync(context.Argument(i), context.CancellationToken).ConfigureAwait(false);

            if (value is null)
            {
                context.Metrics.KeyspaceMiss();
                RespWriter.WriteNull(context.Output);
            }
            else
            {
                context.Metrics.KeyspaceHit();
                RespWriter.WriteBulkString(context.Output, value);
            }
        }
    }

    /// <summary>
    /// <c>MSET</c>, which maps directly onto the engine's atomic batch — so it is genuinely
    /// all-or-nothing rather than a loop of individual writes, and costs one fsync instead of one
    /// per key.
    /// </summary>
    internal static async ValueTask MSetAsync(CommandContext context)
    {
        if (context.ArgumentCount % 2 != 0)
        {
            CommandTable.WriteWrongArity(context, "mset");
            return;
        }

        var batch = new WriteBatch();
        for (int i = 1; i < context.Arguments.Count; i += 2)
        {
            batch.Put(context.Argument(i), context.Argument(i + 1));
        }

        await context.Database.WriteAsync(batch, context.CancellationToken).ConfigureAwait(false);
        RespWriter.WriteOk(context.Output);
    }

    /// <summary>
    /// <c>DEL</c>. Returns the number of keys that existed, which requires reading each one
    /// first; the deletes themselves go out as a single atomic batch.
    /// </summary>
    internal static async ValueTask DelAsync(CommandContext context)
    {
        long removed = 0;
        var batch = new WriteBatch();

        for (int i = 1; i < context.Arguments.Count; i++)
        {
            byte[] key = context.Argument(i);
            if (await context.Database.Engine.GetAsync(key, context.CancellationToken)
                    .ConfigureAwait(false) is not null)
            {
                removed++;
            }
            batch.Delete(key);
        }

        await context.Database.WriteAsync(batch, context.CancellationToken).ConfigureAwait(false);
        RespWriter.WriteInteger(context.Output, removed);
    }

    internal static async ValueTask ExistsAsync(CommandContext context)
    {
        long found = 0;

        for (int i = 1; i < context.Arguments.Count; i++)
        {
            if (await context.Database.Engine.GetAsync(context.Argument(i), context.CancellationToken)
                    .ConfigureAwait(false) is not null)
            {
                found++;
            }
        }

        RespWriter.WriteInteger(context.Output, found);
    }

    internal static async ValueTask TypeAsync(CommandContext context)
    {
        byte[]? value = await context.Database.Engine
            .GetAsync(context.Argument(1), context.CancellationToken).ConfigureAwait(false);

        RespWriter.WriteSimpleString(context.Output, value is null ? "none" : "string");
    }

    // ---------------------------------------------------------------- counters

    internal static ValueTask IncrAsync(CommandContext context) => AddAsync(context, 1);

    internal static ValueTask DecrAsync(CommandContext context) => AddAsync(context, -1);

    internal static ValueTask IncrByAsync(CommandContext context)
    {
        if (!CommandTable.TryParseInteger(context.Argument(2), out long delta))
        {
            RespWriter.WriteError(context.Output, "ERR value is not an integer or out of range");
            return ValueTask.CompletedTask;
        }
        return AddAsync(context, delta);
    }

    internal static ValueTask DecrByAsync(CommandContext context)
    {
        if (!CommandTable.TryParseInteger(context.Argument(2), out long delta))
        {
            RespWriter.WriteError(context.Output, "ERR value is not an integer or out of range");
            return ValueTask.CompletedTask;
        }
        return AddAsync(context, -delta);
    }

    /// <summary>
    /// The read-modify-write behind every counter command.
    /// </summary>
    /// <remarks>
    /// A counter is the one place where "roughly right" is useless: two clients incrementing at
    /// once must produce two increments, not one. The engine offers no compare-and-swap, so the
    /// atomicity has to come from above, which is what <see cref="KeyLocks"/> is for — a
    /// per-key lock rather than a global one, so unrelated counters do not contend.
    /// </remarks>
    private static async ValueTask AddAsync(CommandContext context, long delta)
    {
        byte[] key = context.Argument(1);

        using var _ = await KeyLocks.AcquireAsync(key, context.CancellationToken).ConfigureAwait(false);

        byte[]? existing = await context.Database.Engine
            .GetAsync(key, context.CancellationToken).ConfigureAwait(false);

        long current = 0;
        if (existing is { Length: > 0 })
        {
            if (!long.TryParse(Encoding.UTF8.GetString(existing), out current))
            {
                RespWriter.WriteError(context.Output, "ERR value is not an integer or out of range");
                return;
            }
        }

        // Redis reports overflow rather than wrapping, and so should this.
        try
        {
            current = checked(current + delta);
        }
        catch (OverflowException)
        {
            RespWriter.WriteError(context.Output, "ERR increment or decrement would overflow");
            return;
        }

        await context.Database.Engine
            .PutAsync(key, Encoding.UTF8.GetBytes(current.ToString()), context.CancellationToken)
            .ConfigureAwait(false);

        RespWriter.WriteInteger(context.Output, current);
    }

    // ---------------------------------------------------------------- iteration

    /// <summary>
    /// <c>SCAN cursor [MATCH pattern] [COUNT n]</c>, implemented over the engine's ordered scan.
    /// </summary>
    /// <remarks>
    /// Redis's cursor is an opaque hash-table position, because its keyspace is a hash table.
    /// Here the keyspace is sorted, so the cursor can be the last key returned — which makes
    /// SCAN a genuine resumable range scan rather than a best-effort walk, and gives it a
    /// guarantee Redis's cannot: a key present throughout the iteration is returned exactly
    /// once, with no duplicates.
    /// </remarks>
    internal static async ValueTask ScanAsync(CommandContext context)
    {
        if (context.Database.Engine is not IScannableStorageEngine scannable)
        {
            RespWriter.WriteError(
                context.Output,
                $"ERR SCAN needs ordered iteration, which the {context.Database.Engine.Name} "
                    + "engine cannot provide; start the server with --engine lsm");
            return;
        }

        string cursor = context.Text(1);
        string? pattern = null;
        int count = 10;

        for (int i = 2; i + 1 < context.Arguments.Count; i += 2)
        {
            switch (context.Keyword(i))
            {
                case "MATCH":
                    pattern = context.Text(i + 1);
                    break;
                case "COUNT":
                    if (!CommandTable.TryParseInteger(context.Argument(i + 1), out long parsed)
                        || parsed <= 0)
                    {
                        RespWriter.WriteError(context.Output, "ERR syntax error");
                        return;
                    }
                    count = (int)Math.Min(parsed, 10_000);
                    break;
                default:
                    RespWriter.WriteError(context.Output, "ERR syntax error");
                    return;
            }
        }

        // "0" starts a fresh iteration; anything else resumes strictly after that key.
        byte[]? start = cursor == "0" ? null : ByteKey.From(cursor);
        bool skipFirst = start is not null;

        var keys = new List<byte[]>(count);
        byte[]? lastKey = null;

        await foreach (var (key, _) in scannable
            .ScanAsync(start, null, context.CancellationToken)
            .ConfigureAwait(false))
        {
            if (skipFirst)
            {
                skipFirst = false;
                if (start is not null && key.AsSpan().SequenceEqual(start)) continue;
            }

            lastKey = key;
            if (pattern is null || GlobMatcher.Matches(pattern, Encoding.UTF8.GetString(key)))
            {
                keys.Add(key);
            }

            if (keys.Count >= count) break;
        }

        // An exhausted iteration reports cursor "0", which is how a client knows to stop.
        bool exhausted = keys.Count < count;
        string nextCursor = exhausted || lastKey is null ? "0" : Encoding.UTF8.GetString(lastKey);

        RespWriter.WriteArrayHeader(context.Output, 2);
        RespWriter.WriteBulkString(context.Output, nextCursor);
        RespWriter.WriteArrayHeader(context.Output, keys.Count);
        foreach (byte[] key in keys)
        {
            RespWriter.WriteBulkString(context.Output, key);
        }
    }

    /// <summary>
    /// <c>KEYS pattern</c>.
    /// </summary>
    /// <remarks>
    /// Kept because it is what everyone reaches for, but it walks the whole keyspace and buffers
    /// the result, so it is as unsuitable for production here as it is in Redis. A literal
    /// prefix is extracted from the pattern where possible and turned into a range scan, which
    /// makes the common <c>user:*</c> case cost the matching range rather than the database.
    /// </remarks>
    internal static async ValueTask KeysAsync(CommandContext context)
    {
        if (context.Database.Engine is not IScannableStorageEngine scannable)
        {
            RespWriter.WriteError(
                context.Output,
                $"ERR KEYS needs ordered iteration, which the {context.Database.Engine.Name} "
                    + "engine cannot provide; start the server with --engine lsm");
            return;
        }

        string pattern = context.Text(1);
        string literalPrefix = GlobMatcher.LiteralPrefixOf(pattern);

        byte[]? start = literalPrefix.Length > 0 ? ByteKey.From(literalPrefix) : null;
        byte[]? end = start is not null ? ByteKey.PrefixUpperBound(start) : null;

        var matches = new List<byte[]>();

        await foreach (var (key, _) in scannable
            .ScanAsync(start, end, context.CancellationToken)
            .ConfigureAwait(false))
        {
            if (GlobMatcher.Matches(pattern, Encoding.UTF8.GetString(key)))
            {
                matches.Add(key);
            }
        }

        RespWriter.WriteArrayHeader(context.Output, matches.Count);
        foreach (byte[] key in matches)
        {
            RespWriter.WriteBulkString(context.Output, key);
        }
    }
}
