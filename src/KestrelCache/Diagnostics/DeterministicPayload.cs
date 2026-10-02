using System.Text;

namespace KestrelCache.Diagnostics;

/// <summary>
/// Derives a value from a key, so that a reader can verify not only that a record is present
/// but that it holds the right bytes, without having to be told what was written.
/// </summary>
/// <remarks>
/// This lives in the library rather than in the load generator because two independent
/// processes have to agree on it: the crash-test harness writes records in a child process that
/// is then killed, and the test that reopens the database afterwards has to know what each
/// surviving key should contain. Encoding the key into its own value makes that agreement
/// implicit — a record that comes back carrying the wrong key's payload is self-evidently
/// wrong, which is exactly the failure a misaligned index produces.
/// </remarks>
public static class DeterministicPayload
{
    /// <summary>Builds the canonical value for <paramref name="key"/>, padded to <paramref name="size"/> bytes.</summary>
    public static string ValueFor(string key, int size)
    {
        ArgumentNullException.ThrowIfNull(key);

        var builder = new StringBuilder(Math.Max(size, key.Length + 1));
        builder.Append(key).Append('|');
        while (builder.Length < size)
        {
            builder.Append((char)('a' + (builder.Length % 26)));
        }
        return builder.ToString();
    }

    /// <summary>The same payload as UTF-8 bytes.</summary>
    public static byte[] BytesFor(string key, int size) => ByteKey.From(ValueFor(key, size));
}
