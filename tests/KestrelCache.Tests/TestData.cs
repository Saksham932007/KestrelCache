using System.Text;

namespace KestrelCache.Tests;

/// <summary>Small helpers to keep the tests themselves readable.</summary>
public static class TestData
{
    public static byte[] Key(string s) => Encoding.UTF8.GetBytes(s);

    public static byte[] Value(string s) => Encoding.UTF8.GetBytes(s);

    public static string? Text(byte[]? bytes) => bytes is null ? null : Encoding.UTF8.GetString(bytes);

    /// <summary>Deterministic pseudo-random bytes, so a failure reproduces from its seed alone.</summary>
    public static byte[] RandomBytes(Random random, int length)
    {
        var bytes = new byte[length];
        random.NextBytes(bytes);
        return bytes;
    }

    /// <summary>A key drawn from a small space, so collisions and overwrites actually happen.</summary>
    public static byte[] RandomKey(Random random, int keyspace) =>
        Key($"key:{random.Next(keyspace):D6}");
}
