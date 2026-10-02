using Microsoft.Win32.SafeHandles;

namespace KestrelCache.Internal;

/// <summary>
/// Thin wrappers that turn <see cref="RandomAccess"/>'s short-read/short-write contract into
/// read-it-all / write-it-all operations.
/// </summary>
/// <remarks>
/// <para>
/// Every read and write in the engine goes through positional I/O rather than a stream cursor.
/// That choice is what makes concurrent reads possible at all: a <see cref="FileStream"/> has a
/// single mutable file position, so two threads reading at once interleave their seeks and
/// corrupt each other's reads. The only way to make that safe is to serialise readers behind a
/// lock, which is exactly what the original implementation had to do — and it meant "fast
/// indexed reads" were in practice single-threaded. <see cref="RandomAccess"/> takes the offset
/// as an argument (<c>pread</c>/<c>pwrite</c> underneath), so there is no shared cursor and no
/// lock is needed on the read path at all.
/// </para>
/// <para>
/// The loops matter too: <c>pread</c> is explicitly allowed to return fewer bytes than asked
/// for, and ignoring that produces reads that are silently short under memory pressure or on
/// network filesystems.
/// </para>
/// </remarks>
internal static class PositionalIo
{
    /// <summary>Reads exactly <paramref name="buffer"/>.Length bytes, or throws.</summary>
    internal static void ReadExactly(SafeFileHandle handle, Span<byte> buffer, long offset)
    {
        int read = 0;
        while (read < buffer.Length)
        {
            int n = RandomAccess.Read(handle, buffer[read..], offset + read);
            if (n == 0)
            {
                throw new EndOfStreamException(
                    $"Expected {buffer.Length} bytes at offset {offset} but only {read} were available.");
            }
            read += n;
        }
    }

    /// <summary>Reads up to <paramref name="buffer"/>.Length bytes, returning how many arrived.</summary>
    internal static int ReadAtMost(SafeFileHandle handle, Span<byte> buffer, long offset)
    {
        int read = 0;
        while (read < buffer.Length)
        {
            int n = RandomAccess.Read(handle, buffer[read..], offset + read);
            if (n == 0) break;
            read += n;
        }
        return read;
    }

    /// <summary>Asynchronously reads exactly <paramref name="buffer"/>.Length bytes, or throws.</summary>
    internal static async ValueTask ReadExactlyAsync(
        SafeFileHandle handle,
        Memory<byte> buffer,
        long offset,
        CancellationToken cancellationToken = default)
    {
        int read = 0;
        while (read < buffer.Length)
        {
            int n = await RandomAccess.ReadAsync(handle, buffer[read..], offset + read, cancellationToken)
                .ConfigureAwait(false);
            if (n == 0)
            {
                throw new EndOfStreamException(
                    $"Expected {buffer.Length} bytes at offset {offset} but only {read} were available.");
            }
            read += n;
        }
    }

    /// <summary>Writes the whole buffer at <paramref name="offset"/>.</summary>
    internal static async ValueTask WriteAllAsync(
        SafeFileHandle handle,
        ReadOnlyMemory<byte> buffer,
        long offset,
        CancellationToken cancellationToken = default)
    {
        // RandomAccess.WriteAsync already loops internally until the buffer is consumed, but
        // being explicit keeps the contract obvious at the call site.
        await RandomAccess.WriteAsync(handle, buffer, offset, cancellationToken).ConfigureAwait(false);
    }
}
