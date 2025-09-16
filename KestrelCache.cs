// KestrelCache.cs
using System.Buffers.Binary;
using System.IO.Hashing;
using System.Text;

public sealed class KestrelCache : IAsyncDisposable
{
    private const int HeaderSize = 12; // Checksum (4) + KeyLen (4) + ValueLen (4)
    private readonly FileStream _fileStream;
    private readonly Dictionary<string, long> _index = new();
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    private KestrelCache(FileStream fileStream)
    {
        _fileStream = fileStream;
    }

    public static async Task<KestrelCache> OpenAsync(string path)
    {
        var fileStream = new FileStream(path, FileMode.OpenOrCreate, 
            FileAccess.ReadWrite, FileShare.None, 4096, useAsync: true);
        
        var db = new KestrelCache(fileStream);
        await db.LoadIndexAsync();
        return db;
    }

    private async Task LoadIndexAsync()
    {
        _fileStream.Seek(0, SeekOrigin.Begin);
        var buffer = new byte[HeaderSize];
        long currentOffset = 0;

        while (await _fileStream.ReadAsync(buffer, 0, HeaderSize) == HeaderSize)
        {
            var header = buffer.AsSpan();
            int keyLen = BinaryPrimitives.ReadInt32LittleEndian(header.Slice(4));
            int valueLen = BinaryPrimitives.ReadInt32LittleEndian(header.Slice(8));

            var keyBuffer = new byte[keyLen];
            await _fileStream.ReadExactlyAsync(keyBuffer);
            var key = Encoding.UTF8.GetString(keyBuffer);

            if (valueLen == -1) // -1 signifies a tombstone for deletion
            {
                _index.Remove(key);
            }
            else
            {
                _index[key] = currentOffset;
            }

            // Seek past the value to the next record
            currentOffset = _fileStream.Position + valueLen;
            if(valueLen > 0)
            {
                _fileStream.Seek(valueLen, SeekOrigin.Current);
            }
        }
    }

    public async Task<string?> GetAsync(string key)
    {
        if (!_index.TryGetValue(key, out long offset))
        {
            return null;
        }

        await _writeLock.WaitAsync();
        try
        {
            _fileStream.Seek(offset, SeekOrigin.Begin);
            
            var headerBuffer = new byte[HeaderSize];
            await _fileStream.ReadExactlyAsync(headerBuffer);
            var header = headerBuffer.AsSpan();
            
            uint savedChecksum = BinaryPrimitives.ReadUInt32LittleEndian(header);
            int keyLen = BinaryPrimitives.ReadInt32LittleEndian(header.Slice(4));
            int valueLen = BinaryPrimitives.ReadInt32LittleEndian(header.Slice(8));

            var keyAndValueBuffer = new byte[keyLen + valueLen];
            await _fileStream.ReadExactlyAsync(keyAndValueBuffer);
            
            uint calculatedChecksum = Crc32.HashToUInt32(keyAndValueBuffer);
            if (savedChecksum != calculatedChecksum)
            {
                throw new IOException("Data corruption detected. Checksum mismatch.");
            }
            
            var value = Encoding.UTF8.GetString(keyAndValueBuffer.AsSpan().Slice(keyLen));
            return value;
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async Task PutAsync(string key, string value)
    {
        var keyBytes = Encoding.UTF8.GetBytes(key);
        var valueBytes = Encoding.UTF8.GetBytes(value);

        await _writeLock.WaitAsync();
        try
        {
            long currentOffset = _fileStream.Length;

            var keyAndValueBuffer = new byte[keyBytes.Length + valueBytes.Length];
            keyBytes.CopyTo(keyAndValueBuffer, 0);
            valueBytes.CopyTo(keyAndValueBuffer, keyBytes.Length);
            
            uint checksum = Crc32.HashToUInt32(keyAndValueBuffer);
            
            var headerBuffer = new byte[HeaderSize];
            BinaryPrimitives.WriteUInt32LittleEndian(headerBuffer.AsSpan(), checksum);
            BinaryPrimitives.WriteInt32LittleEndian(headerBuffer.AsSpan(4), keyBytes.Length);
            BinaryPrimitives.WriteInt32LittleEndian(headerBuffer.AsSpan(8), valueBytes.Length);

            _fileStream.Seek(0, SeekOrigin.End);
            await _fileStream.WriteAsync(headerBuffer);
            await _fileStream.WriteAsync(keyAndValueBuffer);
            await _fileStream.FlushAsync();

            _index[key] = currentOffset;
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async Task DeleteAsync(string key)
    {
        var keyBytes = Encoding.UTF8.GetBytes(key);

        await _writeLock.WaitAsync();
        try
        {
            uint checksum = Crc32.HashToUInt32(keyBytes);

            var headerBuffer = new byte[HeaderSize];
            BinaryPrimitives.WriteUInt32LittleEndian(headerBuffer.AsSpan(), checksum);
            BinaryPrimitives.WriteInt32LittleEndian(headerBuffer.AsSpan(4), keyBytes.Length);
            BinaryPrimitives.WriteInt32LittleEndian(headerBuffer.AsSpan(8), -1);

            _fileStream.Seek(0, SeekOrigin.End);
            await _fileStream.WriteAsync(headerBuffer);
            await _fileStream.WriteAsync(keyBytes);
            await _fileStream.FlushAsync();

            _index.Remove(key);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async Task CloseAsync()
    {
        await _fileStream.FlushAsync();
        _fileStream.Close();
    }
    
    public async ValueTask DisposeAsync()
    {
        await CloseAsync();
    }
}