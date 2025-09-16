# KestrelCache: A Simple Key-Value Database from Scratch

KestrelCache is a simple, persistent, key-value database built from the ground up in C\# and .NET 8. This project was created as a learning exercise to explore the fundamental principles of database design, including file I/O, data serialization, indexing, and data integrity.

## Core Concepts

The database is built on the principle of a **log-structured file**, a design used by many high-performance databases like Kafka and Bitcask.

1.  **Append-Only Data File**: All data is written to a single file in an append-only fashion. New key-value pairs are always added to the end. This makes writes incredibly fast and simplifies concurrency, as we never modify existing data in place.

2.  **In-Memory Hash Index**: To provide fast read access, KestrelCache builds an index of key locations in memory on startup. This index is a `Dictionary<string, long>` that maps each key to its byte offset in the data file. A read operation becomes a quick dictionary lookup followed by a single disk seek.

3.  **Tombstone Deletes**: When a key is deleted, we don't remove it from the file. Instead, we append a special "tombstone" record. This marker indicates that the key is no longer valid and is removed from the in-memory index.

Here's a conceptual overview:

```
[ In-Memory Index ]         [ Data File on Disk: "my-data.db" ]
+---------------+           +---------------------------------+
| "user:1" -> 0 | --------> | [Record for user:1]             |  (Offset 0)
| "user:2" -> 85| ----+     | ...                             |
+---------------+     |     +---------------------------------+
                      +---> | [Record for user:2]             |  (Offset 85)
                            | ...                             |
                            +---------------------------------+
                            | [Tombstone for user:1]          |  (Offset 170)
                            +---------------------------------+
```

## ✨ Features

  * **Persistent Storage**: Data is saved to disk and survives application restarts.
  * **Fast Append-Only Writes**: Optimized for high write throughput.
  * **Indexed Reads**: Quick key lookups using an in-memory hash table.
  * **Checksum Verification**: Each record includes a CRC32 checksum to detect and prevent data corruption.
  * **Simple API**: An easy-to-use asynchronous API for `Get`, `Put`, and `Delete` operations.
  * **Cross-Platform**: Built with .NET 8, it runs anywhere .NET does (Windows, macOS, Linux).

## 🚀 Getting Started

### Prerequisites

  * [.NET 8.0 SDK](https://dotnet.microsoft.com/download) or later.

### Installation & Usage

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/your-username/KestrelCache.git
    cd KestrelCache
    ```

2.  **Build the project:**

    ```bash
    dotnet build
    ```

3.  **Run the application:**

    ```bash
    dotnet run
    ```

### Example API Usage

The API is designed to be simple and intuitive.

```csharp
// Open the database file (or create it)
var db = await KestrelCache.OpenAsync("my-data.db");

// Write data
await db.PutAsync("user:1", "{\"name\": \"Alice\", \"email\": \"alice@example.com\"}");
await db.PutAsync("product:123", "{\"name\": \"Super Widget\", \"price\": 99.99}");

// Read data
string user1 = await db.GetAsync("user:1");
Console.WriteLine($"Found user: {user1}");

// Delete data
await db.DeleteAsync("product:123");

// Close the database gracefully
await db.CloseAsync();
```

## 🔧 On-Disk Format

Each record written to the data file follows a simple binary format for maximum efficiency.

| Component      | Size (bytes) | Description                                                  |
| -------------- | ------------ | ------------------------------------------------------------ |
| **Checksum** | 4            | A `CRC32` hash of the Key + Value to ensure data integrity.    |
| **Key Length** | 4            | An `int` representing the size of the Key in bytes.          |
| **Value Length**| 4            | An `int` representing the size of the Value in bytes. A value of `-1` marks a tombstone (delete). |
| **Key** | Variable     | The raw bytes of the key string (UTF-8 encoded).             |
| **Value** | Variable     | The raw bytes of the value string (UTF-8 encoded).           |

## 🛣️ Future Improvements

This project is a solid foundation, but there are many ways it could be extended to become a more feature-rich database:

  * **Compaction**: Implement a process to periodically clean the data file by removing old and deleted records, thus reclaiming disk space.
  * **On-Disk Index (B+Tree)**: Replace the in-memory index with a disk-based structure like a B+Tree to allow the database to scale beyond the limits of available RAM.
  * **Improved Concurrency**: Move from a single write lock to more granular locking or a lock-free design to handle higher-volume concurrent access.
  * **Caching Layer**: Add an in-memory cache (e.g., LRU cache) to keep frequently accessed records in memory and reduce disk I/O.

## ✍️ Author

**Saksham Kapoor**

  * GitHub: [@Saksham932007](https://www.google.com/search?q=https://github.com/Saksham932007)

-----

Licensed under the MIT License.
