// Program.cs
var db = await KestrelCache.OpenAsync("my-data.db");

Console.WriteLine("Putting data into the database...");
await db.PutAsync("user:1", "{\"name\": \"Alice\", \"email\": \"alice@example.com\"}");
await db.PutAsync("user:2", "{\"name\": \"Bob\", \"email\": \"bob@example.com\"}");
Console.WriteLine("Data saved.");

Console.WriteLine("\nFetching user:1...");
string? user1 = await db.GetAsync("user:1");
Console.WriteLine($"Found user: {user1}");

Console.WriteLine("\nDeleting user:2...");
await db.DeleteAsync("user:2");
string? user2 = await db.GetAsync("user:2");
Console.WriteLine($"Found user 2 after delete: {(user2 is null ? "Not Found" : user2)}");

await db.CloseAsync();
Console.WriteLine("\nDatabase closed.");