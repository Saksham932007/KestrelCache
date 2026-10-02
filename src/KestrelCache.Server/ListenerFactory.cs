using System.Net;
using System.Net.Sockets;

namespace KestrelCache.Server;

/// <summary>
/// Creates listening sockets that accept both IPv4 and IPv6 clients.
/// </summary>
/// <remarks>
/// <para>
/// Binding <see cref="IPAddress.Any"/> listens on IPv4 only, which is a quietly broken default
/// on any dual-stack host. <c>localhost</c> resolves to <c>::1</c> before <c>127.0.0.1</c> on
/// every modern system, so <c>curl localhost:9180</c> and <c>redis-cli -h localhost</c> connect
/// over IPv6 and find nothing listening — and inside a container port-forwarder the symptom is
/// worse than a refused connection: the forwarder accepts, fails to reach the IPv4-only
/// listener, and the client sees a connection reset, which reads like a crash rather than a
/// configuration problem. This was exactly how it presented here.
/// </para>
/// <para>
/// The fix is to bind <see cref="IPAddress.IPv6Any"/> with
/// <see cref="Socket.DualMode"/> enabled, which accepts IPv6 connections natively and IPv4 ones
/// as IPv4-mapped addresses, so one socket serves both. A fallback to plain IPv4 covers hosts
/// with IPv6 compiled out, which still exist in minimal container images.
/// </para>
/// </remarks>
internal static class ListenerFactory
{
    /// <summary>Addresses that mean "every interface".</summary>
    private static bool IsWildcard(string address) =>
        string.IsNullOrWhiteSpace(address) || address is "*" or "0.0.0.0" or "::" or "[::]";

    /// <summary>
    /// Binds and starts listening, returning the socket and the port actually bound (which
    /// differs from the request when port 0 was asked for).
    /// </summary>
    internal static (Socket Listener, int Port) Listen(string address, int port, int backlog)
    {
        if (IsWildcard(address))
        {
            if (Socket.OSSupportsIPv6)
            {
                try
                {
                    return Bind(
                        new Socket(AddressFamily.InterNetworkV6, SocketType.Stream, ProtocolType.Tcp)
                        {
                            DualMode = true,
                        },
                        new IPEndPoint(IPAddress.IPv6Any, port),
                        backlog);
                }
                catch (SocketException)
                {
                    // Some kernels and container configurations refuse a dual-mode bind. Falling
                    // back is better than refusing to start.
                }
                catch (NotSupportedException)
                {
                    // Likewise.
                }
            }

            return Bind(
                new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp),
                new IPEndPoint(IPAddress.Any, port),
                backlog);
        }

        var parsed = IPAddress.Parse(address);
        return Bind(
            new Socket(parsed.AddressFamily, SocketType.Stream, ProtocolType.Tcp),
            new IPEndPoint(parsed, port),
            backlog);
    }

    private static (Socket Listener, int Port) Bind(Socket socket, IPEndPoint endpoint, int backlog)
    {
        try
        {
            // Without this, restarting inside the TIME_WAIT window fails to bind, which turns
            // every quick redeploy into a spurious outage.
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);

            socket.Bind(endpoint);
            socket.Listen(backlog);

            return (socket, ((IPEndPoint)socket.LocalEndPoint!).Port);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }
}
