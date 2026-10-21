using System.Net.WebSockets;

namespace crypto_ticker.Ingester.Services;

// In this project, this interface is used to wrap around ClientWebSocket
// Reuse this interface in the unit test to mock ClientWebSocket behavior
public interface IWebSocketConnection : IDisposable
{
    WebSocketState State { get; }
    
    public Exception? ConnectionException { get; init; }
    
    Task ConnectAsync(Uri uri, CancellationToken cancellationToken);

    ValueTask SendAsync(
        ReadOnlyMemory<byte> data,
        WebSocketMessageType type, bool endOfMessage,
        CancellationToken cancellationToken);

    ValueTask<ValueWebSocketReceiveResult> ReceiveAsync(Memory<byte> buffer, CancellationToken cancellationToken);
    
    Task CloseAsync(WebSocketCloseStatus status, string? description, CancellationToken cancellationToken);
}
