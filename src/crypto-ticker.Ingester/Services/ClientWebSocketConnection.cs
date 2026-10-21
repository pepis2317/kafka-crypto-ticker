using System.Net.WebSockets;

namespace crypto_ticker.Ingester.Services;

public class ClientWebSocketConnection : IWebSocketConnection
{
    private readonly ClientWebSocket _ws = new();
    
    public WebSocketState State => _ws.State;
    
    public Exception? ConnectionException { get; init; }
    
    public void Dispose() => _ws.Dispose();

    public Task ConnectAsync(Uri uri, CancellationToken cancellationToken)
        => _ws.ConnectAsync(uri, cancellationToken);

    public ValueTask SendAsync(
        ReadOnlyMemory<byte> data,
        WebSocketMessageType type,
        bool endOfMessage,
        CancellationToken cancellationToken)
        => _ws.SendAsync(data, type, endOfMessage, cancellationToken);

    public ValueTask<ValueWebSocketReceiveResult> ReceiveAsync(
        Memory<byte> buffer,
        CancellationToken cancellationToken)
        => _ws.ReceiveAsync(buffer, cancellationToken);

    public Task CloseAsync(
        WebSocketCloseStatus status,
        string? description,
        CancellationToken cancellationToken)
        => _ws.CloseAsync(status, description, cancellationToken);
}