using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using crypto_ticker.Contracts;
using crypto_ticker.Ingester.Services;

namespace tests;

public class FakeWebSocketConnection : IWebSocketConnection
{
    private sealed class Frame
    {
        public byte[] Data { get; init; } = [];
        public WebSocketMessageType Type { get; init; }
        public int Offset { get; set; }
    }

    private readonly CancellationTokenSource? _stopService;
    private readonly Queue<Frame> _incoming = new();
    private readonly List<string> _sent = new();

    public FakeWebSocketConnection(CancellationTokenSource? stopServiceWhenDrained = null)
        => _stopService = stopServiceWhenDrained;

    public Exception? ConnectionException { get; init; }

    public bool CloseWhenQueueEmpty { get; init; } = true;
    
    public WebSocketState State { get; private set; } = WebSocketState.None;
    public bool IsDisposed { get; private set; }
    public IReadOnlyList<string> SentMessages => _sent;
    public WebSocketCloseStatus? ClosedWith { get; private set; }
    
    // Helpers
    public void EnqueueText(string text) =>
        _incoming.Enqueue(new Frame { Data = Encoding.UTF8.GetBytes(text), Type = WebSocketMessageType.Text });
    public void EnqueueClose() =>
        _incoming.Enqueue(new Frame { Type = WebSocketMessageType.Close });
    
    public Task ConnectAsync(Uri uri, CancellationToken cancellationToken)
    {
        if (ConnectionException != null) return Task.FromException(ConnectionException);
        State = WebSocketState.Open;
        return Task.CompletedTask;
    }

    public ValueTask SendAsync(
        ReadOnlyMemory<byte> data,
        WebSocketMessageType type,
        bool endOfMessage,
        CancellationToken cancellationToken)
    {
        var json = Encoding.UTF8.GetString(data.Span);
        _sent.Add(json);

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        if (root.TryGetProperty("method", out var method) && method.GetString() == "SUBSCRIBE")
        {
            var payload = new
            {
                e = "trade",
                E = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                s = "BNBTC",
                t = 12345,
                p = "0.001",
                q = "100",
                T = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                m = true
            };
            EnqueueText(JsonSerializer.Serialize(payload));
        }
        return ValueTask.CompletedTask;
    }

    public async ValueTask<ValueWebSocketReceiveResult> ReceiveAsync(
        Memory<byte> buffer,
        CancellationToken cancellationToken)
    {
        if(_incoming.Count == 0)
        {
            State = WebSocketState.CloseReceived;
            _stopService?.Cancel();
            return new ValueWebSocketReceiveResult(0, WebSocketMessageType.Close, true); 
        }
        var frame = _incoming.Peek();

        if (frame.Type == WebSocketMessageType.Close)
        {
            _incoming.Dequeue();
            State = WebSocketState.CloseReceived;
            return new ValueWebSocketReceiveResult(0, WebSocketMessageType.Close, true);
        }

        // honour the buffer size so multi-chunk messages (endOfMessage = false) get exercised
        var count = Math.Min(buffer.Length, frame.Data.Length - frame.Offset);
        frame.Data.AsMemory(frame.Offset, count).CopyTo(buffer);
        frame.Offset += count;

        var endOfMessage = frame.Offset >= frame.Data.Length;
        if (endOfMessage) _incoming.Dequeue();

        return new ValueWebSocketReceiveResult(count, frame.Type, endOfMessage);
    }

    public Task CloseAsync(WebSocketCloseStatus status, string? description, CancellationToken cancellationToken)
    {
        ClosedWith = status;
        State = WebSocketState.Closed;
        return Task.CompletedTask;
    }
    
    public void Dispose()
    {
        IsDisposed = true;
        State = WebSocketState.Closed;
    }

}