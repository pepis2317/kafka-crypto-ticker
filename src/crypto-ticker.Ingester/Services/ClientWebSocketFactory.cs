namespace crypto_ticker.Ingester.Services;

// For use in Program.cs to feed the real ClientWebSocket into the ingester worker
public class ClientWebSocketFactory : IWebSocketFactory
{
    public IWebSocketConnection Create() => new ClientWebSocketConnection();
}