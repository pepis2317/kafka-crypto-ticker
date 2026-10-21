namespace crypto_ticker.Ingester.Services;

public interface IWebSocketFactory
{
    IWebSocketConnection Create();
}