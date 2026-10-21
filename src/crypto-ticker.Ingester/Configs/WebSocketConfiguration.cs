namespace crypto_ticker.Ingester.Configs;

public class WebSocketConfiguration
{
    public int DelayExponent { get; set; }
    public int BaseDelaySeconds { get; set; }
    public int MaxDelaySeconds { get; set; }
    public string BaseEndpoint{ get; set; } = string.Empty;
    public List<string> Symbols { get; set; } = [];
    public List<string> StreamTypes { get; set; } = [];

    public IEnumerable<string> Streams =>
        Symbols.SelectMany(s => StreamTypes.Select(t => $"{s.ToLowerInvariant()}@{t}"));
}