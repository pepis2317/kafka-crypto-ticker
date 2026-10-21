using System.Text.Json;
using System.Text.Json.Serialization;

namespace crypto_ticker.Contracts;

[JsonNumberHandling(JsonNumberHandling.AllowReadingFromString)]
public class AggregateTradeStream
{
    [JsonPropertyName("e")] public string EventType { get; set; } = string.Empty;
    
    [JsonPropertyName("E")] 
    [JsonConverter(typeof(UnixMillisecondsConverter))]
    public DateTimeOffset EventTime { get; set; }
    
    [JsonPropertyName("s")] public string Symbol { get; set; } = string.Empty;
    [JsonPropertyName("a")] public long AggregateTradeId { get; set; }
    [JsonPropertyName("p")] public decimal Price { get; set; }
    [JsonPropertyName("q")] public decimal Quantity { get; set; }
    [JsonPropertyName("f")] public long FirstTradeId { get; set; }
    [JsonPropertyName("l")] public long LastTradeId { get; set; }
    
    [JsonPropertyName("T")] 
    [JsonConverter(typeof(UnixMillisecondsConverter))]
    public DateTimeOffset TradeTime { get; set; }
    
    [JsonPropertyName("m")] public bool IsBuyerMarketMaker { get; set; }
    public override string ToString() => JsonSerializer.Serialize(this);
}