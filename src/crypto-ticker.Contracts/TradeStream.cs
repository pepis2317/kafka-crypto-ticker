using System.Text.Json;
using System.Text.Json.Serialization;

namespace crypto_ticker.Contracts;

public class UnixMillisecondsConverter : JsonConverter<DateTimeOffset>
{
    public override DateTimeOffset Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        => DateTimeOffset.FromUnixTimeMilliseconds(reader.GetInt64());
    public override void Write(Utf8JsonWriter writer, DateTimeOffset value, JsonSerializerOptions options)
        => writer.WriteNumberValue(value.ToUnixTimeMilliseconds());
}

[JsonNumberHandling(JsonNumberHandling.AllowReadingFromString)]
public class TradeStream
{
    [JsonPropertyName("e")] public string EventType { get; set; } = string.Empty;
    
    [JsonPropertyName("E")] 
    [JsonConverter(typeof(UnixMillisecondsConverter))]
    public DateTimeOffset EventTime { get; set; }
    
    [JsonPropertyName("s")] public string Symbol { get; set; } = string.Empty;
    [JsonPropertyName("t")] public long TradeId { get; set; }
    [JsonPropertyName("p")] public decimal Price { get; set; }
    [JsonPropertyName("q")] public decimal Quantity { get; set; }
    
    [JsonPropertyName("T")] 
    [JsonConverter(typeof(UnixMillisecondsConverter))]
    public DateTimeOffset TradeTime { get; set; }
    
    [JsonPropertyName("m")] public bool IsBuyerMarketMaker { get; set; }
    public override string ToString() => JsonSerializer.Serialize(this);
}