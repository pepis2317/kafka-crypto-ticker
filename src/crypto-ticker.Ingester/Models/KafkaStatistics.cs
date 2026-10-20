using System.Text.Json.Serialization;

namespace crypto_ticker.Ingester.Models;

public class KafkaStatistics
{
    public KafkaStatistics(IReadOnlyDictionary<string, TopicData> topics)
    {
        Topics = topics;
    }

    [JsonPropertyName("name")] public string Name { get; set; } = string.Empty;

    [JsonPropertyName("topics")] public IReadOnlyDictionary<string, TopicData> Topics { get; set; }
}