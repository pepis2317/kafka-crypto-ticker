using System.Text.Json.Serialization;

namespace crypto_ticker.Kafka;

public class TopicData
{
    public TopicData(IReadOnlyDictionary<string, PartitionData> partitions)
    {
        Partitions = partitions;
    }

    [JsonPropertyName("topic")] public string Topic { get; set; } = string.Empty;

    [JsonPropertyName("partitions")] public IReadOnlyDictionary<string, PartitionData> Partitions { get; set; }
}