using System.Text.Json.Serialization;

namespace crypto_ticker.Kafka;

public class PartitionData
{
    [JsonPropertyName("partition")]
    public int Partition { get; set; }

    [JsonPropertyName("consumer_lag")]
    public int ConsumerLag { get; set; }
}