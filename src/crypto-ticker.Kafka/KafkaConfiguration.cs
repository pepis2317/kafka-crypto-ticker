namespace crypto_ticker.Kafka;

public class KafkaConfiguration
{
    public string ClientId { get; set; } = string.Empty;
    public int NumPartitions { get; set; }
    public int ReplicationFactor { get; set; }
    public string Brokers { get; set; } = string.Empty;
    public string Topic { get; set; } = string.Empty;
    public string ConsumerGroup { get; set; } = string.Empty;
}