namespace crypto_ticker.Ingester.Configs;

public class KafkaConfiguration
{
    public string Brokers { get; set; } = string.Empty;
    public string Topic { get; set; } = string.Empty;
    public string ConsumerGroup { get; set; } = string.Empty;
}