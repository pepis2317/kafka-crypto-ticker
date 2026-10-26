using System.Text.Json;
using Confluent.Kafka;
using crypto_ticker.Kafka;
using Microsoft.Extensions.Options;

namespace crypto_ticker.Archiver;

public class ConsumerFactory:IConsumerFactory
{
    private readonly KafkaConfiguration _kafkaConfig;
    private readonly ILogger<ConsumerFactory> _logger;

    public ConsumerFactory(IOptions<KafkaConfiguration>kafkaOptions, ILogger<ConsumerFactory> logger)
    {
        _logger = logger;
        _kafkaConfig = kafkaOptions.Value;
    }

    public IConsumer<string, byte[]> Create()
    {
        var config = new ConsumerConfig()
        {
            BootstrapServers = _kafkaConfig.Brokers,
            GroupId = _kafkaConfig.ConsumerGroup,
            SecurityProtocol = SecurityProtocol.Plaintext,
            EnableAutoOffsetStore = false,
            StatisticsIntervalMs = 5000,
            SessionTimeoutMs = 6000,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnablePartitionEof = true
        };
        var consumer = new ConsumerBuilder<string, byte[]>(config)
            .SetStatisticsHandler((_, kafkaStatistics) => LogKafkaStats(kafkaStatistics))
            .SetErrorHandler((_, e) => LogKafkaError(e)).Build();
        return consumer;
    }
    private void LogKafkaStats(string kafkaStatistics)
    {
        var stats = JsonSerializer.Deserialize<KafkaStatistics>(kafkaStatistics);
        if (stats?.Topics == null || stats.Topics.Count <= 0) return;
        
        foreach (var topic in stats.Topics)
        {
            foreach (var partition in topic.Value.Partitions)
            {
                _logger.LogInformation(
                    "KafkaStats Topic: {Topic} Partition: {Partition} ConsumerLag: {Lag}",
                    topic.Key, partition.Key, partition.Value.ConsumerLag);
            }
        }
    }
    
    private void LogKafkaError(Error error) 
        => _logger.LogError("Kafka error {Code}: {Reason}", error.Code, error.Reason);
}