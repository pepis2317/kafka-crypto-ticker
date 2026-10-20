using System.Text.Json;
using Confluent.Kafka;
using crypto_ticker.Ingester.Configs;
using crypto_ticker.Ingester.Models;
using Microsoft.Extensions.Options;

namespace crypto_ticker.Ingester.Services;

public class Consumer : BackgroundService
{
    private readonly IConsumer<string, string> _consumer;
    private readonly ILogger<Consumer> _logger;
    private readonly KafkaConfiguration _kafkaConfig;

    public Consumer(ILogger<Consumer> logger, IOptions<KafkaConfiguration> options)
    {
        _logger = logger;
        _kafkaConfig = options.Value;
        
        var config = new ConsumerConfig()
        {
            BootstrapServers = _kafkaConfig.Brokers,
            GroupId = _kafkaConfig.ConsumerGroup,
            SecurityProtocol = SecurityProtocol.Plaintext,
            EnableAutoCommit = false,
            StatisticsIntervalMs = 5000,
            SessionTimeoutMs = 6000,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnablePartitionEof = true
        };

        _consumer = new ConsumerBuilder<string, string>(config)
            .SetStatisticsHandler((_, kafkaStatistics) => LogKafkaStats(kafkaStatistics))
            .SetErrorHandler((_, e) => LogKafkaError(e)).Build();
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) 
        => Task.Run(() => Consume(stoppingToken), stoppingToken);

    private void Consume(CancellationToken cancellationToken)
    {
        _consumer.Subscribe(_kafkaConfig.Topic);
        _logger.LogInformation("Kafka consumer started");
        try
        {
            while(!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumer.Consume(cancellationToken);
                    if (consumeResult?.Message == null) continue;
                    _logger.LogInformation(
                        "[{Key}] {Topic} - {Json}",
                        consumeResult.Message.Key,
                        consumeResult.Topic,
                        consumeResult.Message.Value);
                    _consumer.Commit(consumeResult);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Kafka consume error");
                }
            }
        }
        catch(OperationCanceledException)
        {
            // normal shutdown
        }
        finally
        {
            _consumer.Close();
            _consumer.Dispose();
            _logger.LogInformation("Kafka consumer stopped");
        }
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