using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Confluent.Kafka;
using crypto_ticker.Kafka;
using Microsoft.Extensions.Options;

namespace crypto_ticker.Archiver;

public class TradeArchiver:BackgroundService
{
    private readonly IRustFsService _rustFsService;
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly ILogger<TradeArchiver> _logger;
    private readonly KafkaConfiguration _kafkaConfig;

    public TradeArchiver(
        IRustFsService rustFsService,
        IConsumerFactory consumerFactory, 
        ILogger<TradeArchiver> logger, 
        IOptions<KafkaConfiguration> kafkaOptions)
    {
        _consumer = consumerFactory.Create();
        _logger = logger;
        _rustFsService = rustFsService;
        _kafkaConfig = kafkaOptions.Value;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(_kafkaConfig.Topic);
        _logger.LogInformation("Archive consumer started");
        while(!stoppingToken.IsCancellationRequested)
        {
            try
            {
                ArchiveMessageAsync(stoppingToken);
            }
            catch(OperationCanceledException) when(stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch(ConsumeException ex)
            {
                _logger.LogError(ex, "Archive consumer error. Retrying" );
            }
        }
        _logger.LogInformation("Archive consumer stopped");
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);
        _consumer.Close();
    }

    public override void Dispose()
    {
        _consumer.Dispose();
        base.Dispose();
    }
    
    private void ArchiveTrade(byte[] message, CancellationToken stoppingToken)
    {
        
        
    }

    private void ArchiveMessageAsync(CancellationToken cancellationToken)
    {
        var result = _consumer.Consume(cancellationToken);
        if (result.Message == null) return;
        try
        {
            using var doc = JsonDocument.Parse(result.Message.Value);
            var root = doc.RootElement;
            if (!root.TryGetProperty("e", out var eventType))
            {
                _logger.LogWarning("Missing event type at {Tpo}", result.TopicPartitionOffset);
            }
            else
            {
                switch (eventType.GetString())
                {
                    case "trade":    
                        break;
                    case "aggTrade":
                        break;
                    default:
                        _logger.LogWarning("Unknown event type {Type}", eventType.GetString());
                        break;
                }
            }

        }
        catch(JsonException ex)
        {
            _logger.LogError(ex, "Malformed message at {Tpo}", result.TopicPartitionOffset);
        }
        _consumer.StoreOffset(result);
    }
}