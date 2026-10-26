using System.Text.Json;
using Confluent.Kafka;
using crypto_ticker.Contracts;
using crypto_ticker.Kafka;

namespace crypto_ticker.CandleService;

public class CandlestickArchiver:BackgroundService
{
    private readonly IArchiveAggregateTradeService _archiveAggregateTradeService;
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly ILogger<CandlestickArchiver> _logger;
    private readonly IClickHouseService _chService;
    private const string EventType = "aggTrade";

    public CandlestickArchiver(
        IConsumerFactory consumerFactory, 
        ILogger<CandlestickArchiver> logger, 
        IClickHouseService chService, 
        IArchiveAggregateTradeService archiveAggregateTradeService)
    {
        _consumer = consumerFactory.Create();
        _logger = logger;
        _chService = chService;
        _archiveAggregateTradeService = archiveAggregateTradeService;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(EventType);
        _logger.LogInformation("Candlestick archive consumer started");
        await _chService.CreateAggTradesTableIfNotExists();
        while(!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ArchiveMessageAsync(stoppingToken);
            }
            catch(OperationCanceledException) when(stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch(ConsumeException ex)
            {
                _logger.LogError(ex, "Candlestick archive consumer error. Retrying" );
            }
        }
        _logger.LogInformation("Candlestick archive consumer stopped");
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
    
    public async Task ArchiveMessageAsync(CancellationToken cancellationToken)
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
                if(_archiveAggregateTradeService.ShouldFlush()) await _archiveAggregateTradeService.Flush(_consumer);
                _archiveAggregateTradeService.Handle(result);
            }
        }
        catch(JsonException ex)
        {
            _logger.LogError(ex, "Malformed message at {Tpo}", result.TopicPartitionOffset);
        }
        _consumer.StoreOffset(result);
    }
}