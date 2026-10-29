using System.Text.Json;
using Confluent.Kafka;
using crypto_ticker.Contracts;
using crypto_ticker.Kafka;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Options;

namespace CandlestickApi;

public class AggregateTradeConsumer : BackgroundService
{
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly ILogger<AggregateTradeConsumer> _logger;
    private readonly List<IntervalAggregateHelper> _intervals;
    private readonly Dictionary<string, long> _lastTradeIds = new();
    private const string EventType = "aggTrade";

    public AggregateTradeConsumer(IConsumerFactory consumerFactory, ILogger<AggregateTradeConsumer> logger, IHubContext<CandlestickHub> hub)
    {
        _consumer = consumerFactory.Create();
        _logger = logger;
        _intervals =
        [
            new IntervalAggregateHelper(hub, 1),
            new IntervalAggregateHelper(hub, 5),
            new IntervalAggregateHelper(hub, 10),
            new IntervalAggregateHelper(hub, 15),
            new IntervalAggregateHelper(hub, 30),
            new IntervalAggregateHelper(hub, 60)
        ];
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();
        _consumer.Subscribe(EventType);
        _logger.LogInformation("AggTrade consumer started");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ConsumeMessages();
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "AggTrade consumer error. Retrying");
            }
        }

        _logger.LogInformation("AggTrade consumer stopped");
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

    private async Task ConsumeMessages()
    {
        var result = _consumer.Consume(TimeSpan.FromMilliseconds(200));
        if (result?.Message == null) return;
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
                await UpdateCandleState(result);
            }
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex, "Malformed message at {Tpo}", result.TopicPartitionOffset);
        }

        _consumer.StoreOffset(result);
        _consumer.Commit();
    }

    private async Task UpdateCandleState(ConsumeResult<string, byte[]> result)
    {
        AggregateTradeStream? trade;
        try
        {
            trade = JsonSerializer.Deserialize<AggregateTradeStream>(result.Message.Value);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Bad trade at {Tpo}", result.TopicPartitionOffset);
            return;
        }

        if (trade == null) return;

        var symbol = trade.Symbol;

        if (_lastTradeIds.TryGetValue(symbol, out var lastId) && trade.AggregateTradeId <= lastId) return;
        _lastTradeIds[symbol] = trade.AggregateTradeId;

        await Task.WhenAll(_intervals.Select(q=>q.Handle(trade)));
    }
}