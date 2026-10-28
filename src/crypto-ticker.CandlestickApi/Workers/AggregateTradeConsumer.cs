using System.Text.Json;
using Confluent.Kafka;
using crypto_ticker.Contracts;
using crypto_ticker.Kafka;
using Microsoft.Extensions.Options;

namespace CandlestickApi;

public class AggregateTradeConsumer : BackgroundService
{
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly IClickHouseService _chService;
    private readonly ILogger<AggregateTradeConsumer> _logger;
    private readonly Dictionary<string, Candlestick> _candleStates = new();
    private readonly Dictionary<string, long> _lastTradeIds = new();
    private const string EventType = "aggTrade";

    public AggregateTradeConsumer(
        IConsumerFactory consumerFactory,
        ILogger<AggregateTradeConsumer> logger,
        IClickHouseService chService)
    {
        _consumer = consumerFactory.Create();
        _logger = logger;
        _chService = chService;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(EventType);
        _logger.LogInformation("AggTrade consumer started");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                ConsumeMessages(stoppingToken);
                if (!_candleStates.TryGetValue("BTCUSDT", out var candleState)) continue;
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

    private void ConsumeMessages(CancellationToken cancellationToken)
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
                UpdateCandleState(result);
            }
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex, "Malformed message at {Tpo}", result.TopicPartitionOffset);
        }

        _consumer.StoreOffset(result);
        _consumer.Commit();
    }

    private void UpdateCandleState(ConsumeResult<string, byte[]> result)
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

        var tradeMs = trade.TradeTime.ToUnixTimeMilliseconds();
        var startMs = tradeMs - (tradeMs % 60000);
        var closeMs = startMs + 60000;
        var isClosed = DateTimeOffset.FromUnixTimeMilliseconds(closeMs) < DateTime.UtcNow;

        if (!_candleStates.TryGetValue(symbol, out var candle))
        {
            _candleStates[symbol] = new Candlestick
            {
                Symbol = symbol,
                StartTime = DateTimeOffset.FromUnixTimeMilliseconds(startMs),
                CloseTime = DateTimeOffset.FromUnixTimeMilliseconds(closeMs),
                Open = trade.Price,
                High = trade.Price,
                Low = trade.Price,
                Close = trade.Price,
                Volume = trade.Quantity,
                Trades = 1,
                IsClosed = isClosed
            };
            _logger.LogInformation($"initialized: {symbol}: {trade.Price}, {trade.Price}, {isClosed}");
            return;
        }
        if (startMs < candle.StartTime.ToUnixTimeMilliseconds())
        {
            _logger.LogWarning("Late trade for {Symbol} at {Time}, ignoring", symbol, trade.TradeTime);
            return;
        }

        if (startMs > candle.StartTime.ToUnixTimeMilliseconds())
        {
            // trade belongs to a newer bucket -> the stored candle is done
            candle.IsClosed = true;
            _logger.LogInformation($"closing: {candle.Symbol}: {candle.High}, {candle.Low}, {candle.IsClosed}");

            candle.StartTime = DateTimeOffset.FromUnixTimeMilliseconds(startMs);
            candle.CloseTime = DateTimeOffset.FromUnixTimeMilliseconds(closeMs);
            candle.Open = trade.Price;
            candle.High = trade.Price;
            candle.Low = trade.Price;
            candle.Close = trade.Price;
            candle.Volume = trade.Quantity;
            candle.Trades = 1;
            candle.IsClosed = isClosed;
            _logger.LogInformation($"reset: {candle.Symbol}: {candle.High}, {candle.Low}, {candle.IsClosed}");
            return;
        }

        candle.High = Math.Max(candle.High, trade.Price);
        candle.Low  = Math.Min(candle.Low, trade.Price);
        candle.Close = trade.Price;
        candle.Volume += trade.Quantity;
        candle.Trades++;
        _logger.LogInformation($"update: {candle.Symbol}: {candle.High}, {candle.Low}, {candle.IsClosed}");
    }
}