using System.Text.Json;
using Confluent.Kafka;
using crypto_ticker.Contracts;
using crypto_ticker.Kafka;
using Microsoft.Extensions.Options;

namespace crypto_ticker.CandleService;

public class ArchiveAggregateTradeToDb:IArchiveAggregateTradeService
{
    private readonly IAggregateTradeSink _sink;
    private readonly ILogger<ArchiveAggregateTradeToDb> _logger;

    private readonly int _maxBatch;
    private readonly int _maxAgeMinutes;
    private readonly List<AggregateTradeStream> _buffer;
    private readonly Dictionary<string, long> _lastAggregateTradeId = new();
    private readonly Dictionary<TopicPartition, Offset> _highestOffsets = new();
    private DateTime _batchStartedUtc = DateTime.UtcNow;
    
    public ArchiveAggregateTradeToDb(
        IAggregateTradeSink sink,
        IOptions<ArchiverConfiguration> configOptions,
        ILogger<ArchiveAggregateTradeToDb> logger)
    {
        _sink = sink;
        _logger = logger;
        var config = configOptions.Value;
        _buffer = new(config.MaxBatch);
        _maxBatch = config.MaxBatch;
        _maxAgeMinutes = config.MaxAgeMinutes;
    }
    
    public void Handle(ConsumeResult<string, byte[]> result)
    {
        AggregateTradeStream? aggTrade;
        try
        {
            aggTrade = JsonSerializer.Deserialize<AggregateTradeStream>(result.Message.Value);
            aggTrade?.Partition = result.TopicPartition.Partition.Value;
            aggTrade?.Offset = result.Offset;
        }
        catch(JsonException ex)
        {
            _logger.LogWarning(ex, "Bad trade at {Tpo}", result.TopicPartitionOffset);
            UpdateHighestOffset(result); // skip
            return;
        }
        if(aggTrade != null && Accept(aggTrade))
        {
            if (_buffer.Count == 0) _batchStartedUtc = DateTime.UtcNow;
            _buffer.Add(aggTrade);
        }

        UpdateHighestOffset(result);
    }
    
    private bool Accept(AggregateTradeStream trade)
    {
        if(_lastAggregateTradeId.TryGetValue(trade.Symbol, out var lastTradeId))
        {
            if (trade.AggregateTradeId <= lastTradeId) return false;
            if(trade.AggregateTradeId > lastTradeId + 1)
            {
                _logger.LogWarning("Gap on {Symbol}: {From} -> {To}", trade.Symbol,lastTradeId, trade.AggregateTradeId);
            }
        }
        _lastAggregateTradeId[trade.Symbol] = trade.AggregateTradeId;
        return true;
    }
    
    private void UpdateHighestOffset(ConsumeResult<string, byte[]> result)
        => _highestOffsets[result.TopicPartition] = result.Offset;

    public bool ShouldFlush()
    {
        var exceedMaxBatch = _buffer.Count >= _maxBatch;
        var exceedMaxAge = _buffer.Count > 0 && DateTime.UtcNow -  _batchStartedUtc >= TimeSpan.FromMinutes(_maxAgeMinutes);
        return exceedMaxBatch || exceedMaxAge;
    }

    public async Task Flush(IConsumer<string, byte[]> consumer)
    {
        if(_buffer.Count > 0)
        {
            await _sink.Write(_buffer);
            _buffer.Clear();
        }
        
        if (_highestOffsets.Count == 0) return;
        
        var offsets = _highestOffsets.Select(q => new TopicPartitionOffset(q.Key, q.Value + 1)).ToList();

        consumer.Commit(offsets);
        _highestOffsets.Clear();
    }
}