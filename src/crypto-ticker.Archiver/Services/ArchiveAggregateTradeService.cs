using System.Text.Json;
using Confluent.Kafka;
using crypto_ticker.Contracts;

namespace crypto_ticker.Archiver;

public class ArchiveAggregateTradeService : IArchiveAggregateTradeService
{
    private const int MaxBatch = 10000;
    private static readonly TimeSpan MaxAge = TimeSpan.FromSeconds(5);

    private readonly IAggregateTradeSink _sink;
    private readonly ILogger<ArchiveAggregateTradeService> _logger;

    private readonly List<AggregateTradeStream> _buffer = new(MaxBatch);
    private readonly Dictionary<string, long> _lastAggregateTradeId = new();
    private readonly Dictionary<TopicPartition, Offset> _highestOffsets = new();
    private DateTime _batchStartedUtc = DateTime.UtcNow;

    public ArchiveAggregateTradeService(IAggregateTradeSink sink, ILogger<ArchiveAggregateTradeService> logger)
    {
        _sink = sink;
        _logger = logger;
    }
    
    public void Handle(ConsumeResult<string, byte[]> result)
    {
        AggregateTradeStream? aggTrade;
        try
        {
            aggTrade = JsonSerializer.Deserialize<AggregateTradeStream>(result.Message.Value);
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
            if (trade.AggregateTradeId <= lastTradeId) return false; // detected dupes
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
        var exceedMaxBatch = _buffer.Count >= MaxBatch;
        var exceedMaxAge = _buffer.Count > 0 && DateTime.UtcNow -  _batchStartedUtc >= MaxAge;
        return exceedMaxBatch || exceedMaxAge;
    }
    
    public async Task Flush(IConsumer<string, byte[]> consumer)
    {
        if(_buffer.Count > 0)
        {
            await _sink.Write(_buffer);
            _buffer.Clear();
        }
        
        foreach(var (topicPartition, offset) in _highestOffsets)
        {
            // stores the next message to read in memory
            consumer.StoreOffset(new TopicPartitionOffset(topicPartition, offset + 1));
        }
        _highestOffsets.Clear();
    }
}