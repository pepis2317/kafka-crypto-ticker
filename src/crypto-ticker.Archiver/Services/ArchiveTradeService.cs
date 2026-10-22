using System.Data;
using System.Text.Json;
using Confluent.Kafka;
using crypto_ticker.Contracts;

namespace crypto_ticker.Archiver;

public class ArchiveTradeService : IArchiveTradeService
{
    private const int MaxBatch = 10000;
    private static readonly TimeSpan MaxAge = TimeSpan.FromSeconds(5);
    
    // private readonly ITradeSink _sink
    private readonly ILogger<ArchiveTradeService> _logger;

    private readonly List<TradeStream> _buffer = new(MaxBatch);
    private readonly Dictionary<string, long> _lastTradeId = new();
    private readonly Dictionary<TopicPartition, Offset> _highestOffsets = new();
    private DateTime _batchStartedUtc = DateTime.UtcNow;

    public ArchiveTradeService(ILogger<ArchiveTradeService> logger)
    {
        _logger = logger;
    }
    
    public void Handle(ConsumeResult<string, byte[]> result)
    {
        TradeStream? trade;
        try
        {
            trade = JsonSerializer.Deserialize<TradeStream>(result.Message.Value);
        }
        catch(JsonException ex)
        {
            _logger.LogWarning(ex, "Bad trade at {Tpo}", result.TopicPartitionOffset);
            Track(result);   // unprocessable, so advance past it
            return;
        }
        if(trade != null && Accept(trade))
        {
            if (_buffer.Count == 0) _batchStartedUtc = DateTime.UtcNow;
            _buffer.Add(trade);
        }

        Track(result);
    }
    
    private bool Accept(TradeStream trade)
    {
        if(_lastTradeId.TryGetValue(trade.Symbol, out var lastTradeId))
        {
            if (trade.TradeId <= lastTradeId) return false; // detected dupes
            if(trade.TradeId > lastTradeId)
            {
                _logger.LogWarning("Gap on {Symbol}: {From} -> {To}", trade.Symbol,lastTradeId, trade.TradeId);
            }
        }
        _lastTradeId[trade.Symbol] = trade.TradeId;
        return true;
    }

    private void Track(ConsumeResult<string, byte[]> result) => _highestOffsets[result.TopicPartition] = result.Offset;
    
    public bool ShouldFlush()
    {
        var exceedMaxBatch = _buffer.Count >= MaxBatch;
        var exceedMaxAge = _buffer.Count > 0 && DateTime.UtcNow -  _batchStartedUtc >= MaxAge;
        return exceedMaxBatch || exceedMaxAge;
    }
    
    public void Flush(IConsumer<string, byte[]> consumer)
    {
        if(_buffer.Count > 0)
        {
            // _sink.Write(_buffer);
            _buffer.Clear();
        }
        
        foreach(var (topicPartition, offset) in _highestOffsets)
        {
            // stores the next message i want to read in memory
            consumer.StoreOffset(new TopicPartitionOffset(topicPartition, offset + 1));
        }
        _highestOffsets.Clear();
    }

}