using crypto_ticker.Contracts;

namespace crypto_ticker.CandleService;

public class AggregateTradeSinkToDb:IAggregateTradeSink
{
    private readonly IClickHouseService _chService;
    private readonly ILogger<ClickHouseService> _logger;

    public AggregateTradeSinkToDb(IClickHouseService chService, ILogger<ClickHouseService> logger)
    {
        _chService = chService;
        _logger = logger;
    }

    public async Task Write(IReadOnlyList<AggregateTradeStream> buffer)
    {
        var objects = buffer.Select(q => new object[]
        {
            q.Symbol,
            q.AggregateTradeId,
            q.Price,
            q.Quantity,
            q.FirstTradeId,
            q.LastTradeId,
            q.TradeTime,
            q.IsBuyerMarketMaker,
            q.Partition,
            q.Offset
        });
        
        await _chService.BulkInsertToAggTrades(objects);
    }
}