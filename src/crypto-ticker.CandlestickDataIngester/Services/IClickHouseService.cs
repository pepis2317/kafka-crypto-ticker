namespace crypto_ticker.CandleService;

public interface IClickHouseService
{
    public Task CreateAggTradesTableIfNotExists();
    public Task BulkInsertToAggTrades(IEnumerable<object[]> rows);
}