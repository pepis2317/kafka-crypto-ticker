namespace CandlestickApi;

public interface IClickHouseService
{
    public Task<List<Candlestick>> QueryWeeklyCandlesticks(string symbol, long intervalMilli, int page, int pageSize);
    public Task<List<Candlestick>> QueryYearlyOrMonthlyCandlesticks(string symbol, bool isYearly, int page, int pageSize);
}