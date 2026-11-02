namespace CandlestickApi;

public interface IClickHouseService
{
    public Task<List<Candlestick>> QueryWeeklyCandlesticks(string symbol, long intervalMilli, long fromMs, long toMs,int page, int pageSize);
    public Task<List<Candlestick>> QueryYearlyOrMonthlyCandlesticks(string symbol, bool isYearly, long fromMs, long toMs);
}