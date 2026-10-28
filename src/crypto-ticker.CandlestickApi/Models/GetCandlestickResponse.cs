namespace CandlestickApi;

public class GetCandlestickResponse
{
    public List<Candlestick> Candlesticks { get; set; } = new();
    public int Page { get; set; }
    public int PageSize { get; set; }
}

public class Candlestick
{
    public DateTimeOffset TradeTime { get; set; }
    public DateTimeOffset StartTime { get; set; }
    public DateTimeOffset CloseTime { get; set; }
    public string Symbol { get; set; } = string.Empty;
    public decimal Open { get; set; }
    public decimal High { get; set; }
    public decimal Low { get; set; }
    public decimal Close { get; set; }
    public decimal Volume { get; set; }
    public ulong Trades { get; set; }
    public bool IsClosed { get; set; } = false;
}