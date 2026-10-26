using MediatR;

namespace CandlestickApi;

public class GetWeeklyCandlestickRequest:IRequest<GetCandlestickResponse>
{
    public string Symbol { get; set; } = string.Empty;
    public long IntervalMilli { get; set; } = 60_000;
    public int Page { get; set; } = 1;
    public int PageSize { get; set; } = 100;
}