using MediatR;

namespace CandlestickApi;

public class GetYearlyOrMonthlyCandlestickRequest:IRequest<GetCandlestickResponse>
{
    public string Symbol { get; set; } = string.Empty;
    public bool IsYearly { get; set; } = false;
    public int Page { get; set; } = 1;
    public int PageSize { get; set; } = 100;
}