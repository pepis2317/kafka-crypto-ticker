using MediatR;
using Microsoft.Extensions.Options;

namespace CandlestickApi;

public class GetYearlyOrMonthlyCandlestickHandler:IRequestHandler<GetYearlyOrMonthlyCandlestickRequest, GetCandlestickResponse>
{
    private readonly IClickHouseService _chService;
    private readonly List<string> _symbols;
    private readonly int _maxPageSize;
    
    public GetYearlyOrMonthlyCandlestickHandler(IClickHouseService chService, IOptions<ApiConfiguration> configOptions)
    {
        _chService = chService;
        _symbols = configOptions.Value.Symbols;
        _maxPageSize = configOptions.Value.MaxPageSize;
    }
    
    public async Task<GetCandlestickResponse> Handle(GetYearlyOrMonthlyCandlestickRequest request, CancellationToken cancellationToken)
    {
        if(!_symbols.Contains(request.Symbol) || request.Page < 1 || request.PageSize < 1)
        {
            return new GetCandlestickResponse
            {
                Candlesticks = [],
                Page = 0,
                PageSize = 0
            };
        }

        var pageSize = Math.Min(request.PageSize, _maxPageSize);
        var candlesticks = await _chService.QueryYearlyOrMonthlyCandlesticks(request.Symbol, request.IsYearly,  request.Page, pageSize);
        return new GetCandlestickResponse
        {
            Candlesticks = candlesticks,
            Page = request.Page,
            PageSize = pageSize
        };
    }
}