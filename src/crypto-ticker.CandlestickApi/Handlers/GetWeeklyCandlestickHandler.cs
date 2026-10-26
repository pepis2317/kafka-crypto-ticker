using MediatR;
using Microsoft.Extensions.Options;

namespace CandlestickApi;

public class GetWeeklyCandlestickHandler : IRequestHandler<GetWeeklyCandlestickRequest, GetCandlestickResponse>
{
    private readonly IClickHouseService _chService;
    private readonly List<string> _symbols;
    private readonly int _maxPageSize;
    
    private static readonly HashSet<long> AllowedIntervals =
    [
        60_000,      // 1m
        180_000,     // 3m
        300_000,     // 5m
        900_000,     // 15m
        1_800_000,   // 30m
        3_600_000,   // 1h
        14_400_000,  // 4h
        86_400_000,  // 1d
        604_800_000  // 1w
    ];

    public GetWeeklyCandlestickHandler(IClickHouseService chService, IOptions<ApiConfiguration> configOptions)
    {
        _chService = chService;
        _symbols = configOptions.Value.Symbols;
        _maxPageSize = configOptions.Value.MaxPageSize;
    }

    public async Task<GetCandlestickResponse> Handle(GetWeeklyCandlestickRequest request, CancellationToken cancellationToken)
    {
        if (!_symbols.Contains(request.Symbol) ||
            !AllowedIntervals.Contains(request.IntervalMilli) ||
            request.Page < 1 ||
            request.PageSize < 1)
        {
            return new GetCandlestickResponse
            {
                Candlesticks = [],
                Page = 0,
                PageSize = 0
            };
        }

        var pageSize = Math.Min(request.PageSize, _maxPageSize);
        var candlesticks = await _chService.QueryWeeklyCandlesticks(request.Symbol, request.IntervalMilli, request.Page, pageSize);
        return new GetCandlestickResponse
        {
            Candlesticks = candlesticks,
            Page = request.Page,
            PageSize = pageSize
        };
    }
}