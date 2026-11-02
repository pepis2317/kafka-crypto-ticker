using MediatR;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace CandlestickApi;

public class GetYearlyOrMonthlyCandlestickHandler:IRequestHandler<GetYearlyOrMonthlyCandlestickRequest, GetCandlestickResponse>
{
    private readonly IRedisService  _redisService;
    private readonly IClickHouseService _chService;
    private readonly List<string> _symbols;
    private readonly int _maxPageSize;
    
    public GetYearlyOrMonthlyCandlestickHandler(IRedisService redisService, IClickHouseService chService, IOptions<ApiConfiguration> configOptions)
    {
        _redisService = redisService;
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
        var now = DateTime.UtcNow;

        var periodMonths = request.IsYearly ? 12 : 1;
        var nowMonthIdx = now.Year * 12 + (now.Month - 1);
        var currentPeriod = nowMonthIdx / periodMonths;

        var bucket = (currentPeriod / pageSize) - (request.Page - 1);
        if( bucket < 0 )
        {
            return new GetCandlestickResponse
            {
                Candlesticks = [],
                Page = 0,
                PageSize = 0
            };
        }

        var fromPeriod = bucket * pageSize;
        var toPeriod = fromPeriod + pageSize;

        var from = MonthIdxToDate(fromPeriod * periodMonths);
        var to = MonthIdxToDate(toPeriod * periodMonths);

        var isLive = request.Page == 1;
        var ttl = isLive ? TimeSpan.FromSeconds(1) : TimeSpan.FromHours(6);

        var key = $"candles:{request.Symbol}:{(request.IsYearly ? "1y" : "1M")}:{pageSize}:{bucket}";

        var candles = await _redisService.GetOrSetAsync(
            key,
            () => _chService.QueryYearlyOrMonthlyCandlesticks(
                request.Symbol,
                request.IsYearly,
                new DateTimeOffset(from).ToUnixTimeMilliseconds(),
                new DateTimeOffset(to).ToUnixTimeMilliseconds()),
            ttl);
        
        return new GetCandlestickResponse
        {
            Candlesticks = candles,
            Page = request.Page,
            PageSize = pageSize
        };
    }
    private static DateTime MonthIdxToDate(int monthIdx) =>
        new(monthIdx / 12, monthIdx % 12 + 1, 1, 0, 0, 0, DateTimeKind.Utc);
}