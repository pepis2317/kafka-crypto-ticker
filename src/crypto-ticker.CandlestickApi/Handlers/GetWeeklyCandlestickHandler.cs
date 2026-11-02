using MediatR;
using Microsoft.Extensions.Options;

namespace CandlestickApi;

public class GetWeeklyCandlestickHandler : IRequestHandler<GetWeeklyCandlestickRequest, GetCandlestickResponse>
{
    private readonly IClickHouseService _chService;
    private readonly IRedisService _redisService;
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

    public GetWeeklyCandlestickHandler(IClickHouseService chService, IRedisService redisService, IOptions<ApiConfiguration> configOptions)
    {
        _chService = chService;
        _redisService = redisService;
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

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        var pageSize = Math.Min(request.PageSize, _maxPageSize);
        var windowMs = request.IntervalMilli * pageSize;
        var bucket = (nowMs / windowMs) - (request.Page - 1);
        var fromMs = bucket * windowMs;
        var toMs = fromMs + windowMs;

        var isLive = request.Page == 1;
        var ttl = isLive ? TimeSpan.FromSeconds(1) : TimeSpan.FromHours(6);
        
        var key = $"candles:{request.Symbol}:{request.IntervalMilli}:{pageSize}:{bucket}";

        var candles = await _redisService.GetOrSetAsync<List<Candlestick>>(
            key,
            () => _chService.QueryWeeklyCandlesticks(request.Symbol, request.IntervalMilli, fromMs, toMs, request.Page, pageSize),
            ttl
        );
        
        return new GetCandlestickResponse
        {
            Candlesticks = candles,
            Page = request.Page,
            PageSize = pageSize
        };
    }
}