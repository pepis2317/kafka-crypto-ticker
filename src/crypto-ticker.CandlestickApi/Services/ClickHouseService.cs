using ClickHouse.Driver;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.Utility;
using crypto_ticker.Contracts;
using Microsoft.Extensions.Options;

namespace CandlestickApi;

public class ClickHouseService : IClickHouseService
{
    private readonly ClickHouseClient _client;
    private readonly ILogger<ClickHouseService> _logger;

    private static readonly string[] Columns =
    {
        "symbol",
        "aggregate_trade_id",
        "price",
        "quantity",
        "first_trade_id",
        "last_trade_id",
        "trade_time",
        "is_buyer_maker",
    };

    public ClickHouseService(IOptions<ClickHouseDbConfiguration> options, ILogger<ClickHouseService> logger)
    {
        _logger = logger;
        var config = options.Value;
        _client = new ClickHouseClient(
            $"Host={config.Host};" +
            $"Protocol={config.Protocol};" +
            $"Database={config.Database};" +
            $"Port={config.Port};" +
            $"Username={config.Username};" +
            $"Password={config.Password}");
    }

    public async Task<List<Candlestick>> QueryWeeklyCandlesticks(
        string symbol,
        long intervalMilli,
        long fromMs,
        long toMs,
        int page,
        int pageSize)
    {
        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("symbol", symbol);
        parameters.AddParameter("intervalMilli", intervalMilli);
        parameters.AddParameter("fromMs", fromMs);
        parameters.AddParameter("toMs", toMs);

        const string query = """
                             SELECT
                                 t.symbol,
                                 fromUnixTimestamp64Milli(toUnixTimestamp64Milli(t.trade_time) - (toUnixTimestamp64Milli(t.trade_time) % {intervalMilli:Int64}), 'UTC') AS start_time,
                                 fromUnixTimestamp64Milli(toUnixTimestamp64Milli(start_time) + {intervalMilli:Int64}, 'UTC') AS close_time,
                                 argMin(t.price, t.trade_time) AS open,
                                 max(t.price) AS high,
                                 min(t.price) AS low,
                                 argMax(t.price, t.trade_time) AS close,
                                 sum(t.quantity) AS volume,
                                 count() AS trades
                             FROM aggregate_trade_data.aggregate_trades t
                             WHERE t.symbol = {symbol:String}
                               AND t.trade_time >= fromUnixTimestamp64Milli({fromMs:Int64}, 'UTC')
                               AND t.trade_time <  fromUnixTimestamp64Milli({toMs:Int64}, 'UTC')
                             GROUP BY t.symbol, start_time
                             ORDER BY start_time DESC
                             """;

        await using var reader = await _client.ExecuteReaderAsync(query, parameters);
        var candlesticks = new List<Candlestick>();
        while (reader.Read())
        {
            candlesticks.Add(new Candlestick
            {
                Symbol = reader.GetString(0),
                StartTime = reader.GetDateTime(1),
                CloseTime = reader.GetDateTime(2),
                Open = reader.GetDecimal(3),
                High = reader.GetDecimal(4),
                Low = reader.GetDecimal(5),
                Close = reader.GetDecimal(6),
                Volume = reader.GetDecimal(7),
                Trades = reader.GetUInt64(8)
            });
        }

        return candlesticks;
    }

    public async Task<List<Candlestick>> QueryYearlyOrMonthlyCandlesticks(
        string symbol, 
        bool isYearly, 
        long fromMs, 
        long toMs)
    {
        var startExpr = isYearly
            ? "toStartOfYear(t.trade_time, 'UTC')"
            : "toStartOfMonth(t.trade_time, 'UTC')";
        var closeExpr = isYearly
            ? "addYears(start_time, 1)"
            : "addMonths(start_time, 1)";

        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("symbol", symbol);
        parameters.AddParameter("fromMs", fromMs);
        parameters.AddParameter("toMs", toMs);

        var query = $$"""
                      SELECT
                          t.symbol,
                          toDateTime({{startExpr}}, 'UTC') AS start_time,
                          {{closeExpr}} AS close_time,
                          argMin(t.price, t.trade_time) AS open,
                          max(t.price) AS high,
                          min(t.price) AS low,
                          argMax(t.price, t.trade_time) AS close,
                          sum(t.quantity) AS volume,
                          count() AS trades
                      FROM aggregate_trade_data.aggregate_trades t
                      WHERE t.symbol = {symbol:String}
                        AND t.trade_time >= fromUnixTimestamp64Milli({fromMs:Int64}, 'UTC')
                        AND t.trade_time <  fromUnixTimestamp64Milli({toMs:Int64}, 'UTC')
                      GROUP BY t.symbol, start_time
                      ORDER BY start_time DESC
                      """;

        await using var reader = await _client.ExecuteReaderAsync(query, parameters);
        var candlesticks = new List<Candlestick>();
        while (reader.Read())
        {
            candlesticks.Add(new Candlestick
            {
                Symbol = reader.GetString(0),
                StartTime = DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc),
                CloseTime = DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc),
                Open = reader.GetDecimal(3),
                High = reader.GetDecimal(4),
                Low = reader.GetDecimal(5),
                Close = reader.GetDecimal(6),
                Volume = reader.GetDecimal(7),
                Trades = reader.GetUInt64(8)
            });
        }

        return candlesticks;
    }
}