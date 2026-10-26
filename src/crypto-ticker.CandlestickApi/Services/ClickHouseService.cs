using ClickHouse.Driver;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.Utility;
using crypto_ticker.Contracts;
using Microsoft.Extensions.Options;

namespace CandlestickApi;

public class ClickHouseService:IClickHouseService
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

    public async Task<List<Candlestick>> QueryWeeklyCandlesticks(string symbol, long intervalMilli, int page, int pageSize)
    {
        var checkResult = await _client.ExecuteScalarAsync("EXISTS TABLE aggregate_trade_data.aggregate_trades");
        var exists = Convert.ToInt32(checkResult) == 1;
        if (!exists) return [];
        
        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("symbol", symbol);
        parameters.AddParameter("intervalMilli", intervalMilli);
        parameters.AddParameter("pageSize", pageSize);
        parameters.AddParameter("offset", (page - 1) * pageSize);
        
        var query = """
                    SELECT
                        t.symbol,
                        fromUnixTimestamp64Milli(
                            toUnixTimestamp64Milli(t.trade_time) - (toUnixTimestamp64Milli(t.trade_time) % {intervalMilli:Int64}),
                            'UTC'
                        ) AS interval_bucket,
                        argMin(t.price, t.trade_time) AS open,
                        max(t.price) AS high,
                        min(t.price) AS low,
                        argMax(t.price, t.trade_time) AS close,
                        sum(t.quantity) AS volume,
                        count() AS trades
                    FROM aggregate_trade_data.aggregate_trades t
                    WHERE t.symbol = {symbol:String}
                    GROUP BY t.symbol, interval_bucket
                    ORDER BY interval_bucket DESC
                    LIMIT {pageSize:UInt32} OFFSET {offset:UInt32}
                    """;
        var reader = await _client.ExecuteReaderAsync(query, parameters);
        var candlesticks = new List<Candlestick>();
        while(reader.Read())
        {
            var candlestick = new Candlestick
            {
                Symbol = reader.GetString(0),
                IntervalBucket = reader.GetDateTime(1),
                Open = reader.GetDecimal(2),
                High = reader.GetDecimal(3),
                Low = reader.GetDecimal(4),
                Close = reader.GetDecimal(5),
                Volume = reader.GetDecimal(6),
                Trades = reader.GetUInt64(7)
            };
            candlesticks.Add(candlestick);
        }
        return candlesticks;
    }

    public async Task<List<Candlestick>> QueryYearlyOrMonthlyCandlesticks(string symbol, bool isYearly, int page, int pageSize)
    {
        var checkResult = await _client.ExecuteScalarAsync("EXISTS TABLE aggregate_trade_data.aggregate_trades");
        var exists = Convert.ToInt32(checkResult) == 1;
        if (!exists) return [];
        
        var bucketExpr = isYearly
            ? "toStartOfYear(t.trade_time)"
            : "toStartOfMonth(t.trade_time)";
        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("symbol", symbol);
        parameters.AddParameter("pageSize", pageSize);
        parameters.AddParameter("offset", (page - 1) * pageSize);
        
        var query = $$"""
                      SELECT
                          t.symbol,
                          toDateTime({{bucketExpr}}, 'UTC') AS interval_bucket,
                          argMin(t.price, t.trade_time) AS open,
                          max(t.price) AS high,
                          min(t.price) AS low,
                          argMax(t.price, t.trade_time) AS close,
                          sum(t.quantity) AS volume,
                          count() AS trades
                      FROM aggregate_trade_data.aggregate_trades t
                      WHERE t.symbol = {symbol:String}
                      GROUP BY t.symbol, interval_bucket
                      ORDER BY interval_bucket DESC
                      LIMIT {pageSize:UInt32} OFFSET {offset:UInt32}
                      """;
        
        var reader = await _client.ExecuteReaderAsync(query, parameters);
        var candlesticks = new List<Candlestick>();
        while(reader.Read())
        {
            var candlestick = new Candlestick
            {
                Symbol = reader.GetString(0),
                IntervalBucket = reader.GetDateTime(1),
                Open = reader.GetDecimal(2),
                High = reader.GetDecimal(3),
                Low = reader.GetDecimal(4),
                Close = reader.GetDecimal(5),
                Volume = reader.GetDecimal(6),
                Trades = reader.GetUInt64(7)
            };
            candlesticks.Add(candlestick);
        }
        return candlesticks;
    }
}