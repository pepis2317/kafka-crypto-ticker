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
                        fromUnixTimestamp64Milli(toUnixTimestamp64Milli(t.trade_time) - (toUnixTimestamp64Milli(t.trade_time) % {intervalMilli:Int64}), 'UTC') as start_time,
                        fromUnixTimestamp64Milli(toUnixTimestamp64Milli(start_time) + {intervalMilli:Int64}, 'UTC') as close_time,
                        argMin(t.price, t.trade_time) AS open,
                        max(t.price) AS high,
                        min(t.price) AS low,
                        argMax(t.price, t.trade_time) AS close,
                        sum(t.quantity) AS volume,
                        count() AS trades
                    FROM aggregate_trade_data.aggregate_trades t
                    WHERE t.symbol = {symbol:String}
                    GROUP BY t.symbol, start_time
                    ORDER BY start_time DESC
                    LIMIT {pageSize:UInt32} OFFSET {offset:UInt32}
                    """;
        var reader = await _client.ExecuteReaderAsync(query, parameters);
        var candlesticks = new List<Candlestick>();
        while(reader.Read())
        {
            var candleSymbol = reader.GetString(0);
            var candleStartTime = reader.GetDateTime(1);
            var candleCloseTime = reader.GetDateTime(2);
            var candleOpen = reader.GetDecimal(3);
            var candleHigh = reader.GetDecimal(4);
            var candleLow = reader.GetDecimal(5);
            var candleClose = reader.GetDecimal(6);
            var candleVolume = reader.GetDecimal(7);
            var candleTrades = reader.GetUInt64(8);
            var candlestick = new Candlestick
            {
                Symbol = candleSymbol,
                StartTime = candleStartTime,
                CloseTime = candleCloseTime,
                Open = candleOpen,
                High = candleHigh,
                Low = candleLow,
                Close = candleClose,
                Volume = candleVolume,
                Trades = candleTrades,
                IsClosed = candleCloseTime < DateTime.UtcNow
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
        
        var openExpr = isYearly
            ? "toStartOfYear(t.trade_time)"
            : "toStartOfMonth(t.trade_time)";
        var closeExpr = isYearly
            ? "addYears(start_time, 1)"
            : "addMonths(start_time, 1)";
        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("symbol", symbol);
        parameters.AddParameter("pageSize", pageSize);
        parameters.AddParameter("offset", (page - 1) * pageSize);
        
        var query = $$"""
                      SELECT
                          t.symbol,
                          toDateTime({{openExpr}}, 'UTC') AS start_time,
                          {{closeExpr}} AS close_time,
                          argMin(t.price, t.trade_time) AS open,
                          max(t.price) AS high,
                          min(t.price) AS low,
                          argMax(t.price, t.trade_time) AS close,
                          sum(t.quantity) AS volume,
                          count() AS trades
                      FROM aggregate_trade_data.aggregate_trades t
                      WHERE t.symbol = {symbol:String}
                      GROUP BY t.symbol, start_time
                      ORDER BY start_time DESC
                      LIMIT {pageSize:UInt32} OFFSET {offset:UInt32}
                      """;
        
        var reader = await _client.ExecuteReaderAsync(query, parameters);
        var candlesticks = new List<Candlestick>();
        while(reader.Read())
        {
            var candleSymbol = reader.GetString(0);
            var candleStartTime = reader.GetDateTime(1);
            var candleCloseTime = reader.GetDateTime(2);
            var candleOpen = reader.GetDecimal(3);
            var candleHigh = reader.GetDecimal(4);
            var candleLow = reader.GetDecimal(5);
            var candleClose = reader.GetDecimal(6);
            var candleVolume = reader.GetDecimal(7);
            var candleTrades = reader.GetUInt64(8);
            var candlestick = new Candlestick
            {
                Symbol = candleSymbol,
                StartTime = candleStartTime,
                CloseTime = candleCloseTime,
                Open = candleOpen,
                High = candleHigh,
                Low = candleLow,
                Close = candleClose,
                Volume = candleVolume,
                Trades = candleTrades,
                IsClosed = candleCloseTime < DateTime.UtcNow
            };
            candlesticks.Add(candlestick);
        }
        return candlesticks;
    }

    public async Task<List<OffsetPerSymbolPartition>> QueryHighestOffsetPerSymbolPartition()
    {
        var checkResult = await _client.ExecuteScalarAsync("EXISTS TABLE aggregate_trade_data.aggregate_trades");
        var exists = Convert.ToInt32(checkResult) == 1;
        if (!exists) return [];

        var query = """
                    SELECT t.symbol, t."partition", MAX(t."offset")
                    FROM aggregate_trade_data.aggregate_trades t
                    GROUP BY t.symbol, t."partition"
                    """;

        var reader = await _client.ExecuteReaderAsync(query);
        var result = new List<OffsetPerSymbolPartition>();
        while(reader.Read())
        {
            var row = new OffsetPerSymbolPartition
            {
                Symbol = reader.GetString(0),
                Partition = reader.GetInt16(1),
                Offset = reader.GetUInt64(2)
            };
            result.Add(row);
        }
        
        return result;
    }
}