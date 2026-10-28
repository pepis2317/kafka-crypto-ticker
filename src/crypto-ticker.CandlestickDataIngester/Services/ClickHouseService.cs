using System.Diagnostics.CodeAnalysis;
using ClickHouse.Driver;
using ClickHouse.Driver.Tcp;
using crypto_ticker.Contracts;
using Microsoft.Extensions.Options;

namespace crypto_ticker.CandleService;

public class ClickHouseService: IClickHouseService
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
        "partition",
        "offset"
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

    public async Task CreateAggTradesTableIfNotExists()
    {
        const string sql = """
                           CREATE TABLE IF NOT EXISTS aggregate_trade_data.aggregate_trades
                           (
                               symbol               LowCardinality(String),
                               aggregate_trade_id   UInt64,
                               price                Decimal128(8),
                               quantity             Decimal128(8),
                               first_trade_id       UInt64,
                               last_trade_id        UInt64,
                               trade_time           DateTime64(3, 'UTC'),
                               is_buyer_maker       Bool,
                               partition            UInt16,
                               offset               UInt64
                           )
                           ENGINE = ReplacingMergeTree
                           PARTITION BY toYYYYMM(trade_time)
                           ORDER BY (symbol, trade_time, aggregate_trade_id)
                           """;
        await _client.ExecuteNonQueryAsync(sql);
    }

    public async Task BulkInsertToAggTrades(IEnumerable<object[]> rows)
    {
        var inserted = await _client.InsertBinaryAsync("aggregate_trade_data.aggregate_trades", Columns, rows);
        _logger.LogInformation("Inserted {Count} agg trades", inserted);
    }
}