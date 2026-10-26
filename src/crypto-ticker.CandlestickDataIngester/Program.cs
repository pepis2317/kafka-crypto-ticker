using crypto_ticker.CandleService;
using crypto_ticker.Contracts;
using crypto_ticker.Kafka;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<KafkaConfiguration>(
    builder.Configuration.GetSection("KafkaConfiguration"));
builder.Services.Configure<ArchiverConfiguration>(
    builder.Configuration.GetSection("ArchiverConfiguration"));
builder.Services.Configure<ClickHouseDbConfiguration>(
    builder.Configuration.GetSection("ClickHouseDbConfiguration"));

builder.Services.AddSingleton<IConsumerFactory, ConsumerFactory>();
builder.Services.AddSingleton<IClickHouseService,ClickHouseService>();
builder.Services.AddSingleton<IAggregateTradeSink, AggregateTradeSinkToDb>();
builder.Services.AddSingleton<IArchiveAggregateTradeService, ArchiveAggregateTradeToDb>();
builder.Services.AddHostedService<CandlestickArchiver>();

var host = builder.Build();
host.Run();