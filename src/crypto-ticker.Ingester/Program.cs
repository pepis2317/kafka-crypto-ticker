using Confluent.Kafka;
using crypto_ticker.Ingester.Configs;
using crypto_ticker.Ingester.Services;
using crypto_ticker.Ingester.Workers;
using crypto_ticker.Kafka;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.Configure<KafkaConfiguration>(
    builder.Configuration.GetSection("KafkaConfiguration"));
builder.Services.Configure <WebSocketConfiguration>(
    builder.Configuration.GetSection("WebSocketConfiguration"));

// Whatever implements IWebSocketFactory uses ClientWebSocketFactory
builder.Services.AddSingleton<IWebSocketFactory, ClientWebSocketFactory>();
builder.Services.AddSingleton<IProducerFactory, ProducerFactory>();

builder.Services.AddHostedService<TopicInitializer>();
builder.Services.AddHostedService<BinanceWebSocketIngester>();
builder.Services.AddHostedService<Consumer>();

var host = builder.Build();
host.Run();
