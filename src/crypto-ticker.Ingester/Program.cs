using crypto_ticker.Ingester.Configs;
using crypto_ticker.Ingester.Services;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.Configure<KafkaConfiguration>(
    builder.Configuration.GetSection("KafkaConfiguration"));
builder.Services.AddHostedService<Producer>();
builder.Services.AddHostedService<Consumer>();

var host = builder.Build();
host.Run();
