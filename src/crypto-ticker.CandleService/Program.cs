using crypto_ticker.Kafka;

var builder = Host.CreateApplicationBuilder(args);

var host = builder.Build();
builder.Services.Configure<KafkaConfiguration>(
    builder.Configuration.GetSection("KafkaConfiguration"));
host.Run();