using System.Reflection;
using CandlestickApi;
using ClickHouse.Driver;
using crypto_ticker.Contracts;
using crypto_ticker.Kafka;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<ClickHouseDbConfiguration>(
    builder.Configuration.GetSection("ClickHouseDbConfiguration"));
builder.Services.Configure<KafkaConfiguration>(
    builder.Configuration.GetSection("KafkaConfiguration"));
builder.Services.Configure<ApiConfiguration>(
    builder.Configuration.GetSection("ApiConfiguration"));
builder.Services.Configure<KafkaConfiguration>(
    builder.Configuration.GetSection("KafkaConfiguration"));
builder.Services.Configure<RedisConfiguration>(
    builder.Configuration.GetSection("RedisConfiguration"));

builder.Services.AddSingleton<IConnectionMultiplexer>(sp =>
{
    var config = sp.GetRequiredService<IOptions<RedisConfiguration>>().Value;

    if (string.IsNullOrWhiteSpace(config.Endpoint))
        throw new InvalidOperationException("RedisConfiguration:Endpoint is missing");

    var options = new ConfigurationOptions
    {
        EndPoints = { config.Endpoint },
        Password = config.Password,
        AbortOnConnectFail = false
    };

    return ConnectionMultiplexer.Connect(options);
});

builder.Services.AddSignalR();
builder.Services.AddHealthChecks();
builder.Services.AddControllers();
builder.Services.AddOpenApi();
builder.Services.AddSwaggerGen();
builder.Services.AddMediatR(cfg => cfg.RegisterServicesFromAssembly(Assembly.GetExecutingAssembly()));
builder.Services.AddSingleton<IRedisService, RedisService>();
builder.Services.AddSingleton<IClickHouseService,ClickHouseService>();
builder.Services.AddSingleton<IConsumerFactory, ConsumerFactory>();
builder.Services.AddHostedService<AggregateTradeConsumer>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
    app.UseSwagger();
    app.UseSwaggerUI(c =>
    {
        c.SwaggerEndpoint("/swagger/v1/swagger.json", "Candlestick API V1");
    });
}

app.MapHealthChecks("/health");
app.UseHttpsRedirection();
app.MapControllers();
app.MapHub<CandlestickHub>("/hubs/candlesticks");

app.Run();