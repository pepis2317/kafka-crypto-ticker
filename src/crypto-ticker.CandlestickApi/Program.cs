using System.Reflection;
using CandlestickApi;
using crypto_ticker.Contracts;
using crypto_ticker.Kafka;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<ClickHouseDbConfiguration>(
    builder.Configuration.GetSection("ClickHouseDbConfiguration"));
builder.Services.Configure<KafkaConfiguration>(
    builder.Configuration.GetSection("KafkaConfiguration"));
builder.Services.Configure<ApiConfiguration>(
    builder.Configuration.GetSection("ApiConfiguration"));
builder.Services.Configure<KafkaConfiguration>(
    builder.Configuration.GetSection("KafkaConfiguration"));

builder.Services.AddSignalR();
builder.Services.AddControllers();
builder.Services.AddOpenApi();
builder.Services.AddSwaggerGen();
builder.Services.AddMediatR(cfg => cfg.RegisterServicesFromAssembly(Assembly.GetExecutingAssembly()));
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

app.UseHttpsRedirection();
app.MapControllers();
app.MapHub<CandlestickHub>("/hubs/candlesticks");

app.Run();