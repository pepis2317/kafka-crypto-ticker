using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using crypto_ticker.Archiver;
using crypto_ticker.Archiver.Models;
using crypto_ticker.Kafka;
using Microsoft.Extensions.Options;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.Configure<KafkaConfiguration>(
    builder.Configuration.GetSection("KafkaConfiguration"));
builder.Services.Configure<ArchiverConfiguration>(
    builder.Configuration.GetSection("ArchiverConfiguration"));
builder.Services.Configure<CompactorConfiguration>(
    builder.Configuration.GetSection("CompactorConfiguration"));
builder.Services.Configure<S3StorageOptions>(
    builder.Configuration.GetSection("S3StorageOptions"));

builder.Services.AddSingleton<IAmazonS3>(provider =>
{
    var options = provider.GetRequiredService<IOptions<S3StorageOptions>>().Value;

    var config = options.Endpoint != null
        ? new AmazonS3Config
        {
            ServiceURL = options.Endpoint.AbsoluteUri,
            ForcePathStyle = true,
            UseHttp = true
        }
        : new AmazonS3Config { RegionEndpoint = RegionEndpoint.GetBySystemName(options.Region) };
    
    if(!string.IsNullOrEmpty(options.AccessKey))
    {
        return new AmazonS3Client(
            new BasicAWSCredentials(options.AccessKey, options.SecretKey), 
            config
        );
    }

    return new AmazonS3Client(config);
});

builder.Services.AddSingleton<IRustFsService, RustFsService>();
builder.Services.AddSingleton<IAggregateTradeSink, AggregateTradeSink>();
builder.Services.AddSingleton<IArchiveAggregateTradeService, ArchiveAggregateTradeService>();
builder.Services.AddSingleton<IConsumerFactory, ConsumerFactory>();
builder.Services.AddHostedService<AggregateTradeArchiver>();
builder.Services.AddHostedService<AggregrateTradeCompactor>();

var host = builder.Build();
host.Run();
