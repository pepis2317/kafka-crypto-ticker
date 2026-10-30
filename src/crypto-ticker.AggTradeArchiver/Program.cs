using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using crypto_ticker.Archiver;
using crypto_ticker.Archiver.Models;
using crypto_ticker.Contracts;
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
    var logger = provider.GetRequiredService<ILoggerFactory>().CreateLogger("S3Setup");

    AmazonS3Config config;

    if (options.Endpoint != null)
    {
        logger.LogInformation("S3: using custom endpoint {Endpoint} (path-style)", options.Endpoint.AbsoluteUri);
        config = new AmazonS3Config
        {
            ServiceURL = options.Endpoint.AbsoluteUri,
            ForcePathStyle = true,
            UseHttp = true,
            AuthenticationRegion = options.Region
        };
    }
    else
    {
        logger.LogInformation("S3: Endpoint is null, using AWS region {Region}", options.Region);
        config = new AmazonS3Config { RegionEndpoint = RegionEndpoint.GetBySystemName(options.Region) };
    }

    logger.LogInformation("S3: access key set = {HasKey}", !string.IsNullOrEmpty(options.AccessKey));

    return !string.IsNullOrEmpty(options.AccessKey)
        ? new AmazonS3Client(new BasicAWSCredentials(options.AccessKey, options.SecretKey), config)
        : new AmazonS3Client(config);
});

builder.Services.AddSingleton<IRustFsService, RustFsService>();
builder.Services.AddSingleton<IAggregateTradeSink, AggregateTradeSink>();
builder.Services.AddSingleton<IArchiveAggregateTradeService, ArchiveAggregateTradeService>();
builder.Services.AddSingleton<IConsumerFactory, ConsumerFactory>();
builder.Services.AddHostedService<AggregateTradeArchiver>();
builder.Services.AddHostedService<AggregrateTradeCompactor>();

var host = builder.Build();
host.Run();
