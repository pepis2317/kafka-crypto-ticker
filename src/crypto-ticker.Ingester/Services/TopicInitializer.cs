using Confluent.Kafka;
using Confluent.Kafka.Admin;
using crypto_ticker.Kafka;
using Microsoft.Extensions.Options;

namespace crypto_ticker.Ingester.Services;

public class TopicInitializer:IHostedService
{
    private readonly KafkaConfiguration _config;
    private readonly ILogger<TopicInitializer> _logger;

    public TopicInitializer(IOptions<KafkaConfiguration> config, ILogger<TopicInitializer> logger)
    {
        _config = config.Value;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = _config.Brokers
        }).Build();
        
        try
        {
            await admin.CreateTopicsAsync([
                new TopicSpecification
                {
                    Name = _config.Topic,
                    NumPartitions = _config.NumPartitions,
                    ReplicationFactor = (short)_config.ReplicationFactor
                }
            ]);
            _logger.LogInformation("Created topic {Topic}", _config.Topic);
        }
        catch(CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            _logger.LogInformation("Topic {Topic} already exists", _config.Topic);
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}