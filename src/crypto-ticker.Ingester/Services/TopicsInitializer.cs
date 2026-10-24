using Confluent.Kafka;
using Confluent.Kafka.Admin;
using crypto_ticker.Kafka;
using Microsoft.Extensions.Options;

namespace crypto_ticker.Ingester.Services;

public class TopicsInitializer:IHostedService
{
    private readonly KafkaConfiguration _config;
    private readonly ILogger<TopicsInitializer> _logger;

    public TopicsInitializer(IOptions<KafkaConfiguration> config, ILogger<TopicsInitializer> logger)
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
        
        foreach(var topic in _config.Topics)
        {
            try
            {
                await admin.CreateTopicsAsync([
                    new TopicSpecification
                    {
                        Name = topic,
                        NumPartitions = _config.NumPartitions,
                        ReplicationFactor = (short)_config.ReplicationFactor
                    }
                ]);
                _logger.LogInformation("Created topic {Topic}", topic);
            }
            catch(CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
            {
                _logger.LogInformation("Topic {Topic} already exists", topic);
            }
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}