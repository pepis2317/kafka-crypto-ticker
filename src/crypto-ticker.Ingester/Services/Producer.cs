using Confluent.Kafka;
using crypto_ticker.Ingester.Configs;
using crypto_ticker.Ingester.Enums;
using crypto_ticker.Ingester.Models;
using Microsoft.Extensions.Options;

namespace crypto_ticker.Ingester.Services;

public class Producer: BackgroundService
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<Producer> _logger;
    private readonly KafkaConfiguration _kafkaConfig;

    public Producer(ILogger<Producer> logger, IOptions<KafkaConfiguration> options)
    {
        _logger = logger;
        _kafkaConfig = options.Value;
        
        var config = new ProducerConfig()
        {
            BootstrapServers = _kafkaConfig.Brokers,
            CompressionType = CompressionType.Lz4,
            LingerMs = 10,
            ClientId = "crypto-ticker.ingester",
            SecurityProtocol = SecurityProtocol.Plaintext,
            BatchNumMessages = 500,
            Acks = Acks.All,
        };
        
        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => Task.Run(() => Produce(stoppingToken), stoppingToken);

    private async Task Produce(CancellationToken cancellationToken)
    {
        try
        {
            using(_logger.BeginScope("Kafka app produce sample data"))
            {
                if (cancellationToken.IsCancellationRequested) return;
                for(int i=0; i< 1001; i++)
                {
                    var json = new SampleData().ToString();
                    var msg = new Message<string, string>
                    {
                        Key = MessageKeyEnums.Key,
                        Value = json
                    };
                    await _producer.ProduceAsync(_kafkaConfig.Topic, msg, cancellationToken);
                }
                _logger.LogInformation("Produced all sample messages.");
            }
        }
        catch(OperationCanceledException)
        {
            _logger.LogInformation("Producer cancelled.");
        }
        catch(ProduceException<string, string>ex)
        {
            _logger.LogError(ex,"Kafka produce error.");
        }
        finally
        {
            _producer.Flush(TimeSpan.FromSeconds(10));
            _producer.Dispose();
            _logger.LogInformation("Kafka producer stopped.");
        }
    }
}