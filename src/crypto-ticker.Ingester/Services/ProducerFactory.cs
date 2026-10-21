using Confluent.Kafka;
using crypto_ticker.Kafka;
using Microsoft.Extensions.Options;

namespace crypto_ticker.Ingester.Services;

public class ProducerFactory:IProducerFactory
{
    private readonly KafkaConfiguration _kafkaConfig;

    public ProducerFactory(IOptions<KafkaConfiguration>kafkaOptions)
    {
        _kafkaConfig = kafkaOptions.Value;
    }

    public IProducer<string, string> Create()
    {
        var config = new ProducerConfig()
        {
            BootstrapServers = _kafkaConfig.Brokers,
            CompressionType = CompressionType.Lz4,
            LingerMs = 10,
            ClientId = _kafkaConfig.ClientId,
            SecurityProtocol = SecurityProtocol.Plaintext,
            BatchNumMessages = 500,
            Acks = Acks.All,
        };
        return new ProducerBuilder<string, string>(config).Build();
    }
}