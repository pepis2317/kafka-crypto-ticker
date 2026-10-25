using Confluent.Kafka;

namespace crypto_ticker.Kafka;

public interface IProducerFactory
{
    IProducer<string, byte[]> Create();
}