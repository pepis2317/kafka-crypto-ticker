using Confluent.Kafka;

namespace crypto_ticker.Ingester.Services;

public interface IProducerFactory
{
    IProducer<string,string> Create();
}