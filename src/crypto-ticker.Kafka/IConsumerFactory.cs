using Confluent.Kafka;

namespace crypto_ticker.Kafka;

public interface IConsumerFactory
{
    IConsumer<string, byte[]> Create();
}