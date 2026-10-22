using Confluent.Kafka;

namespace crypto_ticker.Archiver;

public interface IConsumerFactory
{
    IConsumer<string, byte[]> Create();
}