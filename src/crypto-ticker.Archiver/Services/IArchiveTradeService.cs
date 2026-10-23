using Confluent.Kafka;
using crypto_ticker.Contracts;

namespace crypto_ticker.Archiver;

public interface IArchiveTradeService
{
    public void Handle(ConsumeResult<string, byte[]> result);
    public bool ShouldFlush();
    public void Flush(IConsumer<string, byte[]> consumer);
}