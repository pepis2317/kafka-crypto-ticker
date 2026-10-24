using Confluent.Kafka;
using crypto_ticker.Contracts;

namespace crypto_ticker.Archiver;

public interface IArchiveAggregateTradeService
{
    public void Handle(ConsumeResult<string, byte[]> result);
    public bool ShouldFlush();
    public Task Flush(IConsumer<string, byte[]> consumer);
}