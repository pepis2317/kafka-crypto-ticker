using Confluent.Kafka;

namespace crypto_ticker.Contracts;

public interface IArchiveAggregateTradeService
{
    public void Handle(ConsumeResult<string, byte[]> result);
    public bool ShouldFlush();
    public Task Flush(IConsumer<string, byte[]> consumer);
}