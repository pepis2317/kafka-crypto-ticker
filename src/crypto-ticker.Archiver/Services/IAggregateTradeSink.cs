using crypto_ticker.Contracts;

namespace crypto_ticker.Archiver;

public interface IAggregateTradeSink
{
    public Task Write(IReadOnlyList<AggregateTradeStream> buffer);
}