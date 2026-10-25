
namespace crypto_ticker.Contracts;

public interface IAggregateTradeSink
{
    public Task Write(IReadOnlyList<AggregateTradeStream> buffer);
}