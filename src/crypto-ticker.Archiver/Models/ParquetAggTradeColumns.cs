namespace crypto_ticker.Archiver.Models;

public class ParquetAggTradeColumns
{
    public List<string> Symbols { get; } = [];
    public List<long> TradeIds { get; } = new();
    public List<decimal> Prices { get; } = new();
    public List<decimal> Quantities { get; } = new();
    public List<long> FirstTradeIds { get; } = new();
    public List<long> LastTradeIds { get; } = new();
    public List<DateTime> TradeTimestamps { get; } = new();
    public List<bool> Makers { get; } = new();

    public int Count => TradeIds.Count;
    
    public void Append(ParquetAggTradeColumns other)
    {
        Symbols.AddRange(other.Symbols);
        TradeIds.AddRange(other.TradeIds);
        Prices.AddRange(other.Prices);
        Quantities.AddRange(other.Quantities);
        FirstTradeIds.AddRange(other.FirstTradeIds);
        LastTradeIds.AddRange(other.LastTradeIds);
        TradeTimestamps.AddRange(other.TradeTimestamps);
        Makers.AddRange(other.Makers);
    }
    
    public void SortByTradeId()
    {
        if (IsSortedByTradeId()) return;
        var n = Count;
        var order = new int[n];
        for (int i = 0; i < n; i++) order[i] = i;
        Array.Sort(order, (a,b) =>
        {
            var c = TradeIds[a].CompareTo(TradeIds[b]);
            return c != 0 ? c : a.CompareTo(b);
        });
        
        Reorder(Symbols, order);
        Reorder(TradeIds, order);
        Reorder(Prices, order);
        Reorder(Quantities, order);
        Reorder(FirstTradeIds, order);
        Reorder(LastTradeIds, order);
        Reorder(TradeTimestamps, order);
        Reorder(Makers, order);
    }
    
    private bool IsSortedByTradeId()
    {
        for(int i=1; i< TradeIds.Count; i++)
        {
            if (TradeIds[i] < TradeIds[i - 1]) return false;
        }

        return true;
    }
    
    private static void Reorder<T>(List<T> list, int[] order)
    {
        var copy = list.ToArray();
        for(int i=0; i<order.Length; i++)
        {
            list[i] = copy[order[i]];
        }
    }
}