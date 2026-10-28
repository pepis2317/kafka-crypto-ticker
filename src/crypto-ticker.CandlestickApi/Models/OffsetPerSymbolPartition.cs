namespace CandlestickApi;

public class OffsetPerSymbolPartition
{
    public ulong Offset { get; set; }
    public string Symbol { get; set; } = string.Empty;
    public int Partition { get; set; }
}