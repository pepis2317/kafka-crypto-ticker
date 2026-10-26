namespace crypto_ticker.Archiver;

public class CompactorConfiguration
{
    public int TaskDelayMinutes { get; set; }
    public List<string> Symbols { get; set; } = [];
    public int CutoffMinutesFromNow { get; set; }
    public long MaxSizeMb { get; set; }
}