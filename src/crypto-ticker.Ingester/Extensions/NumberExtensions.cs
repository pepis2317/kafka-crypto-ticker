namespace crypto_ticker.Ingester.Extensions;

public class NumberExtensions
{
    private static readonly Random Random = new Random();
    public static double RandomNumberBetween(double min, double max)
    {
        var next = Random.NextDouble();
        return min + (next * (max - min));
    }
}