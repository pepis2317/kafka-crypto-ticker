using System.Text.Json;
using System.Text.Json.Serialization;
using crypto_ticker.Ingester.Extensions;

namespace crypto_ticker.Ingester.Models;

public class SampleData
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public double Value { get; set; } = NumberExtensions.RandomNumberBetween(0.00, 999999.999999);
    public DateTime Created { get; set; } = DateTime.UtcNow;
    private static readonly JsonSerializerOptions Options = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public override string ToString() => JsonSerializer.Serialize(this, Options);
}