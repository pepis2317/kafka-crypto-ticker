using System.Text.Json;
using StackExchange.Redis;

namespace CandlestickApi;

public class RedisService : IRedisService
{
    private readonly IDatabase _db;
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public RedisService(IConnectionMultiplexer redis) => _db = redis.GetDatabase();

    public Task SetAsync<T>(string key, T value, TimeSpan? expiry = null) 
        => _db.StringSetAsync(key, JsonSerializer.Serialize(value, JsonOptions), expiry, When.Always);

    public async Task<T?> GetAsync<T>(string key)
    {
        var value = await _db.StringGetAsync(key);
        return value.IsNullOrEmpty ? default : JsonSerializer.Deserialize<T>((string)value!, JsonOptions);
    }

    public Task<bool> DeleteAsync(string key) => _db.KeyDeleteAsync(key);

    public Task<bool> ExistsAsync(string key) => _db.KeyExistsAsync(key);

    public async Task<T> GetOrSetAsync<T>(string key, Func<Task<T>> factory, TimeSpan? expiry = null)
    {
        var cached = await GetAsync<T>(key);
        if (cached is not null) return cached;

        var value = await factory();
        await SetAsync(key, value, expiry);
        return value;
    }

    public Task<bool> SetIfNotExistsAsync<T>(string key, T value, TimeSpan? expiry = null) 
        => _db.StringSetAsync(key, JsonSerializer.Serialize(value, JsonOptions), expiry, When.NotExists);

    public async Task<long> IncrementAsync(string key, long by = 1, TimeSpan? expiry = null)
    {
        var result = await _db.StringIncrementAsync(key, by);
        if (expiry.HasValue && result == by) // first increment, so set the TTL
            await _db.KeyExpireAsync(key, expiry);
        return result;
    }

    public Task<bool> ExpireAsync(string key, TimeSpan expiry) => _db.KeyExpireAsync(key, expiry);

    public Task<TimeSpan?> GetTtlAsync(string key) => _db.KeyTimeToLiveAsync(key);
}