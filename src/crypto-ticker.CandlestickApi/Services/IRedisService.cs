namespace CandlestickApi;

public interface IRedisService
{
    Task SetAsync<T>(string key, T value, TimeSpan? expiry = null);
    Task<T?> GetAsync<T>(string key);
    Task<bool> DeleteAsync(string key);
    Task<bool> ExistsAsync(string key);
    Task<T> GetOrSetAsync<T>(string key, Func<Task<T>> factory, TimeSpan? expiry = null);
    Task<bool> SetIfNotExistsAsync<T>(string key, T value, TimeSpan? expiry = null);
    Task<long> IncrementAsync(string key, long by = 1, TimeSpan? expiry = null);
    Task<bool> ExpireAsync(string key, TimeSpan expiry);
    Task<TimeSpan?> GetTtlAsync(string key);
}