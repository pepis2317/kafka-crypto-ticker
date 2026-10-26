namespace crypto_ticker.Contracts;

public class ClickHouseDbConfiguration
{
    public string Host { get; set; } = string.Empty;
    public string Database{ get; set; } = string.Empty;
    public string Protocol { get; set; } = string.Empty;
    public string Port { get; set; } = string.Empty;
    public string Username { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;

}