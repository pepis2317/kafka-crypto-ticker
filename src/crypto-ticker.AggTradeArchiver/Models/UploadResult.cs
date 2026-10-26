namespace crypto_ticker.Archiver.Models;

public record UploadResult(bool Success, string? Url = null, string? ErrorMessage = null);