using System.ComponentModel.DataAnnotations;

namespace crypto_ticker.Archiver.Models;

public record PresignedUrlOptions(
    [Required(ErrorMessage = "Key cannot be empty")]
    string Key,
    string? ContentType = null,
    string? BucketName = null,
    double DurationMinutes = 10
);
