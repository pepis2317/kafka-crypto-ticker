using crypto_ticker.Archiver.Models;

namespace crypto_ticker.Archiver;

public interface IRustFsService
{
    Task<bool> CreateBucketAsync(string bucketName);
    Task<bool> BucketExistsAsync(string bucketName);
    Task<bool> DeleteBucketAsync(string bucketName);
    Task<IEnumerable<string>> ListBucketsAsync();

    Task<string> GeneratePresignedUploadUrlAsync(PresignedUrlOptions options);
    Task<UploadResult> UploadFileAsync(string bucketName, string key, Stream fileStream, string contentType);
    Task<Stream> GetFileAsync(string bucketName, string key);
    Task<bool> DeleteFileAsync(string bucketName, string key);
    Task<IEnumerable<string>> ListFilesAsync(string bucketName);
    Task<IEnumerable<string>> ListFilesToCompact(string bucketName, string prefix, DateTime maxDateTime, long maxSize);
    Task<bool> PrefixExistsAsync(string bucketName, string prefix);
}