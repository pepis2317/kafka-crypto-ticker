using System.Net.Mime;
using Amazon.S3;
using Amazon.S3.Model;
using crypto_ticker.Archiver.Models;
using Microsoft.Extensions.Options;

namespace crypto_ticker.Archiver;

public class RustFsService : IRustFsService
{
    private readonly IAmazonS3 _s3Client;
    private readonly ILogger<RustFsService> _logger;
    private readonly string _serviceUrl;

    public RustFsService(IAmazonS3 s3Client, ILogger<RustFsService> logger, IOptions<S3StorageOptions> storageOptions)
    {
        _s3Client = s3Client;
        _logger = logger;
        _serviceUrl = storageOptions.Value.Endpoint?.AbsoluteUri ?? "http://localhost:9000";
    }

    public async Task<bool> CreateBucketAsync(string bucketName)
    {
        var hasBucket = await BucketExistsAsync(bucketName);
        if (hasBucket) return true;
        var request = new PutBucketRequest
        {
            BucketName = bucketName,
            UseClientRegion = false
        };

        await _s3Client.PutBucketAsync(request);
        return true;
    }

    public async Task<bool> BucketExistsAsync(string bucketName)
    {
        var response = await _s3Client.ListBucketsAsync();
        return response.Buckets?.Any(b => b.BucketName == bucketName) ?? false;
    }

    public async Task<bool> DeleteBucketAsync(string bucketName)
    {
        var files = await ListFilesAsync(bucketName);
        var enumerable = files as string[] ?? files.ToArray();
        if(enumerable.Any())
        {
            var deleteObjectsRequest = new DeleteObjectsRequest
            {
                BucketName = bucketName,
                Objects = enumerable.Select(k => new KeyVersion { Key = k }).ToList()
            };

            await _s3Client.DeleteObjectsAsync(deleteObjectsRequest);
        }

        var deleteBucketRequest = new DeleteBucketRequest
        {
            BucketName = bucketName
        };
        
        await _s3Client.DeleteBucketAsync(deleteBucketRequest);
        return true;
    }

    public async Task<IEnumerable<string>> ListBucketsAsync()
    {
        var response = await _s3Client.ListBucketsAsync();
        return response.Buckets?.Select(b => b.BucketName) ?? Enumerable.Empty<string>();
    }

    public async Task<string> GeneratePresignedUploadUrlAsync(PresignedUrlOptions options)
    {
        if(string.IsNullOrWhiteSpace(options.BucketName))
        {
            throw new ArgumentException("BucketName cannot be empty", nameof(options.BucketName));
        }
        
        if(!await BucketExistsAsync(options.BucketName))
        {
            await CreateBucketAsync(options.BucketName);
        }

        var request = new GetPreSignedUrlRequest
        {
            BucketName = options.BucketName,
            Key = options.Key,
            Verb = HttpVerb.PUT,
            Expires = DateTime.UtcNow.AddMinutes(options.DurationMinutes),
            ContentType = options.ContentType ?? MediaTypeNames.Application.Octet,
            Protocol = _serviceUrl.StartsWith("https", StringComparison.OrdinalIgnoreCase)
                ? Protocol.HTTPS
                : Protocol.HTTP
        };

        return await _s3Client.GetPreSignedURLAsync(request);
    }

    public async Task<UploadResult> UploadFileAsync(string bucketName, string key, Stream fileStream, string contentType)
    {
        if(!await BucketExistsAsync(bucketName))
        {
            await CreateBucketAsync(bucketName);
        }

        var request = new PutObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            InputStream = fileStream,
            ContentType = contentType
        };

        await _s3Client.PutObjectAsync(request);
        
        var url = $"{_serviceUrl}/{bucketName}/{key}";
        return new UploadResult(true, url);
    }

    public async Task<Stream> GetFileAsync(string bucketName, string key)
    {
        var request = new GetObjectRequest
        {
            BucketName = bucketName,
            Key = key
        };

        var response = await _s3Client.GetObjectAsync(request);
        return response.ResponseStream;
    }

    public async Task<bool> DeleteFileAsync(string bucketName, string key)
    {
        var request = new DeleteObjectRequest
        {
            BucketName = bucketName,
            Key = key
        };

        var response = await _s3Client.DeleteObjectAsync(request);
        return response.HttpStatusCode == System.Net.HttpStatusCode.NoContent ||
               response.HttpStatusCode == System.Net.HttpStatusCode.OK;
    }

    public async Task<IEnumerable<string>> ListFilesAsync(string bucketName)
    {
        if (!await BucketExistsAsync(bucketName))
        {
            return Enumerable.Empty<string>();
        }

        var request = new ListObjectsV2Request
        {
            BucketName = bucketName
        };

        var response = await _s3Client.ListObjectsV2Async(request);
        return response.S3Objects?.Select(o => o.Key) ?? Enumerable.Empty<string>();
    }
}