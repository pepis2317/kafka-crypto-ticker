using System.Globalization;
using crypto_ticker.Archiver.Models;
using Microsoft.Extensions.Options;
using ParquetSharp;
using ParquetSharp.IO;

namespace crypto_ticker.Archiver;

public class AggregrateTradeCompactor : BackgroundService
{
    private readonly ILogger<AggregateTradeArchiver> _logger;
    private readonly IRustFsService _rustFsService;
    private readonly CompactorConfiguration _config;
    private readonly WriterProperties _props;
    
    private const string ContentType = "application/vnd.apache.parquet";
    private const string EventType = "aggTrade";
    private readonly long _maxFileSize;
    
    private readonly Column[] _columns =
    [
        new Column<string>("Symbol"),
        new Column<long>("AggregateTradeId"),
        new Column<decimal>("Price", LogicalType.Decimal(precision: 29, scale: 8)),
        new Column<decimal>("Quantity", LogicalType.Decimal(precision: 29, scale: 8)),
        new Column<long>("FirstTradeId"),
        new Column<long>("LastTradeId"),
        new Column<DateTime>("TradeTime"),
        new Column<bool>("IsBuyerMarketMaker")
    ];

    public AggregrateTradeCompactor(
        ILogger<AggregateTradeArchiver> logger, 
        IOptions<CompactorConfiguration> config, 
        IRustFsService rustFsService)
    {
        _logger = logger;
        _rustFsService = rustFsService;
        _config = config.Value;
        _maxFileSize = _config.MaxSizeMb * 1024 * 1024;
        _props = new WriterPropertiesBuilder()
            .Compression(Compression.Zstd)
            .CompressionLevel(3)
            .Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Parquet compactor started");
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await CompactFiles(DateTime.UtcNow);
                await CompactFiles(DateTime.UtcNow.AddDays(-1));
            }
            catch(OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch(IOException ex)
            {
                _logger.LogError(ex, "Parquet compactor error");
            }
            await Task.Delay(_config.TaskDelayMinutes * 60 * 1000, stoppingToken);
        }
        _logger.LogInformation("Parquet compactor stopped");
    }
    
    private async Task CompactFiles(DateTime date)
    {
        var dateString = date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        var datePrefix = $"{EventType}/date={dateString}/";
        
        var exists = await _rustFsService.PrefixExistsAsync(BucketNameEnum.Archive, datePrefix);
        if (!exists) return;
        
        var symbolPrefixes = _config.Symbols.Select(q => datePrefix + $"symbol={q}/").ToList();
        foreach(var prefix in symbolPrefixes)
        {
            var cutoff = DateTime.UtcNow.AddMinutes(_config.CutoffMinutesFromNow);
            var files = (await _rustFsService.ListFilesToCompact(
                BucketNameEnum.Archive, prefix, 
                cutoff, _maxFileSize)).ToList();

            var result = await HandleFileMerge(files, prefix);
            if(!result.Success) _logger.LogWarning("Unable to compact files in {p}", prefix);
        }
    }
    
    private async Task<UploadResult> HandleFileMerge(List<string> files, string key)
    {
        long minId = long.MaxValue;
        long maxId = long.MinValue;
        var merged = new ParquetAggTradeColumns();
        var mergedFiles = new List<string>();
            
        foreach(var file in files)
        {
            if(!TryParseIdRange(file, out var fileMinId, out var fileMaxId))
            {
                _logger.LogWarning("Cannot find min id and max id in {f}", file);
                continue;
            }
            minId = Math.Min(fileMinId, minId);
            maxId = Math.Max(fileMaxId, maxId);
            
            await using var fileStream = await _rustFsService.GetFileAsync(BucketNameEnum.Archive, file);
            var parquetColumns = await ReadColumns(fileStream);
            merged.Append(parquetColumns);
            
            mergedFiles.Add(file);
        }

        if (mergedFiles.Count == 0) return new UploadResult(false);
        
        foreach(var file in files)
        {
            var deleteSuccess = await _rustFsService.DeleteFileAsync(BucketNameEnum.Archive, file);
            if(!deleteSuccess)
            {
                _logger.LogWarning("Cannot delete file {f}", file);
            }
        }
        
        merged.SortByTradeId();
        var mergedFileName = $"part-{minId}-{maxId}.parquet";
        var mergedKey = $"{key}{mergedFileName}";
        var localPath = Path.Combine(Path.GetTempPath(), mergedFileName);
            
        try
        {
            WriteMergedParquetFile(localPath, merged);

            await using var stream = new FileStream(
                localPath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: 81920,
                useAsync: true);
            
            var result = await _rustFsService.UploadFileAsync(BucketNameEnum.Archive, mergedKey, stream, ContentType);

            if (!result.Success) return result; // dont delete anything
            
            foreach (var file in mergedFiles)
            {
                if (file == mergedKey) continue; // skip newly merged
                var deleteSuccess = await _rustFsService.DeleteFileAsync(BucketNameEnum.Archive, file);
                if (!deleteSuccess)
                {
                    _logger.LogWarning("Cannot delete file {f}", file);
                }
            }

            return result;
        }
        finally
        {
            File.Delete(localPath);
        }
    }
    
    private async Task<ParquetAggTradeColumns> ReadColumns(Stream stream)
    {
        using var ms = new MemoryStream();
        await stream.CopyToAsync(ms);
        ms.Position = 0;

        using var input = new ManagedRandomAccessFile(ms, leaveOpen: true);
        using var reader = new ParquetFileReader(input);

        var result = new ParquetAggTradeColumns();

        for (int rg = 0; rg < reader.FileMetaData.NumRowGroups; rg++)
        {
            using var rowGroup = reader.RowGroup(rg);
            int n = (int)rowGroup.MetaData.NumRows;

            result.Symbols.AddRange(rowGroup.Column(0).LogicalReader<string>().ReadAll(n));
            result.TradeIds.AddRange(rowGroup.Column(1).LogicalReader<long>().ReadAll(n));
            result.Prices.AddRange(rowGroup.Column(2).LogicalReader<decimal>().ReadAll(n));
            result.Quantities.AddRange(rowGroup.Column(3).LogicalReader<decimal>().ReadAll(n));
            result.FirstTradeIds.AddRange(rowGroup.Column(4).LogicalReader<long>().ReadAll(n));
            result.LastTradeIds.AddRange(rowGroup.Column(5).LogicalReader<long>().ReadAll(n));
            result.TradeTimestamps.AddRange(rowGroup.Column(6).LogicalReader<DateTime>().ReadAll(n));
            result.Makers.AddRange(rowGroup.Column(7).LogicalReader<bool>().ReadAll(n));
        }

        return result;
    }
    
    private void WriteMergedParquetFile(string path, ParquetAggTradeColumns columns)
    {
        using var file = new ParquetFileWriter(path, _columns, _props);
        using (var rowGroup = file.AppendRowGroup())
        {
            using (var w = rowGroup.NextColumn().LogicalWriter<string>())
            {
                w.WriteBatch(columns.Symbols.ToArray());
            }
            
            using (var w = rowGroup.NextColumn().LogicalWriter<long>())
            {
                w.WriteBatch(columns.TradeIds.ToArray());
            }

            using (var w = rowGroup.NextColumn().LogicalWriter<decimal>())
            {
                w.WriteBatch(columns.Prices.ToArray());
            }

            using (var w = rowGroup.NextColumn().LogicalWriter<decimal>())
            {
                w.WriteBatch(columns.Quantities.ToArray());
            }
            
            using (var w = rowGroup.NextColumn().LogicalWriter<long>())
            {
                w.WriteBatch(columns.FirstTradeIds.ToArray());
            }
            
            using (var w = rowGroup.NextColumn().LogicalWriter<long>())
            {
                w.WriteBatch(columns.LastTradeIds.ToArray());
            }

            using (var w = rowGroup.NextColumn().LogicalWriter<DateTime>())
            {
                w.WriteBatch(columns.TradeTimestamps.ToArray());
            }

            using (var w = rowGroup.NextColumn().LogicalWriter<bool>())
            {
                w.WriteBatch(columns.Makers.ToArray());
            }
        }
        file.Close();
    }
    
    
    private bool TryParseIdRange(string key, out long minId, out long maxId)
    {
        minId = maxId = 0;
        var fileName = key[(key.LastIndexOf('/') + 1)..];
        
        if (!fileName.StartsWith("part-") || 
            !fileName.EndsWith(".parquet")) return false;
        
        var range = fileName["part-".Length..^".parquet".Length]; 
        var parts = range.Split('-');

        return parts.Length == 2
               && long.TryParse(parts[0], out minId)
               && long.TryParse(parts[1], out maxId);
    }
}