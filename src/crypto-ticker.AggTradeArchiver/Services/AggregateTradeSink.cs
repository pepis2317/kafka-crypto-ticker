using System.Globalization;
using crypto_ticker.Archiver.Models;
using crypto_ticker.Contracts;
using crypto_ticker.Kafka;
using ParquetSharp;

namespace crypto_ticker.Archiver;

public class AggregateTradeSink:IAggregateTradeSink
{
    private readonly Column[] _columns =
    [
        new Column<string>("Symbol"),
        new Column<long>("AggregateTradeId"),
        new Column<decimal>("Price", LogicalType.Decimal(precision: 29, scale: 8)),
        new Column<decimal>("Quantity", LogicalType.Decimal(precision: 29, scale: 8)),
        new Column<long>("FirstTradeId"),
        new Column<long>("LastTradeId"),
        new Column<DateTime>("TradeTime"),
        new Column<bool>("IsBuyerMarketMaker"),
        new Column<int>("Partition"),
        new Column<long>("Offset")
    ];
    
    private readonly IRustFsService _rustFsService;
    private readonly WriterProperties _props;
    private const string ContentType = "application/vnd.apache.parquet";
    private const string EventType = "aggTrade";

    public AggregateTradeSink(IRustFsService rustFsService)
    {
        _rustFsService = rustFsService;
        _props = new WriterPropertiesBuilder()
            .Compression(Compression.Zstd)
            .CompressionLevel(3)
            .Build();
    }

    public async Task Write(IReadOnlyList<AggregateTradeStream> buffer)
    {
        var groups = buffer.GroupBy(q => (
            Date: DateOnly.FromDateTime(q.TradeTime.UtcDateTime),
            Symbol: q.Symbol));
        
        foreach (var group in groups)
        {
            var rows = group.OrderBy(q => q.AggregateTradeId).ToList();
            var result = await ArchiveBatch(group.Key.Date, group.Key.Symbol, rows);
            if(!result.Success)
            {
                throw new IOException($"Archive upload failed for {group.Key.Symbol} {group.Key.Date}: {result.ErrorMessage}");
            }
        }
    }
    
    private async Task<UploadResult> ArchiveBatch(DateOnly date, string symbol, List<AggregateTradeStream> buffer)
    {
        if (buffer.Count == 0) throw new ArgumentException("Empty batch", nameof(buffer));
        
        var fileName = $"part-{buffer.Min(q=>q.AggregateTradeId)}-{buffer.Max(q=>q.AggregateTradeId)}.parquet";
        var dateString = date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        var key = $"{EventType}/date={dateString}/symbol={symbol}/{fileName}";
        var localPath = Path.Combine(Path.GetTempPath(), fileName);
        
        try
        {
            WriteParquet(localPath, buffer);
            
            await using var stream = new FileStream(
                localPath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: 81920,
                useAsync: true);
            
            return await _rustFsService.UploadFileAsync(BucketNameEnum.Archive, key, stream, ContentType);
        }
        finally
        {
            File.Delete(localPath);
        }
    }
    
    private void WriteParquet(string path, List<AggregateTradeStream> buffer)
    {
        using var file = new ParquetFileWriter(path ,_columns, _props);
        using (var rowGroup = file.AppendRowGroup())
        {
            using (var w = rowGroup.NextColumn().LogicalWriter<string>())
            {
                w.WriteBatch(buffer.Select(q=>q.Symbol).ToArray());
            }
            
            using (var w = rowGroup.NextColumn().LogicalWriter<long>())
            {
                w.WriteBatch(buffer.Select(q => q.AggregateTradeId).ToArray());
            }

            using (var w = rowGroup.NextColumn().LogicalWriter<decimal>())
            {
                w.WriteBatch(buffer.Select(q => q.Price).ToArray());
            }

            using (var w = rowGroup.NextColumn().LogicalWriter<decimal>())
            {
                w.WriteBatch(buffer.Select(q => q.Quantity).ToArray());
            }
            
            using (var w = rowGroup.NextColumn().LogicalWriter<long>())
            {
                w.WriteBatch(buffer.Select(q => q.FirstTradeId).ToArray());
            }
            
            using (var w = rowGroup.NextColumn().LogicalWriter<long>())
            {
                w.WriteBatch(buffer.Select(q => q.LastTradeId).ToArray());
            }

            using (var w = rowGroup.NextColumn().LogicalWriter<DateTime>())
            {
                w.WriteBatch(buffer.Select(q => q.TradeTime.UtcDateTime).ToArray());
            }

            using (var w = rowGroup.NextColumn().LogicalWriter<bool>())
            {
                w.WriteBatch(buffer.Select(q => q.IsBuyerMarketMaker).ToArray());
            }
            
            using (var w = rowGroup.NextColumn().LogicalWriter<int>())
            {
                w.WriteBatch(buffer.Select(q => q.Partition).ToArray());
            }
            
            using (var w = rowGroup.NextColumn().LogicalWriter<long>())
            {
                w.WriteBatch(buffer.Select(q => q.Offset).ToArray());
            }
        }
        file.Close();
    }
}