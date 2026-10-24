using Confluent.Kafka;
using crypto_ticker.Archiver;

namespace crypto_ticker.CandleService;

public class CandlestickArchiver:BackgroundService
{
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly ILogger<CandlestickArchiver> _logger;
    private const string EventType = "kline";

    public CandlestickArchiver(
        IConsumerFactory consumerFactory, 
        ILogger<CandlestickArchiver> logger)
    {
        _consumer = consumerFactory.Create();
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(EventType);
        _logger.LogInformation("Candlestick archive consumer started");
        while(!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ArchiveMessageAsync(stoppingToken);
            }
            catch(OperationCanceledException) when(stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch(ConsumeException ex)
            {
                _logger.LogError(ex, "Candlestick archive consumer error. Retrying" );
            }
        }
        _logger.LogInformation("Candlestick archive consumer stopped");
    }
    
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);
        _consumer.Close();
    }

    public override void Dispose()
    {
        _consumer.Dispose();
        base.Dispose();
    }
    
    public async Task ArchiveMessageAsync(CancellationToken stoppingToken)
    {
        
    }
}