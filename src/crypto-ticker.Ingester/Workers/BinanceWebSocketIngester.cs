using System.Buffers;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using crypto_ticker.Ingester.Configs;
using crypto_ticker.Ingester.Services;
using crypto_ticker.Kafka;
using Microsoft.Extensions.Options;

namespace crypto_ticker.Ingester.Workers;

public class BinanceWebSocketIngester: BackgroundService
{
    private readonly IWebSocketFactory _socketFactory;
    private readonly ILogger<BinanceWebSocketIngester> _logger;
    private readonly IProducer<string, string> _producer;
    private readonly KafkaConfiguration _kafkaConfig;
    private readonly WebSocketConfiguration _socketConfig;
    private int _nextId = 0;
    private int _delayCounter = 0;

    public BinanceWebSocketIngester(
        IWebSocketFactory socketFactory,
        IProducerFactory producerFactory,
        ILogger<BinanceWebSocketIngester> logger,
        IOptions<KafkaConfiguration> kafkaOptions,
        IOptions<WebSocketConfiguration> socketOptions)
    {
        _socketFactory = socketFactory;
        _logger = logger;
        _socketConfig = socketOptions.Value;
        _kafkaConfig = kafkaOptions.Value;
        _producer = producerFactory.Create();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while(!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunConnectionAsync(stoppingToken);
                _logger.LogWarning("Socket closed, reconnecting");
            }
            catch(OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch(Exception ex)
            {
                _logger.LogWarning(ex, "Socket failed, reconnecting");
            }
            try
            {
                var waitTime = Math.Min(
                    _socketConfig.BaseDelaySeconds * Math.Pow(_socketConfig.DelayExponent, Interlocked.Increment(ref _delayCounter)), 
                    _socketConfig.MaxDelaySeconds);
                await Task.Delay(TimeSpan.FromSeconds(waitTime), stoppingToken);
            }
            catch(OperationCanceledException)
            {
                break;
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);
        _producer.Flush(TimeSpan.FromSeconds(5));
    }

    public override void Dispose()
    {
        _producer.Dispose();
        base.Dispose();
    }

    private async Task RunConnectionAsync(CancellationToken cancellationToken)
    {
        using var ws = _socketFactory.Create();
        await ws.ConnectAsync(new Uri(_socketConfig.BaseEndpoint), cancellationToken);
        var subscribe = JsonSerializer.Serialize(new
        {
            method = "SUBSCRIBE",
            @params = _socketConfig.Streams,
            id = Interlocked.Increment(ref _nextId)
        });
        
        await ws.SendAsync(
            Encoding.UTF8.GetBytes(subscribe),
            WebSocketMessageType.Text,
            endOfMessage:true, cancellationToken);
        
        var message = new ArrayBufferWriter<byte>();
        var buffer = new Memory<byte>(new byte[8192]);
        
        while(ws.State == WebSocketState.Open)
        {
            message.Clear();
            ValueWebSocketReceiveResult result;
            do
            {
                result = await ws.ReceiveAsync(buffer, cancellationToken);
                    
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, string.Empty, cancellationToken);
                    return;
                }

                message.Write(buffer.Span.Slice(0, result.Count));
            } while (!result.EndOfMessage);

            HandleMessage(message.WrittenMemory);
        }
    }
    
    private void HandleMessage(ReadOnlyMemory<byte>payload)
    {
        using var doc = JsonDocument.Parse(payload);
        var root = doc.RootElement;
        
        if(root.TryGetProperty("id", out var id))
        {
            _logger.LogInformation("Ack for request {Id}", id.GetInt32());
            return;
        }

        if (!root.TryGetProperty("e", out var eventType) || !root.TryGetProperty("s", out var symbol)) return;
        
        if (eventType.GetString() is not ("trade" or "aggTrade")) return;
        
        var message = new Message<string, string>
        {
            Key = symbol.GetString()!,
            Value = Encoding.UTF8.GetString(payload.Span)
        };

        try
        {
            _producer.Produce(_kafkaConfig.Topic, message, OnDelivery);
        }
        catch (ProduceException<string, string> ex) when (ex.Error.Code == ErrorCode.Local_QueueFull)
        {
            _producer.Poll(TimeSpan.FromMilliseconds(100)); 
            _producer.Produce(_kafkaConfig.Topic, message, OnDelivery);
        }
    }
    
    private void OnDelivery(DeliveryReport<string, string> report)
    {
        if(report.Error.IsError)
        {
            _logger.LogError("Kafka delivery failed: {Reason}", report.Error.Reason);
        }
    }
}