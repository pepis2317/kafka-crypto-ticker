using System.Net.WebSockets;
using Confluent.Kafka;
using crypto_ticker.Ingester;
using crypto_ticker.Ingester.Configs;
using crypto_ticker.Ingester.Services;
using crypto_ticker.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace tests;

public class IngesterTest
{
    private readonly Mock<IWebSocketFactory> _socketFactory = new();
    private readonly Mock<IProducerFactory> _producerFactory = new();
    
    private readonly KafkaConfiguration _kafkaConfig = new()
    {
        ClientId = "test-client-id",
        NumPartitions = 3,
        ReplicationFactor = 1,
        Brokers = "localhost:5555",
        Topics = ["test-topic-1","test-topic-2"],
        ConsumerGroup = "test-consumer-group"
    };
    
    private readonly WebSocketConfiguration _socketConfig = new()
    {
        DelayExponent = 1,
        BaseDelaySeconds = 1,
        MaxDelaySeconds = 1,
        BaseEndpoint = "wss://stream.testendpoint.com",
        Symbols = ["symbol"],
        StreamTypes = ["type"]
    };
    
    private BinanceWebSocketIngester CreateSut()
    {
        return new BinanceWebSocketIngester(
            _socketFactory.Object,
            _producerFactory.Object,
            new NullLogger<BinanceWebSocketIngester>(),
            Options.Create(_socketConfig),
            Options.Create(_kafkaConfig));
    }
    
    [Fact]
    public async Task CreatesAppropriateKafkaMessage()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        var ws = new FakeWebSocketConnection(cts);
        _socketFactory.SetupSequence(q => q.Create()).Returns(ws);
        
        var producer = new Mock<IProducer<string, byte[]>>();
        _producerFactory.Setup(q => q.Create()).Returns(producer.Object);

        Message<string, byte[]>? sent = null;
        producer.Setup(p => 
                p.Produce(
                    It.IsAny<string>(), 
                    It.IsAny<Message<string, byte[]>>(), 
                    It.IsAny<Action<DeliveryReport<string, byte[]>>>()))
            .Callback<string, Message<string, byte[]>, 
                Action<DeliveryReport<string, byte[]>>>((topic, message, _) => sent =  message);

        var sut = CreateSut();
        await sut.StartAsync(cts.Token);
        await sut.ExecuteTask!;
        await sut.StopAsync(CancellationToken.None);
        
        producer.Verify(p => p.Produce(
                "test-topic-1",
                It.IsAny<Message<string, byte[]>>(),
                It.IsAny<Action<DeliveryReport<string, byte[]>>>()),
            Times.Once);
        
        Assert.NotNull(sent);
        Assert.Contains("BNBTC", sent.Key);
    }
    
    [Fact]
    public async Task RetriesAfterConnectFailure()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var failingWs = new FakeWebSocketConnection(cts)
        {
            ConnectionException = new WebSocketException("refused")
        };
        var healthyWs = new FakeWebSocketConnection(cts);
        var secondCreate = new TaskCompletionSource(); // Task that gets completed manually
        _socketFactory.SetupSequence(q => q.Create())
            .Returns(failingWs) // First time reate is called, return failingWs
            .Returns(()=>
            {
                secondCreate.TrySetResult();
                return healthyWs;
            });  // Second time Create is called, return healthyWs
        
        var producer = new Mock<IProducer<string, byte[]>>();
        _producerFactory.Setup(q => q.Create()).Returns(producer.Object);
        
        var sut = CreateSut();
        await sut.StartAsync(CancellationToken.None);
        await secondCreate.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await sut.StopAsync(CancellationToken.None);
        
        _socketFactory.Verify(q => q.Create(), Times.AtLeast(2));
    }
}