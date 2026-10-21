using System.Net.WebSockets;
using Confluent.Kafka;
using crypto_ticker.Ingester.Configs;
using crypto_ticker.Ingester.Services;
using crypto_ticker.Ingester.Workers;
using crypto_ticker.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace tests;

public class IngesterTest
{
    private readonly Mock<IWebSocketFactory> _socketFactory = new();
    private readonly Mock<IProducerFactory> _producerfactory = new();
    
    private readonly KafkaConfiguration _kafkaConfig = new()
    {
        ClientId = "test-client-id",
        NumPartitions = 3,
        ReplicationFactor = 1,
        Brokers = "localhost:5555",
        Topic = "test-topic",
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
            _producerfactory.Object,
            new NullLogger<BinanceWebSocketIngester>(),
            Options.Create(_kafkaConfig),
            Options.Create(_socketConfig));
    }
    
    [Fact]
    public async Task CreatesAppropriateKafkaMessage()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        var ws = new FakeWebSocketConnection(cts);
        _socketFactory.SetupSequence(q => q.Create()).Returns(ws);
        
        var producer = new Mock<IProducer<string, string>>();
        _producerfactory.Setup(q => q.Create()).Returns(producer.Object);

        Message<string, string>? sent = null;
        //TODO: make this readable to my puny mind
        producer.Setup(p => p.Produce(
                It.IsAny<string>(),
                It.IsAny<Message<string, string>>(),
                It.IsAny<Action<DeliveryReport<string, string>>>()))
            .Callback<string, Message<string, string>, Action<DeliveryReport<string, string>>>(
                (topic, message, _) => sent = message);

        var sut = CreateSut();
        await sut.StartAsync(cts.Token);
        await sut.ExecuteTask!;
        await sut.StopAsync(CancellationToken.None);
        
        producer.Verify(p => p.Produce(
                _kafkaConfig.Topic,
                It.IsAny<Message<string, string>>(),
                It.IsAny<Action<DeliveryReport<string, string>>>()), Times.Once);
        
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
        
        var producer = new Mock<IProducer<string, string>>();
        _producerfactory.Setup(q => q.Create()).Returns(producer.Object);
        
        var sut = CreateSut();
        await sut.StartAsync(CancellationToken.None);
        await secondCreate.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await sut.StopAsync(CancellationToken.None);
        
        _socketFactory.Verify(q => q.Create(), Times.AtLeast(2));
    }
}