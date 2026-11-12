using Confluent.Kafka;
using EMC.Queue.Abstractions;
using EMC.Queue.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Xunit;

namespace EMC.Queue.Tests;

public sealed class KafkaTests
{
    private static ProducerKafkaQueueOptions ProducerOptions => new()
    {
        Producer = new() { BootstrapServers = "localhost:9092" }
    };

    private static ConsumerKafkaQueueOptions ConsumerOptions => new()
    {
        Consumer = new() { BootstrapServers = "localhost:9092", GroupId = "tests" }
    };

    [Fact]
    public void AddKafkaQueueProducer_RegistersPublisherWithoutConsumer()
    {
        var services = new ServiceCollection();

        services.AddKafkaQueueProducer(ProducerOptions);

        Assert.Contains(services, descriptor => descriptor.ServiceType == typeof(IQueuePublisher));
        Assert.DoesNotContain(services, descriptor => descriptor.ServiceType == typeof(IQueueConsumer));
        Assert.Contains(services, descriptor => descriptor.ServiceType == typeof(ProducerKafkaQueueOptions));
    }

    [Fact]
    public void AddKafkaConsumerOptions_RegistersConsumerWithoutPublisher()
    {
        var services = new ServiceCollection();

        services.AddKafkaConsumerOptions(ConsumerOptions);

        Assert.Contains(services, descriptor => descriptor.ServiceType == typeof(IQueueConsumer));
        Assert.DoesNotContain(services, descriptor => descriptor.ServiceType == typeof(IQueuePublisher));
        Assert.Contains(services, descriptor => descriptor.ServiceType == typeof(ConsumerKafkaQueueOptions));
    }

    [Fact]
    public void AddConsumerHandler_RegistersHandlerForTopic()
    {
        var options = ConsumerOptions;
        var services = new ServiceCollection().AddKafkaConsumerOptions(options);

        services.AddConsumerHandler<TestHandler>("orders");

        Assert.Contains(services, descriptor => descriptor.ServiceType == typeof(TestHandler));
        Assert.Contains(services, descriptor => descriptor.ServiceType == typeof(IConsumerHandler));
    }

    [Fact]
    public void AddConsumerHandler_RejectsMissingOptionsAndDuplicateTopic()
    {
        Assert.Throws<InvalidOperationException>(() => new ServiceCollection().AddConsumerHandler<TestHandler>("orders"));

        var services = new ServiceCollection().AddKafkaConsumerOptions(ConsumerOptions);
        services.AddConsumerHandler<TestHandler>("orders");

        Assert.Throws<InvalidOperationException>(() => services.AddConsumerHandler<TestHandler>("orders"));
    }

    [Theory]
    [InlineData(null, "id")]
    [InlineData("customer", "customer")]
    public async Task PublishAsync_SendsPayloadAndUsesKeyOrMessageId(string key, string expectedKey)
    {
        var producer = new Mock<IProducer<string, byte[]>>();
        var message = new QueueMessage { Topic = "orders", Payload = [1, 2, 3], Id = "id", Key = key };
        Message<string, byte[]> sent = null;
        producer.Setup(p => p.ProduceAsync("orders", It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()))
            .Callback<string, Message<string, byte[]>, CancellationToken>((_, body, _) => sent = body)
            .ReturnsAsync(new DeliveryResult<string, byte[]> { Status = PersistenceStatus.Persisted });

        var published = await new KafkaQueuePublisher(producer.Object, ProducerOptions).PublishAsync(message);

        Assert.True(published);
        Assert.NotNull(sent);
        Assert.Equal(expectedKey, sent.Key);
        Assert.Equal(message.Payload, sent.Value);
        producer.Verify(p => p.ProduceAsync("orders", It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()), Times.Once());
    }

    [Theory]
    [InlineData(PersistenceStatus.NotPersisted)]
    [InlineData(PersistenceStatus.PossiblyPersisted)]
    public async Task PublishAsync_ReturnsFalseWhenBrokerDoesNotConfirmPersistence(PersistenceStatus status)
    {
        var producer = new Mock<IProducer<string, byte[]>>();
        producer.Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeliveryResult<string, byte[]> { Status = status });

        var published = await new KafkaQueuePublisher(producer.Object, ProducerOptions)
            .PublishAsync(new QueueMessage { Topic = "orders", Payload = [1] });

        Assert.False(published);
    }

    [Fact]
    public async Task PublishAsync_PropagatesBrokerFailure()
    {
        var producer = new Mock<IProducer<string, byte[]>>();
        producer.Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new IOException("offline"));

        await Assert.ThrowsAsync<IOException>(() => new KafkaQueuePublisher(producer.Object, ProducerOptions)
            .PublishAsync(new QueueMessage { Topic = "orders", Payload = [1] }));
    }

    [Fact]
    public async Task PublishAsync_RejectsInvalidInputBeforeCallingProducer()
    {
        var producer = new Mock<IProducer<string, byte[]>>();
        var sut = new KafkaQueuePublisher(producer.Object, ProducerOptions);

        await Assert.ThrowsAsync<ArgumentNullException>(() => sut.PublishAsync(null));
        await Assert.ThrowsAsync<ArgumentNullException>(() => sut.PublishAsync(new QueueMessage { Topic = "orders", Payload = null }));
        await Assert.ThrowsAsync<ArgumentException>(() => sut.PublishAsync(new QueueMessage { Topic = " ", Payload = [1] }));
        producer.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task PublishAsync_ThrowsIfCancellationWasRequested()
    {
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        var producer = new Mock<IProducer<string, byte[]>>();
        var sut = new KafkaQueuePublisher(producer.Object, ProducerOptions);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sut.PublishAsync(
            new QueueMessage { Topic = "orders", Payload = [1] }, cancellation.Token));

        producer.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task ConsumeAsync_DeliversMessageAndCommitsWhenHandlerRequestsIt()
    {
        using var cancellation = new CancellationTokenSource();
        var delivery = CreateDelivery("orders", "customer-1", [4, 5]);
        var client = CreateConsumer(delivery);
        QueueMessage handled = null;
        var handler = new TestHandler((message, _) =>
        {
            handled = message;
            message.Commited = true;
            cancellation.Cancel();
            return Task.CompletedTask;
        });
        var (sut, provider) = CreateSut(client, handler);

        await sut.ConsumeAsync(cancellation.Token);

        Assert.NotNull(handled);
        Assert.Equal("orders", handled.Topic);
        Assert.Equal("customer-1", handled.Id);
        Assert.Equal("customer-1", handled.Key);
        Assert.Equal(new byte[] { 4, 5 }, handled.Payload);
        Assert.True(handled.CreatedAt <= DateTimeOffset.UtcNow);
        VerifySubscribed(client, "orders");
        client.Verify(c => c.Commit(delivery), Times.Once());
        VerifyClosedAndDisposed(client);
        provider.Verify(p => p.GetService(typeof(TestHandler)), Times.Once());
    }

    [Fact]
    public async Task ConsumeAsync_DoesNotCommitUnlessHandlerRequestsIt()
    {
        using var cancellation = new CancellationTokenSource();
        var delivery = CreateDelivery("orders", "customer-1", [1]);
        var client = CreateConsumer(delivery);
        var handler = new TestHandler((_, _) =>
        {
            cancellation.Cancel();
            return Task.CompletedTask;
        });
        var (sut, _) = CreateSut(client, handler);

        await sut.ConsumeAsync(cancellation.Token);

        client.Verify(c => c.Commit(It.IsAny<ConsumeResult<string, byte[]>>()), Times.Never());
        VerifyClosedAndDisposed(client);
    }

    [Fact]
    public async Task ConsumeAsync_PropagatesHandlerFailureAndStillClosesConsumer()
    {
        var client = CreateConsumer(CreateDelivery("orders", "id", [1]));
        var handler = new TestHandler((_, _) => throw new InvalidOperationException("handler failed"));
        var (sut, _) = CreateSut(client, handler);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => sut.ConsumeAsync());

        Assert.Equal("handler failed", error.Message);
        client.Verify(c => c.Commit(It.IsAny<ConsumeResult<string, byte[]>>()), Times.Never());
        VerifyClosedAndDisposed(client);
    }

    [Fact]
    public async Task ConsumeAsync_IgnoresMessageWithoutRegisteredHandler()
    {
        using var cancellation = new CancellationTokenSource();
        var client = CreateConsumer(() =>
        {
            cancellation.Cancel();
            return CreateDelivery("unregistered", "id", [1]);
        });
        var handler = new TestHandler((_, _) => Task.CompletedTask);
        var (sut, provider) = CreateSut(client, handler);

        await sut.ConsumeAsync(cancellation.Token);

        provider.Verify(p => p.GetService(It.IsAny<Type>()), Times.Never());
        client.Verify(c => c.Commit(It.IsAny<ConsumeResult<string, byte[]>>()), Times.Never());
        VerifyClosedAndDisposed(client);
    }

    [Fact]
    public async Task ConsumeAsync_ContinuesAfterConsumeException()
    {
        using var cancellation = new CancellationTokenSource();
        var delivery = CreateDelivery("orders", "id", [1]);
        var client = new Mock<IConsumer<string, byte[]>>();
        client.SetupSequence(c => c.Consume(It.IsAny<CancellationToken>()))
            .Throws(new ConsumeException(new ConsumeResult<byte[], byte[]>(), new Error(ErrorCode.Local_Transport, "offline")))
            .Returns(delivery);
        client.Setup(c => c.Commit(delivery)).Callback(() => cancellation.Cancel());
        var handler = new TestHandler((message, _) =>
        {
            message.Commited = true;
            return Task.CompletedTask;
        });
        var (sut, _) = CreateSut(client, handler);

        await sut.ConsumeAsync(cancellation.Token);

        client.Verify(c => c.Commit(delivery), Times.Once());
        VerifyClosedAndDisposed(client);
    }

    [Fact]
    public async Task ConsumeAsync_AlreadyCancelledDoesNotCreateConsumer()
    {
        var factory = new Mock<Func<IConsumer<string, byte[]>>>();
        var (sut, _) = CreateSut(factory, new TestHandler((_, _) => Task.CompletedTask));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sut.ConsumeAsync(new CancellationToken(canceled: true)));

        factory.Verify(f => f(), Times.Never());
    }

    [Fact]
    public void Constructor_RejectsNullDependencies()
    {
        var options = ConsumerOptions;
        var factory = Mock.Of<Func<IConsumer<string, byte[]>>>();
        var provider = Mock.Of<IServiceProvider>();

        Assert.Throws<ArgumentNullException>(() => new KafkaQueueConsumer(null, options, provider));
        Assert.Throws<ArgumentNullException>(() => new KafkaQueueConsumer(factory, null, provider));
        Assert.Throws<ArgumentNullException>(() => new KafkaQueueConsumer(factory, options, null));
    }

    private static (KafkaQueueConsumer Sut, Mock<IServiceProvider> Provider) CreateSut(
        Mock<IConsumer<string, byte[]>> client,
        TestHandler handler)
    {
        var factory = new Mock<Func<IConsumer<string, byte[]>>>();
        factory.Setup(f => f()).Returns(client.Object);
        return CreateSut(factory, handler);
    }

    private static (KafkaQueueConsumer Sut, Mock<IServiceProvider> Provider) CreateSut(
        Mock<Func<IConsumer<string, byte[]>>> factory,
        TestHandler handler)
    {
        var options = ConsumerOptions;
        new ServiceCollection()
            .AddKafkaConsumerOptions(options)
            .AddConsumerHandler<TestHandler>("orders");

        var provider = new Mock<IServiceProvider>();
        provider.Setup(p => p.GetService(typeof(TestHandler))).Returns(handler);
        return (new KafkaQueueConsumer(factory.Object, options, provider.Object), provider);
    }

    private static Mock<IConsumer<string, byte[]>> CreateConsumer(ConsumeResult<string, byte[]> delivery)
    {
        return CreateConsumer(() => delivery);
    }

    private static Mock<IConsumer<string, byte[]>> CreateConsumer(Func<ConsumeResult<string, byte[]>> consume)
    {
        var client = new Mock<IConsumer<string, byte[]>>();
        client.Setup(c => c.Consume(It.IsAny<CancellationToken>())).Returns(consume);
        return client;
    }

    private static ConsumeResult<string, byte[]> CreateDelivery(string topic, string key, byte[] value) => new()
    {
        Topic = topic,
        Partition = 0,
        Offset = 1,
        Message = new Message<string, byte[]> { Key = key, Value = value }
    };

    private static void VerifySubscribed(Mock<IConsumer<string, byte[]>> client, params string[] topics)
    {
        client.Verify(c => c.Subscribe(It.Is<IEnumerable<string>>(actual => actual.SequenceEqual(topics))), Times.Once());
    }

    private static void VerifyClosedAndDisposed(Mock<IConsumer<string, byte[]>> client)
    {
        client.Verify(c => c.Close(), Times.Once());
        client.Verify(c => c.Dispose(), Times.Once());
    }

    private sealed class TestHandler(Func<QueueMessage, CancellationToken, Task> handle) : IConsumerHandler
    {
        public Task ConsumerHandleAsync(QueueMessage message, CancellationToken cancellationToken) =>
            handle(message, cancellationToken);
    }
}
