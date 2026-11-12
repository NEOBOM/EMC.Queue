using EMC.Queue.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace EMC.Queue.Tests;

public sealed class QueueWSHostedTests
{
    [Fact]
    public async Task ExecuteAsync_PassesStoppingTokenToConsumerAndHandlesShutdownCancellation()
    {
        using var enteredConsumer = new ManualResetEventSlim();
        var consumer = new Mock<IQueueConsumer>();
        consumer.Setup(c => c.ConsumeAsync(It.IsAny<CancellationToken>()))
            .Callback<CancellationToken>(_ => enteredConsumer.Set())
            .Returns<CancellationToken>(token => Task.Delay(Timeout.Infinite, token));
        var logger = new Mock<ILogger<QueueWSHosted>>();
        using var sut = new QueueWSHosted(consumer.Object, logger.Object);

        await sut.StartAsync(CancellationToken.None);
        Assert.True(enteredConsumer.Wait(TimeSpan.FromSeconds(5)));

        await sut.StopAsync(CancellationToken.None);

        consumer.Verify(c => c.ConsumeAsync(It.Is<CancellationToken>(token => token.IsCancellationRequested)), Times.Once());
        logger.Verify(
            l => l.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((state, _) => state.ToString().Contains("Queue WebSocket listener canceled")),
                It.IsAny<OperationCanceledException>(),
                It.IsAny<Func<It.IsAnyType, Exception, string>>()),
            Times.Once());
    }

    [Fact]
    public async Task ExecuteAsync_LogsAndHandlesUnexpectedConsumerFailure()
    {
        var error = new InvalidOperationException("consumer failed");
        var consumer = new Mock<IQueueConsumer>();
        consumer.Setup(c => c.ConsumeAsync(It.IsAny<CancellationToken>())).ThrowsAsync(error);
        var logger = new Mock<ILogger<QueueWSHosted>>();
        using var sut = new QueueWSHosted(consumer.Object, logger.Object);

        await sut.StartAsync(CancellationToken.None);
        await sut.ExecuteTask;

        logger.Verify(
            l => l.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((state, _) => state.ToString().Contains("An error occurred while running the Queue WebSocket listener")),
                error,
                It.IsAny<Func<It.IsAnyType, Exception, string>>()),
            Times.Once());
    }

    [Fact]
    public void Constructor_RejectsNullDependencies()
    {
        var consumer = Mock.Of<IQueueConsumer>();
        var logger = Mock.Of<ILogger<QueueWSHosted>>();

        Assert.Throws<ArgumentNullException>(() => new QueueWSHosted(null, logger));
        Assert.Throws<ArgumentNullException>(() => new QueueWSHosted(consumer, null));
    }
}
