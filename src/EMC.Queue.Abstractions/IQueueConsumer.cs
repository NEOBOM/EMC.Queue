namespace EMC.Queue.Abstractions;

public interface IQueueConsumer
{
    Task ConsumeAsync(CancellationToken cancellationToken = default);
}
