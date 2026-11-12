namespace EMC.Queue.Abstractions;

public interface IQueuePublisher
{
    Task<bool> PublishAsync(QueueMessage message, CancellationToken cancellationToken = default);
}
