namespace EMC.Queue.Abstractions;

public interface IConsumerHandler
{
    Task ConsumerHandleAsync(QueueMessage message, CancellationToken cancellationToken);
}
