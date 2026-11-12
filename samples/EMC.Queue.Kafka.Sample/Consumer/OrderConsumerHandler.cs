using EMC.Queue.Abstractions;

namespace EMC.Queue.Samples.Consumer;

public sealed class OrderConsumerHandler : IConsumerHandler
{
    public Task ConsumerHandleAsync(QueueMessage message, CancellationToken cancellationToken)
    {
        var value = System.Text.Encoding.UTF8.GetString(message.Payload ?? []);

        Console.WriteLine("Handled message {0}, payload {1}", message.Id, value);
        message.Commited = true;
        
        return Task.CompletedTask;
    }
}