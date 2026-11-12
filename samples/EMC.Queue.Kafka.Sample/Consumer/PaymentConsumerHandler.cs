using System.Text;
using EMC.Queue.Abstractions;

namespace EMC.Queue.Samples.Consumer;

public sealed class PaymentConsumerHandler : IConsumerHandler
{
    public Task ConsumerHandleAsync(QueueMessage message, CancellationToken cancellationToken)
    {
        var payload = Encoding.UTF8.GetString(message.Payload ?? []);

        Console.WriteLine("Handled payment message {0}, payload {1}", message.Id, payload);
        message.Commited = true;

        return Task.CompletedTask;
    }
}
