using Confluent.Kafka;
using EMC.Queue.Abstractions;

namespace EMC.Queue.Kafka;

public sealed class KafkaQueuePublisher : IQueuePublisher
{
    private readonly IProducer<string, byte[]> _producer;
    public KafkaQueuePublisher(IProducer<string, byte[]> producer, ProducerKafkaQueueOptions options)
    {
        ArgumentNullException.ThrowIfNull(producer);
        ArgumentNullException.ThrowIfNull(options);
        options.ValidateForPublish();
        _producer = producer;
    }

    public async Task<bool> PublishAsync(QueueMessage message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(message.Payload);
        ArgumentException.ThrowIfNullOrWhiteSpace(message.Topic);
        cancellationToken.ThrowIfCancellationRequested();
        var result = await _producer.ProduceAsync(message.Topic, new Message<string, byte[]> { Key = message.Key ?? message.Id, Value = message.Payload },
            cancellationToken).ConfigureAwait(false);

        return result.Status == PersistenceStatus.Persisted;
    }
}
