using Confluent.Kafka;
using EMC.Queue.Abstractions;

namespace EMC.Queue.Kafka;

public sealed class ConsumerKafkaQueueOptions
{
    internal Dictionary<string, Type> HandlerTypes { get; } = new(StringComparer.OrdinalIgnoreCase);

    public required ConsumerConfig Consumer { get; init; }

    public void ValidateForConsume()
    {
        ArgumentNullException.ThrowIfNull(Consumer);
        ArgumentException.ThrowIfNullOrWhiteSpace(Consumer.BootstrapServers);
        ArgumentException.ThrowIfNullOrWhiteSpace(Consumer.GroupId);
    }
}
