using Confluent.Kafka;

namespace EMC.Queue.Kafka;

public sealed class ProducerKafkaQueueOptions
{
    public required ProducerConfig Producer { get; init; }

    public void ValidateForPublish()
    {
        ArgumentNullException.ThrowIfNull(Producer);
        ArgumentException.ThrowIfNullOrWhiteSpace(Producer.BootstrapServers);
    }
}
