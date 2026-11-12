using Confluent.Kafka;
using EMC.Queue.Kafka;
using Xunit;

namespace EMC.Queue.Tests;

public sealed class KafkaQueueOptionsTests
{
    [Fact]
    public void ProducerOptions_ValidateForPublish()
    {
        var options = new ProducerKafkaQueueOptions
        {
            Producer = new ProducerConfig { BootstrapServers = "localhost:9092" }
        };

        options.ValidateForPublish();
    }

    [Fact]
    public void ConsumerOptions_ValidateForConsume()
    {
        var options = new ConsumerKafkaQueueOptions
        {
            Consumer = new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "workers" }
        };

        options.ValidateForConsume();
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void ModeValidation_RejectsEmptyConnectionSettings(string value)
    {
        var producer = new ProducerKafkaQueueOptions
        {
            Producer = new ProducerConfig { BootstrapServers = value }
        };
        var consumer = new ConsumerKafkaQueueOptions
        {
            Consumer = new ConsumerConfig { BootstrapServers = value, GroupId = "workers" }
        };

        Assert.Equal("Producer.BootstrapServers", Assert.Throws<ArgumentException>(() => producer.ValidateForPublish()).ParamName);
        Assert.Equal("Consumer.BootstrapServers", Assert.Throws<ArgumentException>(() => consumer.ValidateForConsume()).ParamName);
        consumer.Consumer.BootstrapServers = "localhost:9092";
        consumer.Consumer.GroupId = value;
        Assert.Equal("Consumer.GroupId", Assert.Throws<ArgumentException>(() => consumer.ValidateForConsume()).ParamName);
    }
}
