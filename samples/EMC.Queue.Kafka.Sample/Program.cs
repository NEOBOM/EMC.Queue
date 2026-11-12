using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using EMC.Queue.Kafka;
using EMC.Queue.Samples.Consumer;
using EMC.Queue.Samples.Producer;

var builder = Host.CreateApplicationBuilder(args);
var configuration = builder.Configuration;
var bootstrapServers = configuration["Kafka:BootstrapServers"] ?? "127.0.0.1:9092";
var topic = configuration["Kafka:Topics"]?.Split(",") ?? ["orders"];
var mode = configuration["Mode"] ?? "publish";

if(mode.Equals("publish", StringComparison.OrdinalIgnoreCase))
{
    var producerOptions = new ProducerKafkaQueueOptions
    {
        Producer = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            MessageTimeoutMs = 10000,
            EnableIdempotence = true,
            Acks = Acks.All
        }
};

    builder.Services.AddKafkaQueueProducer(producerOptions);
    builder.Services.AddHostedService<SampleWorker>();
}
else
{
    var consumerOptions = new ConsumerKafkaQueueOptions
    {
        Consumer = new ConsumerConfig
        {
            EnableAutoCommit = false,
            EnableAutoOffsetStore = true,
            BootstrapServers = bootstrapServers,
            GroupId = configuration["Kafka:GroupId"] ?? "EMC-orders-sample",
            AutoOffsetReset = AutoOffsetReset.Earliest
        }
    };

    builder.Services
        .AddKafkaConsumerOptions(consumerOptions)
        .AddConsumerHandler<OrderConsumerHandler>(topic[0])
        .AddConsumerHandler<PaymentConsumerHandler>(topic[1]);
}
await builder.Build().RunAsync();
