# EMC.Queue

A .NET 10 library for publishing and consuming Kafka messages as bytes, with broker-independent interfaces and .NET Generic Host integration.

## Projects

| Project | Responsibility |
| --- | --- |
| `src/EMC.Queue.Abstractions` | `QueueMessage`, publisher and consumer interfaces, handlers, and the hosted service. |
| `src/EMC.Queue.Kafka` | Kafka adapter based on Confluent.Kafka and dependency injection registration extensions. |
| `samples/EMC.Queue.Kafka.Sample` | Console application for publishing one message or running a consumer. |
| `tests/EMC.Queue.Tests` | Unit tests for the abstractions, adapter, and registrations; tests use mocks and do not need a broker. |

All projects target `net10.0`, defined in `Directory.Build.props`. The expected SDK is .NET 10.0.100 or a compatible later version, as specified by `global.json`.

## Build and run tests

With the .NET 10 SDK installed:

```sh
dotnet restore EMC.Queue.sln
dotnet build EMC.Queue.sln
dotnet test EMC.Queue.sln
```

The test suite does not require Kafka to be running.

## Run the sample

The sample connects to an existing Kafka broker. The default address is `127.0.0.1:9092`; create the `orders` topic on the broker first.

Publish one message:

```sh
dotnet run --project samples/EMC.Queue.Kafka.Sample -- --Mode=publish
```

The publisher sends one message with key `001` and UTF-8 payload `001|99.50|BRL`, then exits. The sample publisher currently uses the fixed topic `orders`.

Run the consumer in another terminal:

```sh
dotnet run --project samples/EMC.Queue.Kafka.Sample -- --Mode=consume
```

The consumer uses the handler registered for `orders` and runs until Ctrl+C. The sample handler prints the key and payload, then marks the message for acknowledgement.

The sample accepts command-line arguments and environment variables through .NET configuration:

| Setting | Default | Purpose |
| --- | --- | --- |
| `Mode` | `publish` | `publish` sends one message. Any other value starts the consumer. |
| `Kafka:BootstrapServers` | `127.0.0.1:9092` | Broker address. Use `Kafka__BootstrapServers` as an environment variable. |
| `Kafka:Topic` | `orders` | Topic registered by the sample consumer. The sample publisher topic is fixed to `orders`. |
| `Kafka:GroupId` | `EMC-orders-sample` | Consumer group. |
| `RunSeconds` | No timeout | Sets a positive timeout in seconds for the sample publish operation. |

Example using PowerShell environment variables:

```powershell
$env:Kafka__BootstrapServers = 'localhost:9092'
$env:Kafka__Topic = 'orders'
$env:Kafka__GroupId = 'orders-worker'
dotnet run --project samples/EMC.Queue.Kafka.Sample -- --Mode=consume
```

## Use the library

Reference `EMC.Queue.Abstractions` and `EMC.Queue.Kafka`. Configure the Confluent options and register the required mode. The consumer also needs a handler registered for its topic; consumer registration adds `QueueWSHosted` to the Generic Host.

```csharp
using Confluent.Kafka;
using EMC.Queue.Kafka;

services.AddKafkaQueueProducer(new ProducerKafkaQueueOptions
{
    Producer = new ProducerConfig
    {
        BootstrapServers = "localhost:9092",
        EnableIdempotence = true,
        Acks = Acks.All
    }
});

services.AddKafkaConsumerOptions(new ConsumerKafkaQueueOptions
{
    Consumer = new ConsumerConfig
    {
        BootstrapServers = "localhost:9092",
        GroupId = "orders-worker",
        EnableAutoCommit = false,
        AutoOffsetReset = AutoOffsetReset.Earliest
    }
});
services.AddConsumerHandler<OrderHandler>("orders");
```

Implement `IConsumerHandler` and set `Commited = true` after successful processing to acknowledge the offset:

```csharp
using System.Text;
using EMC.Queue.Abstractions;

public sealed class OrderHandler : IConsumerHandler
{
    public Task ConsumerHandleAsync(QueueMessage message, CancellationToken cancellationToken)
    {
        var payload = Encoding.UTF8.GetString(message.Payload ?? []);
        // Process the message using payload and cancellationToken.
        Console.WriteLine($"Message {message.Id}: {payload}");

        message.Commited = true;
        return Task.CompletedTask;
    }
}
```

Publish through the common `IQueuePublisher` contract:

```csharp
using System.Text;
using EMC.Queue.Abstractions;

var persisted = await publisher.PublishAsync(new QueueMessage
{
    Topic = "orders",
    Key = "customer-123",
    Payload = Encoding.UTF8.GetBytes("001|99.50|BRL")
}, cancellationToken);
```

`QueueMessage` carries a topic, key, byte payload, ID, and creation timestamp. The publisher uses `Key` as the Kafka key, falling back to `Id` when no key is supplied. `PublishAsync` returns `true` when the broker reports the message as persisted. The library does not serialize domain objects; producers and consumers must agree on the byte format.

## Acknowledgement and failures

The publisher awaits delivery to Kafka. Enable idempotence and `acks=all` in producer options when required; the sample sets both.

The consumer processes messages sequentially. Set `EnableAutoCommit = false` so Kafka does not commit offsets automatically. After the handler returns, the adapter commits the offset only when the handler set `QueueMessage.Commited` to `true`. If the handler fails, the exception propagates from `IQueueConsumer` and the hosted service logs the error. Messages without a registered handler are not committed. `ConsumeException` errors are logged and the consume loop continues.

Manual acknowledgement provides at-least-once processing: a message may be delivered again if the process stops before committing it. Make handlers safe to run more than once. When reading, the Kafka key is assigned to both `QueueMessage.Id` and `QueueMessage.Key`; the adapter does not persist the original ID or `CreatedAt` in Kafka headers, and assigns a new read timestamp.

Each consume run creates, closes, and disposes its Kafka consumer. When constructing `KafkaQueueConsumer` directly, also configure the client with `EnableAutoCommit = false`; acknowledge offsets through `Commited`. Set Kafka limits such as `MaxPollIntervalMs` to match handler processing times.

For production, configure authentication and TLS as needed, along with cluster replication, retention, monitoring, and access controls. See the [Confluent .NET client documentation](https://docs.confluent.io/kafka-clients/dotnet/current/overview.html).
