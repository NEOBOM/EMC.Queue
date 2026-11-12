using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using EMC.Queue.Abstractions;

namespace EMC.Queue.Kafka;

/// <summary>
/// Represents a Kafka queue consumer that consumes messages from a Kafka topic and invokes a provided handler for each message.
/// </summary>
public sealed class KafkaQueueConsumer : IQueueConsumer
{
    private readonly IServiceProvider _serviceProvider;
    private readonly Func<IConsumer<string, byte[]>> _consumerFactory;
    private readonly Dictionary<string, Func<QueueMessage, CancellationToken, Task>> _handles = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConsumerKafkaQueueOptions _configureAwaitOptions;
    
    public KafkaQueueConsumer(
        Func<IConsumer<string, byte[]>> consumerFactory,
        ConsumerKafkaQueueOptions options,
        IServiceProvider serviceProvider)
    {
        ArgumentNullException.ThrowIfNull(consumerFactory);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(serviceProvider);
        options.ValidateForConsume();

        _configureAwaitOptions = options;
        _serviceProvider = serviceProvider;
        _consumerFactory = consumerFactory;
    }

    public async Task ConsumeAsync(CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(_configureAwaitOptions.HandlerTypes);
        cancellationToken.ThrowIfCancellationRequested();

        await Task.Run(async () =>
        {
            using var consumer = _consumerFactory();
            try
            {
                consumer.Subscribe(_configureAwaitOptions.HandlerTypes.Keys);

                while (!cancellationToken.IsCancellationRequested)
                {
                    await ConsumeSingleMessageAsync(consumer, cancellationToken).ConfigureAwait(false);
                }

                cancellationToken.ThrowIfCancellationRequested();
            }
            catch (OperationCanceledException opEx) when (opEx.CancellationToken == cancellationToken)
            {
                Loggers.ConsoleLogger(new (nameof(ConsumeAsync),
                    SyslogLevel.Info,
                    nameof(KafkaQueueConsumer),
                    "Consume operation canceled."));
            }
            catch (Exception ex)
            {
                Loggers.ConsoleLogger(new (nameof(ConsumeAsync),
                    SyslogLevel.Error,
                    nameof(ConsumeAsync),
                    $"Consume operation failed: {ex.Message}"));

                throw;
            }
            finally
            {
                consumer.Close();
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    private async Task ConsumeSingleMessageAsync(
        IConsumer<string, byte[]> consumer,
        CancellationToken cancellationToken)
    {
        try
        {
            var delivery = consumer.Consume(cancellationToken);

            if (delivery is null || delivery.IsPartitionEOF)
                return;

            if (!_handles.TryGetValue(delivery.Topic, out var handler))
            {
                if (!_configureAwaitOptions.HandlerTypes.TryGetValue(delivery.Topic, out var handlerType))
                {
                    Loggers.ConsoleLogger(new (nameof(KafkaQueueConsumer),
                        SyslogLevel.Warning,
                        nameof(KafkaQueueConsumer),
                        $"No handler registered for topic '{delivery.Topic}'."));

                    return;
                }

                handler = ((IConsumerHandler)_serviceProvider.GetRequiredService(handlerType)).ConsumerHandleAsync;
                _handles.Add(delivery.Topic, handler);
            }

            var queueMessage = new QueueMessage
            {
                Id = delivery.Message.Key,
                Payload = delivery.Message.Value,
                CreatedAt = DateTime.UtcNow,
                Key = delivery.Message.Key,
                Topic = delivery.Topic
            };
           
            await handler(queueMessage, cancellationToken).ConfigureAwait(false);

            if (queueMessage.Commited)
                consumer.Commit(delivery);
        }
        catch (ConsumeException ex)
        {
            Loggers.ConsoleLogger(new (nameof(KafkaQueueConsumer),
                SyslogLevel.Error,
                nameof(KafkaQueueConsumer),
                $"Consume error: {ex.Error.Reason}"));
        }
    }
}
