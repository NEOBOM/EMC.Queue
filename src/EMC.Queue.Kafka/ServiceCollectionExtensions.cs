using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using EMC.Queue.Abstractions;

namespace EMC.Queue.Kafka;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddKafkaQueueProducer(this IServiceCollection services, ProducerKafkaQueueOptions options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);
        options.ValidateForPublish();
        services.TryAddSingleton(options);
        var producer = new ProducerConfig(options.Producer.ToDictionary(pair => pair.Key, pair => pair.Value));
        services.AddSingleton(_ => new ProducerBuilder<string, byte[]>(producer).Build());
        services.AddSingleton<IQueuePublisher, KafkaQueuePublisher>();
        return services;
    }

    public static IServiceCollection AddKafkaConsumerOptions(this IServiceCollection services, ConsumerKafkaQueueOptions options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);
        options.ValidateForConsume();
        services.TryAddSingleton(options);
        var consumer = new ConsumerConfig(options.Consumer.ToDictionary(pair => pair.Key, pair => pair.Value));
        services.AddSingleton<Func<IConsumer<string, byte[]>>>(_ => () => new ConsumerBuilder<string, byte[]>(consumer).Build());
        services.AddSingleton<IQueueConsumer, KafkaQueueConsumer>();
        services.AddHostedService<QueueWSHosted>();
        return services;
    }

    public static IServiceCollection AddConsumerHandler<TConsumerHandler>(this IServiceCollection services, string topic) where TConsumerHandler : class, IConsumerHandler
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);

        var registration = services
            .Where(descriptor => descriptor.ServiceType == typeof(ConsumerKafkaQueueOptions))
            .Select(descriptor => descriptor.ImplementationInstance)
            .OfType<ConsumerKafkaQueueOptions>()
            .FirstOrDefault();

        if (registration is null)
            throw new InvalidOperationException($"Service {typeof(ConsumerKafkaQueueOptions)} not registred.");

        if (registration.HandlerTypes.ContainsKey(topic))
            throw new InvalidOperationException($"A consumer handler for topic '{topic}' has already been registered.");

        registration.HandlerTypes.Add(topic, typeof(TConsumerHandler));

        services.AddSingleton<TConsumerHandler>();
        services.AddSingleton<IConsumerHandler>(provider => provider.GetRequiredService<TConsumerHandler>());
        return services;
    }
}
