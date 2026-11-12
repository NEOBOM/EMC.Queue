using Confluent.Kafka;
using EMC.Queue.Abstractions;
using EMC.Queue.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace EMC.Queue.Tests;

public sealed class RegistrationTests
{
    [Fact]
    public void AddKafkaQueueProducer_RegistersSingletonPublisherAndOptions()
    {
        var options = new ProducerKafkaQueueOptions
        {
            Producer = new ProducerConfig { BootstrapServers = "localhost:9092" }
        };
        var services = new ServiceCollection();

        var returnedServices = services.AddKafkaQueueProducer(options);

        Assert.Same(services, returnedServices);
        var optionsRegistration = Assert.Single(services, d => d.ServiceType == typeof(ProducerKafkaQueueOptions));
        Assert.Same(options, optionsRegistration.ImplementationInstance);
        var publisherRegistration = Assert.Single(services, d => d.ServiceType == typeof(IQueuePublisher));
        Assert.Equal(ServiceLifetime.Singleton, publisherRegistration.Lifetime);
        Assert.Equal(typeof(KafkaQueuePublisher), publisherRegistration.ImplementationType);
        var producerRegistration = Assert.Single(services, d => d.ServiceType == typeof(IProducer<string, byte[]>));
        Assert.Equal(ServiceLifetime.Singleton, producerRegistration.Lifetime);
        Assert.NotNull(producerRegistration.ImplementationFactory);
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(IQueueConsumer));
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(IHostedService));
    }

    [Fact]
    public void AddKafkaConsumerOptions_RegistersSingletonConsumerFactoryAndHostedService()
    {
        var options = new ConsumerKafkaQueueOptions
        {
            Consumer = new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "workers" }
        };
        var services = new ServiceCollection();

        var returnedServices = services.AddKafkaConsumerOptions(options);

        Assert.Same(services, returnedServices);
        var optionsRegistration = Assert.Single(services, d => d.ServiceType == typeof(ConsumerKafkaQueueOptions));
        Assert.Same(options, optionsRegistration.ImplementationInstance);
        var consumerRegistration = Assert.Single(services, d => d.ServiceType == typeof(IQueueConsumer));
        Assert.Equal(ServiceLifetime.Singleton, consumerRegistration.Lifetime);
        Assert.Equal(typeof(KafkaQueueConsumer), consumerRegistration.ImplementationType);
        var factoryRegistration = Assert.Single(services, d => d.ServiceType == typeof(Func<IConsumer<string, byte[]>>));
        Assert.Equal(ServiceLifetime.Singleton, factoryRegistration.Lifetime);
        Assert.NotNull(factoryRegistration.ImplementationFactory);
        var hostedServiceRegistration = Assert.Single(services, d => d.ServiceType == typeof(IHostedService));
        Assert.Equal(typeof(QueueWSHosted), hostedServiceRegistration.ImplementationType);
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(IQueuePublisher));
    }

    [Fact]
    public void AddConsumerHandler_RegistersConcreteAndInterfaceAsSameSingleton()
    {
        var services = new ServiceCollection()
            .AddKafkaConsumerOptions(new ConsumerKafkaQueueOptions
            {
                Consumer = new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "workers" }
            });
        services.AddConsumerHandler<RegisteredHandler>("orders");
        using var provider = services.BuildServiceProvider();

        var concrete = provider.GetRequiredService<RegisteredHandler>();
        var service = provider.GetRequiredService<IConsumerHandler>();

        Assert.Same(concrete, service);
        Assert.Equal(ServiceLifetime.Singleton,
            Assert.Single(services, d => d.ServiceType == typeof(RegisteredHandler)).Lifetime);
        Assert.Equal(ServiceLifetime.Singleton,
            Assert.Single(services, d => d.ServiceType == typeof(IConsumerHandler)).Lifetime);
    }

    [Fact]
    public void AddConsumerHandler_RequiresConsumerOptionsRegistration()
    {
        var services = new ServiceCollection();

        var error = Assert.Throws<InvalidOperationException>(() => services.AddConsumerHandler<RegisteredHandler>("orders"));

        Assert.Contains(nameof(ConsumerKafkaQueueOptions), error.Message);
        Assert.Empty(services);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void AddConsumerHandler_RejectsBlankTopic(string topic)
    {
        var services = new ServiceCollection()
            .AddKafkaConsumerOptions(new ConsumerKafkaQueueOptions
            {
                Consumer = new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "workers" }
            });

        Assert.Throws<ArgumentException>(() => services.AddConsumerHandler<RegisteredHandler>(topic));
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(RegisteredHandler));
    }

    [Fact]
    public void AddConsumerHandler_RejectsDuplicateTopicIgnoringCase()
    {
        var services = new ServiceCollection()
            .AddKafkaConsumerOptions(new ConsumerKafkaQueueOptions
            {
                Consumer = new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "workers" }
            });
        services.AddConsumerHandler<RegisteredHandler>("orders");

        Assert.Throws<InvalidOperationException>(() => services.AddConsumerHandler<RegisteredHandler>("ORDERS"));
        Assert.Single(services, d => d.ServiceType == typeof(RegisteredHandler));
    }

    [Fact]
    public void AddKafkaConsumerOptions_RejectsMissingGroupId()
    {
        var options = new ConsumerKafkaQueueOptions
        {
            Consumer = new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "" }
        };
        var services = new ServiceCollection();

        Assert.Equal("Consumer.GroupId", Assert.Throws<ArgumentException>(() => services.AddKafkaConsumerOptions(options)).ParamName);
        Assert.Empty(services);
    }

    [Fact]
    public void KafkaRegistrationExtensions_RejectNullArguments()
    {
        var producerOptions = new ProducerKafkaQueueOptions
        {
            Producer = new ProducerConfig { BootstrapServers = "localhost:9092" }
        };
        var consumerOptions = new ConsumerKafkaQueueOptions
        {
            Consumer = new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "workers" }
        };

        Assert.Throws<ArgumentNullException>(() => ServiceCollectionExtensions.AddKafkaQueueProducer(null, producerOptions));
        Assert.Throws<ArgumentNullException>(() => new ServiceCollection().AddKafkaQueueProducer(null));
        Assert.Throws<ArgumentNullException>(() => ServiceCollectionExtensions.AddKafkaConsumerOptions(null, consumerOptions));
        Assert.Throws<ArgumentNullException>(() => new ServiceCollection().AddKafkaConsumerOptions(null));
        Assert.Throws<ArgumentNullException>(() => ServiceCollectionExtensions.AddConsumerHandler<RegisteredHandler>(null, "orders"));
    }

    public sealed class RegisteredHandler : IConsumerHandler
    {
        public Task ConsumerHandleAsync(QueueMessage message, CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
