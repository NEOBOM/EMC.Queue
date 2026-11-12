using EMC.Queue.Abstractions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text;

namespace EMC.Queue.Samples.Producer;

internal sealed class SampleWorker(
    IQueuePublisher publisher,
    IConfiguration configuration,
    IHostApplicationLifetime lifetime,
    ILogger<SampleWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var runTimeout = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var operationToken = runTimeout.Token;
        try
        {
            if (configuration["RunSeconds"] is { } duration)
            {
                if (!int.TryParse(duration, out var seconds) || seconds <= 0)
                    throw new ArgumentException("RunSeconds must be a positive integer.");
                runTimeout.CancelAfter(TimeSpan.FromSeconds(seconds));
            }

            var payload = Encoding.UTF8.GetBytes("001|99.50|BRL");

            var queueMessage = new QueueMessage
            {
                Topic = "orders",
                Payload = payload,
                Key = "001"
            };

            var created = await publisher.PublishAsync(queueMessage, operationToken);
            logger.LogInformation("Published message {MessageId}, payload {Payload}, created {Created}", queueMessage.Id, Encoding.UTF8.GetString(payload), created);
            lifetime.StopApplication();
        }
        catch (OperationCanceledException) when (operationToken.IsCancellationRequested)
        {
            lifetime.StopApplication();
        }
        catch (Exception error)
        {
            logger.LogError(error, "Queue sample failed");
            Environment.ExitCode = 1;
            lifetime.StopApplication();
        }
    }
}
