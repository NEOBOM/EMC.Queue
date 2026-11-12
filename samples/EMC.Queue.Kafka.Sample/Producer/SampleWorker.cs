using EMC.Queue.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text;

namespace EMC.Queue.Samples.Producer;

internal sealed class SampleWorker(
    IQueuePublisher publisher,
    IHostApplicationLifetime lifetime,
    ILogger<SampleWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            var queueMessageOrder = new QueueMessage
            {
                Topic = "orders",
                Payload = Encoding.UTF8.GetBytes("001|99.50|BRL"),
                Key = "001"
            };

            await PubMesaageAsync(queueMessageOrder, stoppingToken);

            var queueMessagePaygment = new QueueMessage
            {
                Topic = "payments",
                Payload = Encoding.UTF8.GetBytes("1|99.50|2026-11-10 09:10:50"),
                Key = "001"
            };

            await PubMesaageAsync(queueMessagePaygment, stoppingToken);
        }
        catch (Exception error)
        {
            logger.LogError(error, "Queue sample failed");
            Environment.ExitCode = 1;
        }
        finally
        {
            lifetime.StopApplication();
        }
    }

    private async Task PubMesaageAsync(QueueMessage queueMessage, CancellationToken stoppingToken)
    {
        try
        {
            ArgumentNullException.ThrowIfNull(queueMessage);
            ArgumentNullException.ThrowIfNull(queueMessage.Payload);
            
            var created = await publisher.PublishAsync(queueMessage, stoppingToken);
            logger.LogInformation("Published Paymento message {MessageId}, payload {Payload}, created {Created}", 
                queueMessage.Id, Encoding.UTF8.GetString(queueMessage.Payload), created);
        }
        catch (OperationCanceledException op) when (stoppingToken.IsCancellationRequested)
        {
            logger.LogWarning(op, "Cancellation requested while publishing message {MessageId}", queueMessage.Id);
        }
    }
}
