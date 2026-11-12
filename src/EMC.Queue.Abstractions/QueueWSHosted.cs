using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EMC.Queue.Abstractions;

public sealed class QueueWSHosted : BackgroundService
{
    private readonly IQueueConsumer _queueConsumer;
    private readonly ILogger<QueueWSHosted> _logger;

    public QueueWSHosted(IQueueConsumer queueConsumer,  ILogger<QueueWSHosted> logger)
    {
        _queueConsumer = queueConsumer ?? throw new ArgumentNullException(nameof(queueConsumer));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            _logger.LogInformation("Queue WebSocket listener started {Timestamp}.", DateTimeOffset.UtcNow);
            await _queueConsumer.ConsumeAsync(stoppingToken);
        }
        catch (OperationCanceledException ex) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation(ex, "Queue WebSocket listener canceled {Timestamp}.", DateTimeOffset.UtcNow);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "An error occurred while running the Queue WebSocket listener {Timestamp}.", DateTimeOffset.UtcNow);
        }
    }
}
