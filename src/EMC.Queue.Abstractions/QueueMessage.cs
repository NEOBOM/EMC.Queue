namespace EMC.Queue.Abstractions;

public sealed record QueueMessage
{
    public string? Topic { get; set; }
    public string Id { get; set; } = Guid.NewGuid().ToString("N");
    public byte[]? Payload { get; set; }
    public DateTimeOffset CreatedAt { get; init; } = DateTimeOffset.UtcNow;
    public string? Key { get; set; }
    public bool Commited { get; set; }
}