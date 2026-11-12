namespace EMC.Queue.Samples.Shared;

public sealed record OrderCreated(string OrderId, decimal? TotalAmount, string Currency);
