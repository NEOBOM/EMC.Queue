namespace EMC.Queue.Samples.Shared;

public sealed record PaymentCreated(int Id, decimal Value, DateTimeOffset Date);