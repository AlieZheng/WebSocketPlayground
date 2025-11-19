namespace WebSocketPlayground.Configuration;

public class TimeoutConfiguration
{
    public int GracePeriodSeconds { get; set; } = 30;
    public int ConflictResolutionTimeoutSeconds { get; set; } = 30;
}

