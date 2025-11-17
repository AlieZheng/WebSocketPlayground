namespace WebSocketPlayground.Configuration;

public class TimeoutConfiguration
{
    public int GracePeriodSeconds { get; set; } = 30;
    public int PendingConnectionTimeoutSeconds { get; set; } = 10;
}

