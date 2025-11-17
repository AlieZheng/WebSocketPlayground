namespace WebSocketPlayground.Configuration;

public class KafkaConfiguration
{
    public string BootstrapServers { get; set; } = string.Empty;
    public string CommandsTopic { get; set; } = string.Empty;
    public string EventsTopic { get; set; } = string.Empty;
    public string ConsumerGroupId { get; set; } = string.Empty;
}

