using System.Text.Json;
using Confluent.Kafka;
using WebSocketPlayground.Configuration;
using WebSocketPlayground.Models;

namespace WebSocketPlayground.Services;

public class ActivityEventPublisher : IActivityEventPublisher, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly KafkaConfiguration _kafkaConfig;
    private readonly ILogger<ActivityEventPublisher> _logger;

    public ActivityEventPublisher(
        KafkaConfiguration kafkaConfig,
        ILogger<ActivityEventPublisher> logger)
    {
        _kafkaConfig = kafkaConfig;
        _logger = logger;

        var config = new ProducerConfig
        {
            BootstrapServers = kafkaConfig.BootstrapServers,
            ClientId = "websocket-service-producer",
            Acks = Acks.All, // Required when EnableIdempotence = true
            EnableIdempotence = true,
            MaxInFlight = 5,
            MessageSendMaxRetries = 3,
            RequestTimeoutMs = 5000,
            // Add compression for better throughput
            CompressionType = CompressionType.Snappy
        };

        _producer = new ProducerBuilder<string, string>(config)
            .SetErrorHandler((_, error) =>
            {
                _logger.LogError("Kafka Producer Error: {Reason} - {Code}", error.Reason, error.Code);
            })
            .SetLogHandler((_, logMessage) =>
            {
                var level = logMessage.Level switch
                {
                    SyslogLevel.Emergency or SyslogLevel.Alert or SyslogLevel.Critical or SyslogLevel.Error => LogLevel.Error,
                    SyslogLevel.Warning => LogLevel.Warning,
                    SyslogLevel.Notice or SyslogLevel.Info => LogLevel.Information,
                    _ => LogLevel.Debug
                };
                _logger.Log(level, "Kafka Producer Log: {Message}", logMessage.Message);
            })
            .Build();

        _logger.LogInformation("Kafka producer initialized with bootstrap servers: {BootstrapServers}", 
            kafkaConfig.BootstrapServers);
    }

    public async Task PublishActivityStartedAsync(StudentActivityStartedEvent eventData, CancellationToken cancellationToken = default)
    {
        try
        {
            var key = $"{eventData.UserId}:{eventData.AssignmentId}";
            var value = JsonSerializer.Serialize(eventData, new JsonSerializerOptions 
            { 
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase 
            });

            var message = new Message<string, string>
            {
                Key = key,
                Value = value,
                Headers = new Headers
                {
                    { "event-type", System.Text.Encoding.UTF8.GetBytes("StudentActivityStarted") },
                    { "event-version", System.Text.Encoding.UTF8.GetBytes("1.0") },
                    { "timestamp", System.Text.Encoding.UTF8.GetBytes(eventData.Timestamp.ToString("O")) }
                }
            };

            var result = await _producer.ProduceAsync(_kafkaConfig.EventsTopic, message, cancellationToken);

            _logger.LogInformation(
                "Published StudentActivityStarted event: UserId={UserId}, AssignmentId={AssignmentId}, ParticipationId={ParticipationId}, " +
                "Topic={Topic}, Partition={Partition}, Offset={Offset}",
                eventData.UserId, eventData.AssignmentId, eventData.ParticipationId,
                result.Topic, result.Partition.Value, result.Offset.Value);
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError(ex, 
                "Failed to publish StudentActivityStarted event: UserId={UserId}, AssignmentId={AssignmentId}, Error={Error}",
                eventData.UserId, eventData.AssignmentId, ex.Error.Reason);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, 
                "Unexpected error publishing StudentActivityStarted event: UserId={UserId}, AssignmentId={AssignmentId}",
                eventData.UserId, eventData.AssignmentId);
            throw;
        }
    }

    public async Task PublishActivityEndedAsync(StudentActivityEndedEvent eventData, CancellationToken cancellationToken = default)
    {
        try
        {
            var key = $"{eventData.UserId}:{eventData.AssignmentId}";
            var value = JsonSerializer.Serialize(eventData, new JsonSerializerOptions 
            { 
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase 
            });

            var message = new Message<string, string>
            {
                Key = key,
                Value = value,
                Headers = new Headers
                {
                    { "event-type", System.Text.Encoding.UTF8.GetBytes("StudentActivityEnded") },
                    { "event-version", System.Text.Encoding.UTF8.GetBytes("1.0") },
                    { "timestamp", System.Text.Encoding.UTF8.GetBytes(eventData.Timestamp.ToString("O")) },
                    { "disconnect-reason", System.Text.Encoding.UTF8.GetBytes(eventData.Reason.ToString()) }
                }
            };

            var result = await _producer.ProduceAsync(_kafkaConfig.EventsTopic, message, cancellationToken);

            _logger.LogInformation(
                "Published StudentActivityEnded event: UserId={UserId}, AssignmentId={AssignmentId}, ParticipationId={ParticipationId}, " +
                "Reason={Reason}, Topic={Topic}, Partition={Partition}, Offset={Offset}",
                eventData.UserId, eventData.AssignmentId, eventData.ParticipationId, eventData.Reason,
                result.Topic, result.Partition.Value, result.Offset.Value);
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError(ex, 
                "Failed to publish StudentActivityEnded event: UserId={UserId}, AssignmentId={AssignmentId}, Error={Error}",
                eventData.UserId, eventData.AssignmentId, ex.Error.Reason);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, 
                "Unexpected error publishing StudentActivityEnded event: UserId={UserId}, AssignmentId={AssignmentId}",
                eventData.UserId, eventData.AssignmentId);
            throw;
        }
    }

    public void Dispose()
    {
        try
        {
            // Flush any pending messages before disposing
            _producer.Flush(TimeSpan.FromSeconds(10));
            _producer.Dispose();
            _logger.LogInformation("Kafka producer disposed");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error disposing Kafka producer");
        }
    }
}

