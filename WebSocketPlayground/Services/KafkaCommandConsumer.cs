using System.Text.Json;
using Confluent.Kafka;
using Microsoft.AspNetCore.SignalR;
using WebSocketPlayground.Configuration;
using WebSocketPlayground.Hubs;
using WebSocketPlayground.Models;

namespace WebSocketPlayground.Services;

/// <summary>
/// Background service that consumes EndSessionCommand messages from Kafka.
/// 
/// NOTE: This consumer is now ONLY used for administrative forced logouts/disconnects,
/// not for duplicate session resolution. Duplicate sessions are now handled entirely
/// within the WebSocket/SignalR flow via the SessionConflict mechanism.
/// 
/// Use cases for EndSessionCommand:
/// - Administrative actions (e.g., teacher forcibly ending a student's session)
/// - System-initiated disconnections (e.g., maintenance, security)
/// - External triggers that require immediate session termination
/// </summary>
public class KafkaCommandConsumer : BackgroundService
{
    private readonly IHubContext<StudentActivityHub> _hubContext;
    private readonly IServiceProvider _serviceProvider;
    private readonly KafkaConfiguration _kafkaConfig;
    private readonly ILogger<KafkaCommandConsumer> _logger;
    private IConsumer<string, string>? _consumer;

    public KafkaCommandConsumer(
        IHubContext<StudentActivityHub> hubContext,
        IServiceProvider serviceProvider,
        KafkaConfiguration kafkaConfig,
        ILogger<KafkaCommandConsumer> logger)
    {
        _hubContext = hubContext;
        _serviceProvider = serviceProvider;
        _kafkaConfig = kafkaConfig;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("KafkaCommandConsumer starting. Waiting for EndSessionCommands from topic: {Topic}", 
            _kafkaConfig.CommandsTopic);

        var config = new ConsumerConfig
        {
            BootstrapServers = _kafkaConfig.BootstrapServers,
            GroupId = _kafkaConfig.ConsumerGroupId,
            ClientId = "websocket-service-consumer",
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = false, // Manual commit for better control
            SessionTimeoutMs = 6000,
            HeartbeatIntervalMs = 3000,
            MaxPollIntervalMs = 300000,
            // Optimize for low latency
            FetchMinBytes = 1,
            FetchWaitMaxMs = 100
        };

        _consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, error) =>
            {
                _logger.LogError("Kafka Consumer Error: {Reason} - {Code}", error.Reason, error.Code);
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
                _logger.Log(level, "Kafka Consumer Log: {Message}", logMessage.Message);
            })
            .SetPartitionsAssignedHandler((_, partitions) =>
            {
                _logger.LogInformation("Partitions assigned: {Partitions}", 
                    string.Join(", ", partitions.Select(p => $"{p.Topic}[{p.Partition}]")));
            })
            .SetPartitionsRevokedHandler((_, partitions) =>
            {
                _logger.LogInformation("Partitions revoked: {Partitions}", 
                    string.Join(", ", partitions.Select(p => $"{p.Topic}[{p.Partition}]")));
            })
            .Build();

        try
        {
            _consumer.Subscribe(_kafkaConfig.CommandsTopic);
            _logger.LogInformation("Subscribed to topic: {Topic}", _kafkaConfig.CommandsTopic);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumer.Consume(TimeSpan.FromMilliseconds(100));
                    
                    if (consumeResult?.Message?.Value != null)
                    {
                        await HandleMessageAsync(consumeResult, stoppingToken);
                        
                        // Commit offset after successful processing
                        try
                        {
                            _consumer.Commit(consumeResult);
                        }
                        catch (KafkaException ex)
                        {
                            _logger.LogWarning(ex, "Failed to commit offset for message at {Partition}:{Offset}", 
                                consumeResult.Partition.Value, consumeResult.Offset.Value);
                        }
                    }
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Error consuming message: {Error}", ex.Error.Reason);
                    
                    // Wait a bit before retrying on error
                    await Task.Delay(1000, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    // Expected when stopping
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Unexpected error in consumer loop");
                    
                    // Wait before retrying
                    await Task.Delay(5000, stoppingToken);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("KafkaCommandConsumer stopping due to cancellation");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Fatal error in KafkaCommandConsumer");
            throw;
        }
        finally
        {
            try
            {
                _consumer.Close();
                _consumer.Dispose();
                _logger.LogInformation("KafkaCommandConsumer stopped and disposed");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error closing Kafka consumer");
            }
        }
    }

    private async Task HandleMessageAsync(ConsumeResult<string, string> consumeResult, CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogDebug("Received message from {Topic}[{Partition}] at offset {Offset}: {Key}", 
                consumeResult.Topic, consumeResult.Partition.Value, consumeResult.Offset.Value, consumeResult.Message.Key);

            var command = JsonSerializer.Deserialize<EndSessionCommand>(consumeResult.Message.Value, 
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

            if (command == null)
            {
                _logger.LogWarning("Failed to deserialize EndSessionCommand from message at offset {Offset}", 
                    consumeResult.Offset.Value);
                return;
            }

            // Validate command
            if (string.IsNullOrEmpty(command.UserId) || 
                string.IsNullOrEmpty(command.AssignmentId) || 
                string.IsNullOrEmpty(command.ConnectionId))
            {
                _logger.LogWarning("Invalid EndSessionCommand (missing required fields) at offset {Offset}: {Command}", 
                    consumeResult.Offset.Value, consumeResult.Message.Value);
                return;
            }

            await HandleEndSessionCommand(command, cancellationToken);
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex, "Failed to parse message as EndSessionCommand at offset {Offset}: {Value}", 
                consumeResult.Offset.Value, consumeResult.Message.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error handling message at offset {Offset}", consumeResult.Offset.Value);
        }
    }

    private async Task HandleEndSessionCommand(EndSessionCommand command, CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation(
                "Processing EndSessionCommand (Administrative): UserId={UserId}, AssignmentId={AssignmentId}, ConnectionId={ConnectionId}",
                command.UserId, command.AssignmentId, command.ConnectionId);

            using var scope = _serviceProvider.CreateScope();
            var connectionStateManager = scope.ServiceProvider.GetRequiredService<IConnectionStateManager>();
            var eventPublisher = scope.ServiceProvider.GetRequiredService<IActivityEventPublisher>();

            // Find the connection by userId and assignmentId
            var userConnections = await connectionStateManager.GetActiveConnectionsByUserIdAsync(command.UserId);
            var targetConnection = userConnections.FirstOrDefault(c => 
                c.AssignmentId == command.AssignmentId && c.ConnectionId == command.ConnectionId);

            if (targetConnection != null)
            {
                _logger.LogInformation(
                    "Found target connection for EndSessionCommand: ParticipationId={ParticipationId}, ConnectionId={ConnectionId}",
                    targetConnection.ParticipationId, command.ConnectionId);

                // Remove from active connections and grace period (if any)
                await connectionStateManager.RemoveActiveConnectionAsync(targetConnection.ParticipationId);
                await connectionStateManager.RemoveGracePeriodStateAsync(targetConnection.ParticipationId);

                // Publish activity ended event
                await eventPublisher.PublishActivityEndedAsync(new StudentActivityEndedEvent
                {
                    UserId = targetConnection.UserId,
                    AssignmentId = targetConnection.AssignmentId,
                    ParticipationId = targetConnection.ParticipationId,
                    ConnectionId = targetConnection.ConnectionId,
                    Reason = DisconnectReason.Disconnected
                });

                // Send disconnect message to the client
                await _hubContext.Clients.Client(command.ConnectionId).SendAsync(
                    "ForceDisconnect", 
                    "Session ended by administrative command", 
                    cancellationToken);

                _logger.LogInformation(
                    "Sent ForceDisconnect message to ConnectionId={ConnectionId}",
                    command.ConnectionId);
            }
            else
            {
                // Check if this is a duplicate/stale command (connection already terminated)
                _logger.LogWarning(
                    "EndSessionCommand: Connection not found for UserId={UserId}, AssignmentId={AssignmentId}, ConnectionId={ConnectionId}. " +
                    "Connection may have already been terminated or never existed (idempotent operation).",
                    command.UserId, command.AssignmentId, command.ConnectionId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, 
                "Error handling EndSessionCommand for UserId={UserId}, AssignmentId={AssignmentId}, ConnectionId={ConnectionId}",
                command.UserId, command.AssignmentId, command.ConnectionId);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("KafkaCommandConsumer StopAsync called");
        await base.StopAsync(cancellationToken);
    }
}


