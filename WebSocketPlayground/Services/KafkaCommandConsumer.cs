using System.Text.Json;
using Confluent.Kafka;
using Microsoft.AspNetCore.SignalR;
using WebSocketPlayground.Configuration;
using WebSocketPlayground.Hubs;
using WebSocketPlayground.Models;

namespace WebSocketPlayground.Services;

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
                "Processing EndSessionCommand: UserId={UserId}, AssignmentId={AssignmentId}, ConnectionId={ConnectionId}",
                command.UserId, command.AssignmentId, command.ConnectionId);

            using var scope = _serviceProvider.CreateScope();
            var connectionStateManager = scope.ServiceProvider.GetRequiredService<IConnectionStateManager>();

            // Find the connection by userId and assignmentId
            var userConnections = await connectionStateManager.GetActiveConnectionsByUserIdAsync(command.UserId);
            var targetConnection = userConnections.FirstOrDefault(c => 
                c.AssignmentId == command.AssignmentId && c.ConnectionId == command.ConnectionId);

            if (targetConnection != null)
            {
                _logger.LogInformation(
                    "Found target connection for EndSessionCommand: AttemptId={AttemptId}, ConnectionId={ConnectionId}",
                    targetConnection.AttemptId, command.ConnectionId);

                // Check for pending connection to promote
                var pendingConnection = await connectionStateManager.GetPendingConnectionAsync(targetConnection.AttemptId);
                
                // Send disconnect message to the client
                await _hubContext.Clients.Client(command.ConnectionId).SendAsync(
                    "ForceDisconnect", 
                    "Session ended by command", 
                    cancellationToken);

                _logger.LogInformation(
                    "Sent ForceDisconnect message to ConnectionId={ConnectionId}. Pending connection exists: {HasPending}",
                    command.ConnectionId, pendingConnection != null);

                // The actual cleanup and promotion will be handled by OnDisconnectedAsync in the hub
                // when the client disconnects in response to ForceDisconnect message
            }
            else
            {
                // Check if this is a duplicate/stale command (connection already terminated)
                var gracePeriodState = await connectionStateManager.GetGracePeriodStateAsync(command.ConnectionId);
                if (gracePeriodState != null)
                {
                    _logger.LogInformation(
                        "EndSessionCommand received for connection in grace period: ConnectionId={ConnectionId}. " +
                        "This is likely a duplicate or stale command (idempotent operation).",
                        command.ConnectionId);
                }
                else
                {
                    _logger.LogWarning(
                        "EndSessionCommand: Connection not found for UserId={UserId}, AssignmentId={AssignmentId}, ConnectionId={ConnectionId}. " +
                        "Connection may have already been terminated or never existed.",
                        command.UserId, command.AssignmentId, command.ConnectionId);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, 
                "Error handling EndSessionCommand for UserId={UserId}, AssignmentId={AssignmentId}, ConnectionId={ConnectionId}",
                command.UserId, command.AssignmentId, command.ConnectionId);
            throw;
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("KafkaCommandConsumer StopAsync called");
        await base.StopAsync(cancellationToken);
    }
}

