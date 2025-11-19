using KafkaFlow;
using Microsoft.AspNetCore.SignalR;
using WebSocketPlayground.Hubs;
using WebSocketPlayground.Models;

namespace WebSocketPlayground.Services;

/// <summary>
/// KafkaFlow message handler for EndSessionCommand messages.
/// 
/// NOTE: This handler is now ONLY used for administrative forced logouts/disconnects,
/// not for duplicate session resolution. Duplicate sessions are now handled entirely
/// within the WebSocket/SignalR flow via the SessionConflict mechanism.
/// 
/// Use cases for EndSessionCommand:
/// - Administrative actions (e.g., teacher forcibly ending a student's session)
/// - System-initiated disconnections (e.g., maintenance, security)
/// - External triggers that require immediate session termination
/// </summary>
public class EndSessionCommandHandler : IMessageHandler<EndSessionCommand>
{
    private readonly IHubContext<StudentActivityHub> _hubContext;
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<EndSessionCommandHandler> _logger;

    public EndSessionCommandHandler(
        IHubContext<StudentActivityHub> hubContext,
        IServiceProvider serviceProvider,
        ILogger<EndSessionCommandHandler> logger)
    {
        _hubContext = hubContext;
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    public async Task Handle(IMessageContext context, EndSessionCommand message)
    {
        try
        {
            _logger.LogInformation(
                "Processing EndSessionCommand (Administrative): UserId={UserId}, AssignmentId={AssignmentId}, ConnectionId={ConnectionId}",
                message.UserId, message.AssignmentId, message.ConnectionId);

            // Validate command
            if (message.UserId == Guid.Empty || 
                message.AssignmentId == Guid.Empty || 
                string.IsNullOrEmpty(message.ConnectionId))
            {
                _logger.LogWarning("Invalid EndSessionCommand (missing required fields)");
                return;
            }

            using var scope = _serviceProvider.CreateScope();
            var connectionStateManager = scope.ServiceProvider.GetRequiredService<IConnectionStateManager>();
            var eventPublisher = scope.ServiceProvider.GetRequiredService<IActivityEventPublisher>();

            // Find the connection by userId and assignmentId
            var userConnections = await connectionStateManager.GetActiveConnectionsByUserIdAsync(message.UserId);
            var targetConnection = userConnections.FirstOrDefault(c => 
                c.AssignmentId == message.AssignmentId && c.ConnectionId == message.ConnectionId);

            if (targetConnection != null)
            {
                _logger.LogInformation(
                    "Found target connection for EndSessionCommand: ParticipationId={ParticipationId}, ConnectionId={ConnectionId}",
                    targetConnection.ParticipationId, message.ConnectionId);

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
                await _hubContext.Clients.Client(message.ConnectionId).SendAsync(
                    "ForceDisconnect", 
                    "Session ended by administrative command");

                _logger.LogInformation(
                    "Sent ForceDisconnect message to ConnectionId={ConnectionId}",
                    message.ConnectionId);
            }
            else
            {
                // Check if this is a duplicate/stale command (connection already terminated)
                _logger.LogWarning(
                    "EndSessionCommand: Connection not found for UserId={UserId}, AssignmentId={AssignmentId}, ConnectionId={ConnectionId}. " +
                    "Connection may have already been terminated or never existed (idempotent operation).",
                    message.UserId, message.AssignmentId, message.ConnectionId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, 
                "Error handling EndSessionCommand for UserId={UserId}, AssignmentId={AssignmentId}, ConnectionId={ConnectionId}",
                message.UserId, message.AssignmentId, message.ConnectionId);
            throw; // KafkaFlow will handle retry logic
        }
    }
}

