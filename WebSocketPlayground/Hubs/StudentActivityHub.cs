using System.Security.Claims;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.SignalR;
using WebSocketPlayground.Configuration;
using WebSocketPlayground.Models;
using WebSocketPlayground.Services;

namespace WebSocketPlayground.Hubs;

[Authorize]
public class StudentActivityHub : Hub
{
    private readonly IConnectionStateManager _connectionStateManager;
    private readonly IActivityEventPublisher _eventPublisher;
    private readonly TimeoutConfiguration _timeoutConfig;
    private readonly ILogger<StudentActivityHub> _logger;
    private static readonly Dictionary<string, System.Threading.Timer> _gracePeriodTimers = new();
    private static readonly Dictionary<string, System.Threading.Timer> _pendingTimeoutTimers = new();
    private static readonly object _timerLock = new();

    public StudentActivityHub(
        IConnectionStateManager connectionStateManager,
        IActivityEventPublisher eventPublisher,
        TimeoutConfiguration timeoutConfig,
        ILogger<StudentActivityHub> logger)
    {
        _connectionStateManager = connectionStateManager;
        _eventPublisher = eventPublisher;
        _timeoutConfig = timeoutConfig;
        _logger = logger;
    }

    public override async Task OnConnectedAsync()
    {
        try
        {
            // Extract user ID from JWT claims
            var userId = Context.User?.FindFirst(ClaimTypes.NameIdentifier)?.Value 
                ?? Context.User?.FindFirst("sub")?.Value;
            
            if (string.IsNullOrEmpty(userId))
            {
                _logger.LogWarning("Connection rejected: No user ID found in claims");
                await Clients.Caller.SendAsync("ConnectionRejected", "Missing userId in token");
                Context.Abort();
                return;
            }

            // Get query parameters
            var httpContext = Context.GetHttpContext();
            var assignmentId = httpContext?.Request.Query["assignmentId"].ToString();
            var attemptId = httpContext?.Request.Query["attemptId"].ToString();

            if (string.IsNullOrEmpty(assignmentId) || string.IsNullOrEmpty(attemptId))
            {
                _logger.LogWarning("Connection rejected: Missing assignmentId or attemptId for user {UserId}", userId);
                await Clients.Caller.SendAsync("ConnectionRejected", "Missing required parameters");
                Context.Abort();
                return;
            }

            _logger.LogInformation("Connection attempt: UserId={UserId}, AssignmentId={AssignmentId}, AttemptId={AttemptId}, ConnectionId={ConnectionId}",
                userId, assignmentId, attemptId, Context.ConnectionId);

            // Check if there's an existing active connection for this attempt
            var existingConnection = await _connectionStateManager.GetActiveConnectionAsync(attemptId);
            
            if (existingConnection != null)
            {
                // There's already an active connection for this attempt - queue as pending
                _logger.LogInformation("Duplicate connection detected for AttemptId={AttemptId}. Queueing as pending.", attemptId);
                
                var pendingState = new ConnectionState
                {
                    ConnectionId = Context.ConnectionId,
                    UserId = userId,
                    AssignmentId = assignmentId,
                    AttemptId = attemptId,
                    IsPending = true
                };

                await _connectionStateManager.SetPendingConnectionAsync(
                    pendingState, 
                    TimeSpan.FromSeconds(_timeoutConfig.PendingConnectionTimeoutSeconds));

                // Start timeout timer for pending connection
                StartPendingTimeoutTimer(attemptId, userId, assignmentId);

                // Notify the client that they are in pending state
                await Clients.Caller.SendAsync("ConnectionPending", "Another session is active. Waiting for session switch.");
                return;
            }

            // Check for grace period state (reconnection scenario)
            var gracePeriodState = await _connectionStateManager.GetGracePeriodStateAsync(attemptId);
            if (gracePeriodState != null)
            {
                _logger.LogInformation("Reconnection during grace period for AttemptId={AttemptId}", attemptId);
                
                // Cancel grace period
                await _connectionStateManager.RemoveGracePeriodStateAsync(attemptId);
                CancelGracePeriodTimer(attemptId);

                // Check if user is connecting to a different assignment
                var userActiveConnections = await _connectionStateManager.GetActiveConnectionsByUserIdAsync(userId);
                foreach (var activeConn in userActiveConnections)
                {
                    if (activeConn.AssignmentId != assignmentId)
                    {
                        // User is switching to a different assignment - immediately end the old one
                        _logger.LogInformation("User switching from AssignmentId={OldAssignmentId} to AssignmentId={NewAssignmentId}",
                            activeConn.AssignmentId, assignmentId);

                        await PublishActivityEndedEvent(activeConn, DisconnectReason.SwitchedAssignment);
                        await _connectionStateManager.RemoveActiveConnectionAsync(activeConn.AttemptId);
                        await _connectionStateManager.RemoveGracePeriodStateAsync(activeConn.AttemptId);
                        CancelGracePeriodTimer(activeConn.AttemptId);
                    }
                }
            }

            // Create active connection state
            var connectionState = new ConnectionState
            {
                ConnectionId = Context.ConnectionId,
                UserId = userId,
                AssignmentId = assignmentId,
                AttemptId = attemptId,
                IsPending = false
            };

            await _connectionStateManager.SetActiveConnectionAsync(connectionState);

            // Publish activity started event
            await _eventPublisher.PublishActivityStartedAsync(new StudentActivityStartedEvent
            {
                UserId = userId,
                AssignmentId = assignmentId,
                AttemptId = attemptId,
                ConnectionId = Context.ConnectionId
            });

            _logger.LogInformation("Connection established: UserId={UserId}, AttemptId={AttemptId}, ConnectionId={ConnectionId}",
                userId, attemptId, Context.ConnectionId);

            await base.OnConnectedAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in OnConnectedAsync for ConnectionId={ConnectionId}", Context.ConnectionId);
            Context.Abort();
        }
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        try
        {
            _logger.LogInformation("Disconnection: ConnectionId={ConnectionId}, Exception={Exception}",
                Context.ConnectionId, exception?.Message);

            // Find which attempt this connection belongs to
            var userId = Context.User?.FindFirst(ClaimTypes.NameIdentifier)?.Value 
                ?? Context.User?.FindFirst("sub")?.Value;

            if (string.IsNullOrEmpty(userId))
            {
                await base.OnDisconnectedAsync(exception);
                return;
            }

            // Search for the connection in active connections
            var userConnections = await _connectionStateManager.GetActiveConnectionsByUserIdAsync(userId);
            var connectionState = userConnections.FirstOrDefault(c => c.ConnectionId == Context.ConnectionId);

            if (connectionState == null)
            {
                // Maybe it was a pending connection that got disconnected
                _logger.LogInformation("Disconnected connection not found in active connections: ConnectionId={ConnectionId}", Context.ConnectionId);
                await base.OnDisconnectedAsync(exception);
                return;
            }

            // Check if there's a pending connection waiting
            var pendingConnection = await _connectionStateManager.GetPendingConnectionAsync(connectionState.AttemptId);
            
            if (pendingConnection != null)
            {
                // Promote pending connection to active
                _logger.LogInformation("Promoting pending connection for AttemptId={AttemptId}", connectionState.AttemptId);
                
                await _connectionStateManager.PromotePendingToActiveAsync(connectionState.AttemptId);
                await _connectionStateManager.RemoveActiveConnectionAsync(connectionState.AttemptId);
                
                CancelPendingTimeoutTimer(connectionState.AttemptId);

                // Notify the pending connection that it's now active
                await Clients.Client(pendingConnection.ConnectionId).SendAsync("ConnectionActivated", "Your connection is now active.");

                // Publish ended event for the old connection
                await PublishActivityEndedEvent(connectionState, DisconnectReason.Disconnected);
            }
            else
            {
                // No pending connection - start grace period
                _logger.LogInformation("Starting grace period for AttemptId={AttemptId}", connectionState.AttemptId);
                
                var gracePeriodState = new GracePeriodState
                {
                    ConnectionId = connectionState.ConnectionId,
                    UserId = connectionState.UserId,
                    AssignmentId = connectionState.AssignmentId,
                    AttemptId = connectionState.AttemptId,
                    DisconnectedAt = DateTime.UtcNow
                };

                await _connectionStateManager.SetGracePeriodStateAsync(
                    gracePeriodState,
                    TimeSpan.FromSeconds(_timeoutConfig.GracePeriodSeconds + 5)); // Extra buffer

                await _connectionStateManager.RemoveActiveConnectionAsync(connectionState.AttemptId);

                // Start grace period timer
                StartGracePeriodTimer(connectionState, _timeoutConfig.GracePeriodSeconds);
            }

            await base.OnDisconnectedAsync(exception);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in OnDisconnectedAsync for ConnectionId={ConnectionId}", Context.ConnectionId);
            await base.OnDisconnectedAsync(exception);
        }
    }

    private void StartGracePeriodTimer(ConnectionState connectionState, int gracePeriodSeconds)
    {
        lock (_timerLock)
        {
            // Cancel existing timer if any
            CancelGracePeriodTimer(connectionState.AttemptId);

            var timer = new System.Threading.Timer(async _ =>
            {
                try
                {
                    _logger.LogInformation("Grace period expired for AttemptId={AttemptId}", connectionState.AttemptId);
                    
                    // Check if still in grace period (not reconnected)
                    var gracePeriodState = await _connectionStateManager.GetGracePeriodStateAsync(connectionState.AttemptId);
                    if (gracePeriodState != null)
                    {
                        await PublishActivityEndedEvent(connectionState, DisconnectReason.GracePeriodExpired);
                        await _connectionStateManager.RemoveGracePeriodStateAsync(connectionState.AttemptId);
                    }

                    CancelGracePeriodTimer(connectionState.AttemptId);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in grace period timer for AttemptId={AttemptId}", connectionState.AttemptId);
                }
            }, null, TimeSpan.FromSeconds(gracePeriodSeconds), Timeout.InfiniteTimeSpan);

            _gracePeriodTimers[connectionState.AttemptId] = timer;
        }
    }

    private void CancelGracePeriodTimer(string attemptId)
    {
        lock (_timerLock)
        {
            if (_gracePeriodTimers.TryGetValue(attemptId, out var timer))
            {
                timer?.Dispose();
                _gracePeriodTimers.Remove(attemptId);
                _logger.LogDebug("Grace period timer cancelled for AttemptId={AttemptId}", attemptId);
            }
        }
    }

    private void StartPendingTimeoutTimer(string attemptId, string userId, string assignmentId)
    {
        lock (_timerLock)
        {
            // Cancel existing timer if any
            CancelPendingTimeoutTimer(attemptId);

            var timer = new System.Threading.Timer(async _ =>
            {
                try
                {
                    _logger.LogInformation("Pending connection timeout for AttemptId={AttemptId}", attemptId);
                    
                    var pendingConnection = await _connectionStateManager.GetPendingConnectionAsync(attemptId);
                    if (pendingConnection != null)
                    {
                        // Notify client and disconnect
                        await Clients.Client(pendingConnection.ConnectionId).SendAsync("ConnectionRejected", "SessionSwitchTimeout");
                        await _connectionStateManager.RemovePendingConnectionAsync(attemptId);
                        
                        // Note: The actual disconnection will be handled by the client or OnDisconnectedAsync
                    }

                    CancelPendingTimeoutTimer(attemptId);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in pending timeout timer for AttemptId={AttemptId}", attemptId);
                }
            }, null, TimeSpan.FromSeconds(_timeoutConfig.PendingConnectionTimeoutSeconds), Timeout.InfiniteTimeSpan);

            _pendingTimeoutTimers[attemptId] = timer;
        }
    }

    private void CancelPendingTimeoutTimer(string attemptId)
    {
        lock (_timerLock)
        {
            if (_pendingTimeoutTimers.TryGetValue(attemptId, out var timer))
            {
                timer?.Dispose();
                _pendingTimeoutTimers.Remove(attemptId);
                _logger.LogDebug("Pending timeout timer cancelled for AttemptId={AttemptId}", attemptId);
            }
        }
    }

    private async Task PublishActivityEndedEvent(ConnectionState connectionState, DisconnectReason reason)
    {
        await _eventPublisher.PublishActivityEndedAsync(new StudentActivityEndedEvent
        {
            UserId = connectionState.UserId,
            AssignmentId = connectionState.AssignmentId,
            AttemptId = connectionState.AttemptId,
            ConnectionId = connectionState.ConnectionId,
            Reason = reason
        });
    }

    // Public method that can be called by the Kafka consumer to end a specific session
    public async Task EndSessionByCommand(string attemptId, string connectionId)
    {
        try
        {
            _logger.LogInformation("EndSessionCommand received for AttemptId={AttemptId}, ConnectionId={ConnectionId}",
                attemptId, connectionId);

            var activeConnection = await _connectionStateManager.GetActiveConnectionAsync(attemptId);
            
            if (activeConnection != null && activeConnection.ConnectionId == connectionId)
            {
                _logger.LogInformation("Terminating active connection for AttemptId={AttemptId}", attemptId);
                
                // Check for pending connection to promote
                var pendingConnection = await _connectionStateManager.GetPendingConnectionAsync(attemptId);
                
                // Close the active connection
                await Clients.Client(connectionId).SendAsync("ForceDisconnect", "Session ended by command");
                
                if (pendingConnection != null)
                {
                    // Promote pending to active
                    await _connectionStateManager.PromotePendingToActiveAsync(attemptId);
                    await Clients.Client(pendingConnection.ConnectionId).SendAsync("ConnectionActivated", "Your connection is now active.");
                    CancelPendingTimeoutTimer(attemptId);
                    
                    // Publish started event for the new connection
                    await _eventPublisher.PublishActivityStartedAsync(new StudentActivityStartedEvent
                    {
                        UserId = pendingConnection.UserId,
                        AssignmentId = pendingConnection.AssignmentId,
                        AttemptId = pendingConnection.AttemptId,
                        ConnectionId = pendingConnection.ConnectionId
                    });
                }
                
                // Publish ended event for terminated connection
                await PublishActivityEndedEvent(activeConnection, DisconnectReason.Disconnected);
                await _connectionStateManager.RemoveActiveConnectionAsync(attemptId);
            }
            else
            {
                _logger.LogWarning("EndSessionCommand: Connection not found or connectionId mismatch for AttemptId={AttemptId}", attemptId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in EndSessionByCommand for AttemptId={AttemptId}", attemptId);
        }
    }
}

