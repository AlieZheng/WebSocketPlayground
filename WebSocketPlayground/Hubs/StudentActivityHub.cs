using System.Security.Claims;
using System.Text.Json;
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
    private readonly IScheduledTaskManager _scheduledTaskManager;
    private readonly TimeoutConfiguration _timeoutConfig;
    private readonly ILogger<StudentActivityHub> _logger;

    public StudentActivityHub(
        IConnectionStateManager connectionStateManager,
        IActivityEventPublisher eventPublisher,
        IScheduledTaskManager scheduledTaskManager,
        TimeoutConfiguration timeoutConfig,
        ILogger<StudentActivityHub> logger)
    {
        _connectionStateManager = connectionStateManager;
        _eventPublisher = eventPublisher;
        _scheduledTaskManager = scheduledTaskManager;
        _timeoutConfig = timeoutConfig;
        _logger = logger;
    }

    public override async Task OnConnectedAsync()
    {
        try
        {
            // Extract user ID from JWT claims and parse as Guid
            var userIdStr = Context.User?.FindFirst(ClaimTypes.NameIdentifier)?.Value 
                ?? Context.User?.FindFirst("sub")?.Value;
            
            if (string.IsNullOrEmpty(userIdStr) || !Guid.TryParse(userIdStr, out var userId))
            {
                _logger.LogWarning("Connection rejected: No valid user ID found in claims");
                await Clients.Caller.SendAsync("ConnectionRejected", "Missing userId in token");
                Context.Abort();
                return;
            }

            // Get query parameters and parse as Guids
            var httpContext = Context.GetHttpContext();
            var assignmentIdStr = httpContext?.Request.Query["assignmentId"].ToString();
            var participationIdStr = httpContext?.Request.Query["participationId"].ToString();

            if (string.IsNullOrEmpty(assignmentIdStr) || string.IsNullOrEmpty(participationIdStr) ||
                !Guid.TryParse(assignmentIdStr, out var assignmentId) || 
                !Guid.TryParse(participationIdStr, out var participationId))
            {
                _logger.LogWarning("Connection rejected: Missing or invalid assignmentId or participationId for user {UserId}", userId);
                await Clients.Caller.SendAsync("ConnectionRejected", "Missing required parameters");
                Context.Abort();
                return;
            }

            _logger.LogInformation("Connection attempt: UserId={UserId}, AssignmentId={AssignmentId}, ParticipationId={ParticipationId}, ConnectionId={ConnectionId}",
                userId, assignmentId, participationId, Context.ConnectionId);

            // Check if there's an existing conflict being resolved
            var existingConflict = await _connectionStateManager.GetConflictStateAsync(userId);
            if (existingConflict != null)
            {
                _logger.LogWarning("Connection rejected: User {UserId} already has an unresolved conflict", userId);
                await Clients.Caller.SendAsync("ConnectionRejected", "Another connection conflict is being resolved");
                Context.Abort();
                return;
            }

            // Check for any active connections from this user (student can only attempt 1 assignment at a time)
            var userActiveConnections = await _connectionStateManager.GetActiveConnectionsByUserIdAsync(userId);
            
            if (userActiveConnections.Any())
            {
                var existingConnection = userActiveConnections.First();
                
                _logger.LogInformation("Duplicate connection detected: UserId={UserId}, OldParticipationId={OldParticipationId}, NewParticipationId={NewParticipationId}",
                    userId, existingConnection.ParticipationId, participationId);

                // Create conflict state
                var conflictState = new ConflictState
                {
                    UserId = userId,
                    OldConnectionId = existingConnection.ConnectionId,
                    NewConnectionId = Context.ConnectionId,
                    OldAssignmentId = existingConnection.AssignmentId,
                    NewAssignmentId = assignmentId,
                    OldParticipationId = existingConnection.ParticipationId,
                    NewParticipationId = participationId
                };

                await _connectionStateManager.SetConflictStateAsync(
                    conflictState,
                    TimeSpan.FromSeconds(_timeoutConfig.ConflictResolutionTimeoutSeconds + 5)); // Extra buffer

                // Start timeout timer for conflict resolution
                await StartConflictTimeoutTimerAsync(userId, existingConnection.ConnectionId, Context.ConnectionId);

                // Notify both connections about the conflict
                await Clients.Client(existingConnection.ConnectionId).SendAsync("SessionConflict", new
                {
                    message = "Another connection attempt detected",
                    oldParticipationId = existingConnection.ParticipationId,
                    newParticipationId = participationId,
                    oldAssignmentId = existingConnection.AssignmentId,
                    newAssignmentId = assignmentId,
                    isOldConnection = true
                });

                await Clients.Caller.SendAsync("SessionConflict", new
                {
                    message = "Existing session detected",
                    oldParticipationId = existingConnection.ParticipationId,
                    newParticipationId = participationId,
                    oldAssignmentId = existingConnection.AssignmentId,
                    newAssignmentId = assignmentId,
                    isOldConnection = false
                });

                _logger.LogInformation("Session conflict notification sent to both connections for UserId={UserId}", userId);
                return;
            }

            // Check for grace period state (reconnection scenario)
            var gracePeriodState = await _connectionStateManager.GetGracePeriodStateAsync(participationId);
            if (gracePeriodState != null)
            {
                _logger.LogInformation("Reconnection during grace period for ParticipationId={ParticipationId}", participationId);
                
                // Cancel grace period
                await _connectionStateManager.RemoveGracePeriodStateAsync(participationId);
                await CancelGracePeriodTimerAsync(participationId);
            }

            // Create active connection state
            var connectionState = new ConnectionState
            {
                ConnectionId = Context.ConnectionId,
                UserId = userId,
                AssignmentId = assignmentId,
                ParticipationId = participationId,
                IsPending = false
            };

            await _connectionStateManager.SetActiveConnectionAsync(connectionState);

            // Publish activity started event (only if not a reconnection)
            if (gracePeriodState == null)
            {
                await _eventPublisher.PublishActivityStartedAsync(new StudentActivityStartedEvent
                {
                    UserId = userId,
                    AssignmentId = assignmentId,
                    ParticipationId = participationId,
                    ConnectionId = Context.ConnectionId
                });
            }

            _logger.LogInformation("Connection established: UserId={UserId}, ParticipationId={ParticipationId}, ConnectionId={ConnectionId}",
                userId, participationId, Context.ConnectionId);

            await base.OnConnectedAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in OnConnectedAsync for ConnectionId={ConnectionId}", Context.ConnectionId);
            Context.Abort();
        }
    }

    public async Task ResolveSessionConflict(string choice)
    {
        try
        {
            var userIdStr = Context.User?.FindFirst(ClaimTypes.NameIdentifier)?.Value 
                ?? Context.User?.FindFirst("sub")?.Value;

            if (string.IsNullOrEmpty(userIdStr) || !Guid.TryParse(userIdStr, out var userId))
            {
                _logger.LogWarning("ResolveSessionConflict rejected: No valid user ID found in claims");
                return;
            }

            _logger.LogInformation("ResolveSessionConflict called: UserId={UserId}, Choice={Choice}, ConnectionId={ConnectionId}",
                userId, choice, Context.ConnectionId);

            // Get conflict state
            var conflictState = await _connectionStateManager.GetConflictStateAsync(userId);
            if (conflictState == null)
            {
                _logger.LogWarning("ResolveSessionConflict: No conflict found for UserId={UserId}. May have already been resolved.", userId);
                return;
            }

            // Validate that the caller is one of the connections in conflict
            if (Context.ConnectionId != conflictState.OldConnectionId && 
                Context.ConnectionId != conflictState.NewConnectionId)
            {
                _logger.LogWarning("ResolveSessionConflict: ConnectionId={ConnectionId} is not part of the conflict for UserId={UserId}",
                    Context.ConnectionId, userId);
                return;
            }

            // Cancel timeout timer
            await CancelConflictTimeoutTimerAsync(userId);

            if (choice.Equals("KeepNew", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogInformation("Resolving conflict: Keeping new connection for UserId={UserId}", userId);

                // Terminate old connection
                var oldConnection = await _connectionStateManager.GetActiveConnectionAsync(conflictState.OldParticipationId);
                if (oldConnection != null)
                {
                    await _connectionStateManager.RemoveActiveConnectionAsync(conflictState.OldParticipationId);
                    await _connectionStateManager.RemoveGracePeriodStateAsync(conflictState.OldParticipationId);
                    await CancelGracePeriodTimerAsync(conflictState.OldParticipationId);
                    
                    await PublishActivityEndedEvent(oldConnection, DisconnectReason.SwitchedAssignment);
                    await Clients.Client(conflictState.OldConnectionId).SendAsync("ConflictResolved", new
                    {
                        result = "terminated",
                        message = "Your session was terminated"
                    });
                }

                // Activate new connection
                var newConnectionState = new ConnectionState
                {
                    ConnectionId = conflictState.NewConnectionId,
                    UserId = userId,
                    AssignmentId = conflictState.NewAssignmentId,
                    ParticipationId = conflictState.NewParticipationId,
                    IsPending = false
                };

                await _connectionStateManager.SetActiveConnectionAsync(newConnectionState);

                await _eventPublisher.PublishActivityStartedAsync(new StudentActivityStartedEvent
                {
                    UserId = userId,
                    AssignmentId = conflictState.NewAssignmentId,
                    ParticipationId = conflictState.NewParticipationId,
                    ConnectionId = conflictState.NewConnectionId
                });

                await Clients.Client(conflictState.NewConnectionId).SendAsync("ConflictResolved", new
                {
                    result = "activated",
                    message = "Your session is now active"
                });
            }
            else if (choice.Equals("KeepOld", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogInformation("Resolving conflict: Keeping old connection for UserId={UserId}", userId);

                // Notify new connection before disconnecting it
                await Clients.Client(conflictState.NewConnectionId).SendAsync("ConflictResolved", new
                {
                    result = "rejected",
                    message = "Connection rejected - keeping existing session"
                });

                // Confirm old connection remains active
                await Clients.Client(conflictState.OldConnectionId).SendAsync("ConflictResolved", new
                {
                    result = "active",
                    message = "Your session remains active"
                });

                // Give client brief moment to receive message, then force disconnect
                // Note: We can't use Context here since this method may be called from either connection
                // The client should disconnect itself upon receiving the "rejected" message
                // If it doesn't, the connection will remain open but in an inactive state (not in active connections list)
            }
            else
            {
                _logger.LogWarning("ResolveSessionConflict: Invalid choice '{Choice}' for UserId={UserId}", choice, userId);
                return;
            }

            // Clean up conflict state
            await _connectionStateManager.RemoveConflictStateAsync(userId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in ResolveSessionConflict for ConnectionId={ConnectionId}", Context.ConnectionId);
        }
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        try
        {
            _logger.LogInformation("Disconnection: ConnectionId={ConnectionId}, Exception={Exception}",
                Context.ConnectionId, exception?.Message);

            var userIdStr = Context.User?.FindFirst(ClaimTypes.NameIdentifier)?.Value 
                ?? Context.User?.FindFirst("sub")?.Value;

            if (string.IsNullOrEmpty(userIdStr) || !Guid.TryParse(userIdStr, out var userId))
            {
                await base.OnDisconnectedAsync(exception);
                return;
            }

            // Check if this connection is part of an unresolved conflict
            var conflictState = await _connectionStateManager.GetConflictStateAsync(userId);
            if (conflictState != null)
            {
                if (Context.ConnectionId == conflictState.OldConnectionId)
                {
                    _logger.LogInformation("Old connection disconnected during conflict for UserId={UserId}. Auto-resolving to keep new connection.", userId);
                    
                    // Auto-resolve: keep the new connection
                    var oldConnection = await _connectionStateManager.GetActiveConnectionAsync(conflictState.OldParticipationId);
                    if (oldConnection != null)
                    {
                        await _connectionStateManager.RemoveActiveConnectionAsync(conflictState.OldParticipationId);
                        await PublishActivityEndedEvent(oldConnection, DisconnectReason.Disconnected);
                    }

                    // Activate new connection
                    var newConnectionState = new ConnectionState
                    {
                        ConnectionId = conflictState.NewConnectionId,
                        UserId = userId,
                        AssignmentId = conflictState.NewAssignmentId,
                        ParticipationId = conflictState.NewParticipationId,
                        IsPending = false
                    };

                    await _connectionStateManager.SetActiveConnectionAsync(newConnectionState);

                    await _eventPublisher.PublishActivityStartedAsync(new StudentActivityStartedEvent
                    {
                        UserId = userId,
                        AssignmentId = conflictState.NewAssignmentId,
                        ParticipationId = conflictState.NewParticipationId,
                        ConnectionId = conflictState.NewConnectionId
                    });

                    await Clients.Client(conflictState.NewConnectionId).SendAsync("ConflictResolved", new
                    {
                        result = "activated",
                        message = "Your session is now active (old session disconnected)"
                    });

                    await CancelConflictTimeoutTimerAsync(userId);
                    await _connectionStateManager.RemoveConflictStateAsync(userId);
                }
                else if (Context.ConnectionId == conflictState.NewConnectionId)
                {
                    _logger.LogInformation("New connection disconnected during conflict for UserId={UserId}. Keeping old connection active.", userId);
                    
                    // Just clean up - old connection stays active
                    await Clients.Client(conflictState.OldConnectionId).SendAsync("ConflictResolved", new
                    {
                        result = "active",
                        message = "Your session remains active (new connection disconnected)"
                    });

                    await CancelConflictTimeoutTimerAsync(userId);
                    await _connectionStateManager.RemoveConflictStateAsync(userId);
                }

                await base.OnDisconnectedAsync(exception);
                return;
            }

            // Search for the connection in active connections
            var userConnections = await _connectionStateManager.GetActiveConnectionsByUserIdAsync(userId);
            var connectionState = userConnections.FirstOrDefault(c => c.ConnectionId == Context.ConnectionId);

            if (connectionState == null)
            {
                _logger.LogInformation("Disconnected connection not found in active connections: ConnectionId={ConnectionId}", Context.ConnectionId);
                await base.OnDisconnectedAsync(exception);
                return;
            }

            // Start grace period for reconnection
            _logger.LogInformation("Starting grace period for ParticipationId={ParticipationId}", connectionState.ParticipationId);
            
            var gracePeriodState = new GracePeriodState
            {
                ConnectionId = connectionState.ConnectionId,
                UserId = connectionState.UserId,
                AssignmentId = connectionState.AssignmentId,
                ParticipationId = connectionState.ParticipationId,
                DisconnectedAt = DateTime.UtcNow
            };

            await _connectionStateManager.SetGracePeriodStateAsync(
                gracePeriodState,
                TimeSpan.FromSeconds(_timeoutConfig.GracePeriodSeconds + 5)); // Extra buffer

            await _connectionStateManager.RemoveActiveConnectionAsync(connectionState.ParticipationId);

            // Start grace period timer
            await StartGracePeriodTimerAsync(connectionState, _timeoutConfig.GracePeriodSeconds);

            await base.OnDisconnectedAsync(exception);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in OnDisconnectedAsync for ConnectionId={ConnectionId}", Context.ConnectionId);
            await base.OnDisconnectedAsync(exception);
        }
    }

    private async Task StartGracePeriodTimerAsync(ConnectionState connectionState, int gracePeriodSeconds)
    {
        // Cancel existing timer if any
        await CancelGracePeriodTimerAsync(connectionState.ParticipationId);

        var executeAt = DateTime.UtcNow.AddSeconds(gracePeriodSeconds);
        var payload = new GracePeriodTaskPayload
        {
            ParticipationId = connectionState.ParticipationId,
            UserId = connectionState.UserId,
            AssignmentId = connectionState.AssignmentId,
            ConnectionId = connectionState.ConnectionId
        };

        var payloadJson = JsonSerializer.Serialize(payload);
        var taskId = await _scheduledTaskManager.ScheduleTaskAsync("GracePeriod", executeAt, payloadJson);
        
        _logger.LogDebug("Scheduled grace period task {TaskId} for ParticipationId={ParticipationId}, executeAt={ExecuteAt}",
            taskId, connectionState.ParticipationId, executeAt);
    }

    private async Task CancelGracePeriodTimerAsync(Guid participationId)
    {
        // Cancel all grace period tasks for this participationId
        var cancelled = await _scheduledTaskManager.CancelTasksByTypeAsync("GracePeriod", task =>
        {
            try
            {
                var payload = JsonSerializer.Deserialize<GracePeriodTaskPayload>(task.Payload);
                return payload?.ParticipationId == participationId;
            }
            catch
            {
                return false;
            }
        });

        if (cancelled > 0)
        {
            _logger.LogDebug("Cancelled {Count} grace period timer(s) for ParticipationId={ParticipationId}", 
                cancelled, participationId);
        }
    }

    private async Task StartConflictTimeoutTimerAsync(Guid userId, string oldConnectionId, string newConnectionId)
    {
        // Cancel existing timer if any
        await CancelConflictTimeoutTimerAsync(userId);

        var executeAt = DateTime.UtcNow.AddSeconds(_timeoutConfig.ConflictResolutionTimeoutSeconds);
        var payload = new ConflictTimeoutTaskPayload
        {
            UserId = userId,
            OldConnectionId = oldConnectionId,
            NewConnectionId = newConnectionId
        };

        var payloadJson = JsonSerializer.Serialize(payload);
        var taskId = await _scheduledTaskManager.ScheduleTaskAsync("ConflictTimeout", executeAt, payloadJson);
        
        _logger.LogDebug("Scheduled conflict timeout task {TaskId} for UserId={UserId}, executeAt={ExecuteAt}",
            taskId, userId, executeAt);
    }

    private async Task CancelConflictTimeoutTimerAsync(Guid userId)
    {
        // Cancel all conflict timeout tasks for this userId
        var cancelled = await _scheduledTaskManager.CancelTasksByTypeAsync("ConflictTimeout", task =>
        {
            try
            {
                var payload = JsonSerializer.Deserialize<ConflictTimeoutTaskPayload>(task.Payload);
                return payload?.UserId == userId;
            }
            catch
            {
                return false;
            }
        });

        if (cancelled > 0)
        {
            _logger.LogDebug("Cancelled {Count} conflict timeout timer(s) for UserId={UserId}", 
                cancelled, userId);
        }
    }

    private async Task PublishActivityEndedEvent(ConnectionState connectionState, DisconnectReason reason)
    {
        await _eventPublisher.PublishActivityEndedAsync(new StudentActivityEndedEvent
        {
            UserId = connectionState.UserId,
            AssignmentId = connectionState.AssignmentId,
            ParticipationId = connectionState.ParticipationId,
            ConnectionId = connectionState.ConnectionId,
            Reason = reason
        });
    }
}

