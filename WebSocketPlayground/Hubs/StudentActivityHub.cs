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
    private static readonly Dictionary<string, System.Threading.Timer> _conflictTimeoutTimers = new();
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
            var participationId = httpContext?.Request.Query["participationId"].ToString();

            if (string.IsNullOrEmpty(assignmentId) || string.IsNullOrEmpty(participationId))
            {
                _logger.LogWarning("Connection rejected: Missing assignmentId or participationId for user {UserId}", userId);
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
                StartConflictTimeoutTimer(userId);

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
                CancelGracePeriodTimer(participationId);
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
            var userId = Context.User?.FindFirst(ClaimTypes.NameIdentifier)?.Value 
                ?? Context.User?.FindFirst("sub")?.Value;

            if (string.IsNullOrEmpty(userId))
            {
                _logger.LogWarning("ResolveSessionConflict rejected: No user ID found in claims");
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
            CancelConflictTimeoutTimer(userId);

            if (choice.Equals("KeepNew", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogInformation("Resolving conflict: Keeping new connection for UserId={UserId}", userId);

                // Terminate old connection
                var oldConnection = await _connectionStateManager.GetActiveConnectionAsync(conflictState.OldParticipationId);
                if (oldConnection != null)
                {
                    await _connectionStateManager.RemoveActiveConnectionAsync(conflictState.OldParticipationId);
                    await _connectionStateManager.RemoveGracePeriodStateAsync(conflictState.OldParticipationId);
                    CancelGracePeriodTimer(conflictState.OldParticipationId);
                    
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

            var userId = Context.User?.FindFirst(ClaimTypes.NameIdentifier)?.Value 
                ?? Context.User?.FindFirst("sub")?.Value;

            if (string.IsNullOrEmpty(userId))
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

                    CancelConflictTimeoutTimer(userId);
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

                    CancelConflictTimeoutTimer(userId);
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
            StartGracePeriodTimer(connectionState, _timeoutConfig.GracePeriodSeconds);

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
            CancelGracePeriodTimer(connectionState.ParticipationId);

            var timer = new Timer(async _ =>
            {
                try
                {
                    _logger.LogInformation("Grace period expired for ParticipationId={ParticipationId}", connectionState.ParticipationId);
                    
                    // Check if still in grace period (not reconnected)
                    var gracePeriodState = await _connectionStateManager.GetGracePeriodStateAsync(connectionState.ParticipationId);
                    if (gracePeriodState != null)
                    {
                        await PublishActivityEndedEvent(connectionState, DisconnectReason.GracePeriodExpired);
                        await _connectionStateManager.RemoveGracePeriodStateAsync(connectionState.ParticipationId);
                    }

                    CancelGracePeriodTimer(connectionState.ParticipationId);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in grace period timer for ParticipationId={ParticipationId}", connectionState.ParticipationId);
                }
            }, null, TimeSpan.FromSeconds(gracePeriodSeconds), Timeout.InfiniteTimeSpan);

            _gracePeriodTimers[connectionState.ParticipationId] = timer;
        }
    }

    private void CancelGracePeriodTimer(string participationId)
    {
        lock (_timerLock)
        {
            if (_gracePeriodTimers.TryGetValue(participationId, out var timer))
            {
                timer?.Dispose();
                _gracePeriodTimers.Remove(participationId);
                _logger.LogDebug("Grace period timer cancelled for ParticipationId={ParticipationId}", participationId);
            }
        }
    }

    private void StartConflictTimeoutTimer(string userId)
    {
        lock (_timerLock)
        {
            // Cancel existing timer if any
            CancelConflictTimeoutTimer(userId);

            var timer = new Timer(async _ =>
            {
                try
                {
                    _logger.LogInformation("Conflict resolution timeout for UserId={UserId}", userId);
                    
                    var conflictState = await _connectionStateManager.GetConflictStateAsync(userId);
                    if (conflictState != null)
                    {
                        // Auto-reject new connection on timeout
                        _logger.LogInformation("Auto-rejecting new connection due to timeout: UserId={UserId}", userId);

                        await Clients.Client(conflictState.NewConnectionId).SendAsync("ConflictTimeout", new
                        {
                            message = "Connection timed out - no response to conflict resolution"
                        });

                        await Clients.Client(conflictState.OldConnectionId).SendAsync("ConflictResolved", new
                        {
                            result = "active",
                            message = "Your session remains active (conflict resolution timed out)"
                        });

                        await _connectionStateManager.RemoveConflictStateAsync(userId);
                    }

                    CancelConflictTimeoutTimer(userId);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in conflict timeout timer for UserId={UserId}", userId);
                }
            }, null, TimeSpan.FromSeconds(_timeoutConfig.ConflictResolutionTimeoutSeconds), Timeout.InfiniteTimeSpan);

            _conflictTimeoutTimers[userId] = timer;
        }
    }

    private void CancelConflictTimeoutTimer(string userId)
    {
        lock (_timerLock)
        {
            if (_conflictTimeoutTimers.TryGetValue(userId, out var timer))
            {
                timer?.Dispose();
                _conflictTimeoutTimers.Remove(userId);
                _logger.LogDebug("Conflict timeout timer cancelled for UserId={UserId}", userId);
            }
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

