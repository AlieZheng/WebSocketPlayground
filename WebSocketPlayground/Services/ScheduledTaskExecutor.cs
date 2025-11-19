using System.Text.Json;
using Microsoft.AspNetCore.SignalR;
using WebSocketPlayground.Hubs;
using WebSocketPlayground.Models;

namespace WebSocketPlayground.Services;

public class ScheduledTaskExecutor : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<ScheduledTaskExecutor> _logger;
    private readonly IHubContext<StudentActivityHub> _hubContext;

    public ScheduledTaskExecutor(
        IServiceProvider serviceProvider,
        ILogger<ScheduledTaskExecutor> logger,
        IHubContext<StudentActivityHub> hubContext)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _hubContext = hubContext;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("ScheduledTaskExecutor started");

        // Small delay to ensure services are fully initialized
        await Task.Delay(2000, stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProcessDueTasksAsync(stoppingToken);
                
                // Check every second for due tasks
                await Task.Delay(1000, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                // Expected when stopping
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in ScheduledTaskExecutor loop");
                
                // Wait a bit before retrying on error
                await Task.Delay(5000, stoppingToken);
            }
        }

        _logger.LogInformation("ScheduledTaskExecutor stopped");
    }

    private async Task ProcessDueTasksAsync(CancellationToken stoppingToken)
    {
        using var scope = _serviceProvider.CreateScope();
        var taskManager = scope.ServiceProvider.GetRequiredService<IScheduledTaskManager>();
        var connectionStateManager = scope.ServiceProvider.GetRequiredService<IConnectionStateManager>();
        var eventPublisher = scope.ServiceProvider.GetRequiredService<IActivityEventPublisher>();

        var now = DateTime.UtcNow;
        var dueTasks = await taskManager.GetDueTasksAsync(now);

        foreach (var task in dueTasks)
        {
            if (stoppingToken.IsCancellationRequested)
                break;

            try
            {
                _logger.LogInformation("Executing scheduled task {TaskId} of type {TaskType}", 
                    task.TaskId, task.TaskType);

                await ExecuteTaskAsync(task, connectionStateManager, eventPublisher, stoppingToken);
                await taskManager.DeleteTaskAsync(task.TaskId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing task {TaskId} of type {TaskType}", 
                    task.TaskId, task.TaskType);
                
                // Still delete the task to avoid infinite retries
                await taskManager.DeleteTaskAsync(task.TaskId);
            }
        }
    }

    private async Task ExecuteTaskAsync(
        ScheduledTask task,
        IConnectionStateManager connectionStateManager,
        IActivityEventPublisher eventPublisher,
        CancellationToken cancellationToken)
    {
        switch (task.TaskType)
        {
            case "GracePeriod":
                await ExecuteGracePeriodTaskAsync(task, connectionStateManager, eventPublisher);
                break;

            case "ConflictTimeout":
                await ExecuteConflictTimeoutTaskAsync(task, connectionStateManager, cancellationToken);
                break;

            default:
                _logger.LogWarning("Unknown task type: {TaskType}", task.TaskType);
                break;
        }
    }

    private async Task ExecuteGracePeriodTaskAsync(
        ScheduledTask task,
        IConnectionStateManager connectionStateManager,
        IActivityEventPublisher eventPublisher)
    {
        var payload = JsonSerializer.Deserialize<GracePeriodTaskPayload>(task.Payload);
        if (payload == null)
        {
            _logger.LogWarning("Invalid GracePeriodTaskPayload for task {TaskId}", task.TaskId);
            return;
        }

        _logger.LogInformation("Grace period expired for ParticipationId={ParticipationId}", 
            payload.ParticipationId);

        // Check if still in grace period (not reconnected)
        var gracePeriodState = await connectionStateManager.GetGracePeriodStateAsync(payload.ParticipationId);
        if (gracePeriodState != null)
        {
            // Publish activity ended event
            await eventPublisher.PublishActivityEndedAsync(new StudentActivityEndedEvent
            {
                UserId = payload.UserId,
                AssignmentId = payload.AssignmentId,
                ParticipationId = payload.ParticipationId,
                ConnectionId = payload.ConnectionId,
                Reason = DisconnectReason.GracePeriodExpired
            });

            // Remove grace period state
            await connectionStateManager.RemoveGracePeriodStateAsync(payload.ParticipationId);
            
            _logger.LogInformation("Published activity ended event for ParticipationId={ParticipationId}", 
                payload.ParticipationId);
        }
        else
        {
            _logger.LogDebug("Grace period state not found for ParticipationId={ParticipationId} - already reconnected or cleaned up", 
                payload.ParticipationId);
        }
    }

    private async Task ExecuteConflictTimeoutTaskAsync(
        ScheduledTask task,
        IConnectionStateManager connectionStateManager,
        CancellationToken cancellationToken)
    {
        var payload = JsonSerializer.Deserialize<ConflictTimeoutTaskPayload>(task.Payload);
        if (payload == null)
        {
            _logger.LogWarning("Invalid ConflictTimeoutTaskPayload for task {TaskId}", task.TaskId);
            return;
        }

        _logger.LogInformation("Conflict resolution timeout for UserId={UserId}", payload.UserId);

        var conflictState = await connectionStateManager.GetConflictStateAsync(payload.UserId);
        if (conflictState != null)
        {
            // Auto-reject new connection on timeout
            _logger.LogInformation("Auto-rejecting new connection due to timeout: UserId={UserId}", 
                payload.UserId);

            await _hubContext.Clients.Client(payload.NewConnectionId).SendAsync(
                "ConflictTimeout",
                new
                {
                    message = "Connection timed out - no response to conflict resolution"
                },
                cancellationToken);

            await _hubContext.Clients.Client(payload.OldConnectionId).SendAsync(
                "ConflictResolved",
                new
                {
                    result = "active",
                    message = "Your session remains active (conflict resolution timed out)"
                },
                cancellationToken);

            await connectionStateManager.RemoveConflictStateAsync(payload.UserId);
            
            _logger.LogInformation("Conflict timeout processed for UserId={UserId}", payload.UserId);
        }
        else
        {
            _logger.LogDebug("Conflict state not found for UserId={UserId} - already resolved", 
                payload.UserId);
        }
    }
}

