using System.Text.Json;
using StackExchange.Redis;
using WebSocketPlayground.Models;

namespace WebSocketPlayground.Services;

public class ScheduledTaskManager : IScheduledTaskManager
{
    private readonly IConnectionMultiplexer _redis;
    private readonly IDatabase _db;
    private readonly ILogger<ScheduledTaskManager> _logger;
    private const string TaskKeyPrefix = "signalr:scheduled:";
    private const string TaskIndexKey = "signalr:scheduled:index"; // Sorted set for time-based queries

    public ScheduledTaskManager(
        IConnectionMultiplexer redis,
        ILogger<ScheduledTaskManager> logger)
    {
        _redis = redis;
        _db = redis.GetDatabase();
        _logger = logger;
    }

    public async Task<string> ScheduleTaskAsync(string taskType, DateTime executeAt, string payload)
    {
        var taskId = Guid.NewGuid().ToString();
        var task = new ScheduledTask
        {
            TaskId = taskId,
            TaskType = taskType,
            ExecuteAt = executeAt,
            Payload = payload,
            CreatedAt = DateTime.UtcNow
        };

        var taskJson = JsonSerializer.Serialize(task);
        var key = $"{TaskKeyPrefix}{taskId}";
        
        // Calculate TTL: ExecuteAt + 60 seconds buffer
        var ttl = executeAt.AddSeconds(60) - DateTime.UtcNow;
        if (ttl.TotalSeconds < 1)
        {
            ttl = TimeSpan.FromSeconds(60); // Minimum 60 seconds
        }

        // Store task data with TTL
        await _db.StringSetAsync(key, taskJson, ttl);
        
        // Add to sorted set with ExecuteAt as score for efficient time-based queries
        await _db.SortedSetAddAsync(TaskIndexKey, taskId, executeAt.Ticks);

        _logger.LogDebug("Scheduled task {TaskId} of type {TaskType} to execute at {ExecuteAt}", 
            taskId, taskType, executeAt);

        return taskId;
    }

    public async Task<bool> CancelTaskAsync(string taskId)
    {
        var key = $"{TaskKeyPrefix}{taskId}";
        
        // Remove from both the task store and the index
        var taskDeleted = await _db.KeyDeleteAsync(key);
        var indexDeleted = await _db.SortedSetRemoveAsync(TaskIndexKey, taskId);

        if (taskDeleted || indexDeleted)
        {
            _logger.LogDebug("Cancelled task {TaskId}", taskId);
            return true;
        }

        _logger.LogDebug("Task {TaskId} not found for cancellation", taskId);
        return false;
    }

    public async Task<List<ScheduledTask>> GetDueTasksAsync(DateTime now)
    {
        // Get all task IDs from sorted set where score (ExecuteAt.Ticks) <= now.Ticks
        var dueTaskIds = await _db.SortedSetRangeByScoreAsync(
            TaskIndexKey,
            start: 0,
            stop: now.Ticks);

        var dueTasks = new List<ScheduledTask>();

        foreach (var taskId in dueTaskIds)
        {
            var key = $"{TaskKeyPrefix}{taskId}";
            var taskJson = await _db.StringGetAsync(key);

            if (!taskJson.IsNullOrEmpty)
            {
                var task = JsonSerializer.Deserialize<ScheduledTask>(taskJson!);
                if (task != null)
                {
                    dueTasks.Add(task);
                }
            }
            else
            {
                // Task expired or was deleted, remove from index
                await _db.SortedSetRemoveAsync(TaskIndexKey, taskId);
            }
        }

        return dueTasks;
    }

    public async Task DeleteTaskAsync(string taskId)
    {
        var key = $"{TaskKeyPrefix}{taskId}";
        await _db.KeyDeleteAsync(key);
        await _db.SortedSetRemoveAsync(TaskIndexKey, taskId);
        
        _logger.LogDebug("Deleted task {TaskId}", taskId);
    }

    public async Task<int> CancelTasksByTypeAsync(string taskType, Func<ScheduledTask, bool> filter)
    {
        // Get all task IDs from the index
        var allTaskIds = await _db.SortedSetRangeByScoreAsync(TaskIndexKey);
        
        var cancelledCount = 0;

        foreach (var taskId in allTaskIds)
        {
            var key = $"{TaskKeyPrefix}{taskId}";
            var taskJson = await _db.StringGetAsync(key);

            if (!taskJson.IsNullOrEmpty)
            {
                var task = JsonSerializer.Deserialize<ScheduledTask>(taskJson!);
                if (task != null && task.TaskType == taskType && filter(task))
                {
                    await CancelTaskAsync(task.TaskId);
                    cancelledCount++;
                }
            }
        }

        _logger.LogDebug("Cancelled {Count} tasks of type {TaskType}", cancelledCount, taskType);
        return cancelledCount;
    }
}

