using WebSocketPlayground.Models;

namespace WebSocketPlayground.Services;

public interface IScheduledTaskManager
{
    /// <summary>
    /// Schedule a task to be executed at a specific time
    /// </summary>
    Task<string> ScheduleTaskAsync(string taskType, DateTime executeAt, string payload);
    
    /// <summary>
    /// Cancel a scheduled task by ID
    /// </summary>
    Task<bool> CancelTaskAsync(string taskId);
    
    /// <summary>
    /// Get all tasks that are due to be executed (ExecuteAt <= now)
    /// </summary>
    Task<List<ScheduledTask>> GetDueTasksAsync(DateTime now);
    
    /// <summary>
    /// Delete a task after execution
    /// </summary>
    Task DeleteTaskAsync(string taskId);
    
    /// <summary>
    /// Cancel all tasks of a specific type matching a filter
    /// </summary>
    Task<int> CancelTasksByTypeAsync(string taskType, Func<ScheduledTask, bool> filter);
}
