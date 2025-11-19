using WebSocketPlayground.Models;

namespace WebSocketPlayground.Services;

public interface IConnectionStateManager
{
    Task<ConnectionState?> GetActiveConnectionAsync(string attemptId);
    Task<bool> HasActiveConnectionForUserAndAssignmentAsync(string userId, string assignmentId);
    Task SetActiveConnectionAsync(ConnectionState connectionState, TimeSpan? expiration = null);
    Task RemoveActiveConnectionAsync(string attemptId);
    Task<GracePeriodState?> GetGracePeriodStateAsync(string attemptId);
    Task SetGracePeriodStateAsync(GracePeriodState gracePeriodState, TimeSpan expiration);
    Task RemoveGracePeriodStateAsync(string attemptId);
    Task<List<ConnectionState>> GetActiveConnectionsByUserIdAsync(string userId);
    Task<ConflictState?> GetConflictStateAsync(string userId);
    Task SetConflictStateAsync(ConflictState conflictState, TimeSpan expiration);
    Task RemoveConflictStateAsync(string userId);
}

