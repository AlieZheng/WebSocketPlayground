using WebSocketPlayground.Models;

namespace WebSocketPlayground.Services;

public interface IConnectionStateManager
{
    Task<ConnectionState?> GetActiveConnectionAsync(string participationId);
    Task<bool> HasActiveConnectionForUserAndAssignmentAsync(string userId, string assignmentId);
    Task SetActiveConnectionAsync(ConnectionState connectionState, TimeSpan? expiration = null);
    Task RemoveActiveConnectionAsync(string participationId);
    Task<GracePeriodState?> GetGracePeriodStateAsync(string participationId);
    Task SetGracePeriodStateAsync(GracePeriodState gracePeriodState, TimeSpan expiration);
    Task RemoveGracePeriodStateAsync(string participationId);
    Task<List<ConnectionState>> GetActiveConnectionsByUserIdAsync(string userId);
    Task<ConflictState?> GetConflictStateAsync(string userId);
    Task SetConflictStateAsync(ConflictState conflictState, TimeSpan expiration);
    Task RemoveConflictStateAsync(string userId);
}

