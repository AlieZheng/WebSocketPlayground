using WebSocketPlayground.Models;

namespace WebSocketPlayground.Services;

public interface IConnectionStateManager
{
    Task<ConnectionState?> GetActiveConnectionAsync(Guid participationId);
    Task<bool> HasActiveConnectionForUserAndAssignmentAsync(Guid userId, Guid assignmentId);
    Task SetActiveConnectionAsync(ConnectionState connectionState, TimeSpan? expiration = null);
    Task RemoveActiveConnectionAsync(Guid participationId);
    Task<GracePeriodState?> GetGracePeriodStateAsync(Guid participationId);
    Task SetGracePeriodStateAsync(GracePeriodState gracePeriodState, TimeSpan expiration);
    Task RemoveGracePeriodStateAsync(Guid participationId);
    Task<List<ConnectionState>> GetActiveConnectionsByUserIdAsync(Guid userId);
    Task<ConflictState?> GetConflictStateAsync(Guid userId);
    Task SetConflictStateAsync(ConflictState conflictState, TimeSpan expiration);
    Task RemoveConflictStateAsync(Guid userId);
}

