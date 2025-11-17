using WebSocketPlayground.Models;

namespace WebSocketPlayground.Services;

public interface IConnectionStateManager
{
    Task<ConnectionState?> GetActiveConnectionAsync(string attemptId);
    Task<ConnectionState?> GetPendingConnectionAsync(string attemptId);
    Task<bool> HasActiveConnectionForUserAndAssignmentAsync(string userId, string assignmentId);
    Task SetActiveConnectionAsync(ConnectionState connectionState, TimeSpan? expiration = null);
    Task SetPendingConnectionAsync(ConnectionState connectionState, TimeSpan expiration);
    Task PromotePendingToActiveAsync(string attemptId);
    Task RemoveActiveConnectionAsync(string attemptId);
    Task RemovePendingConnectionAsync(string attemptId);
    Task<GracePeriodState?> GetGracePeriodStateAsync(string attemptId);
    Task SetGracePeriodStateAsync(GracePeriodState gracePeriodState, TimeSpan expiration);
    Task RemoveGracePeriodStateAsync(string attemptId);
    Task<List<ConnectionState>> GetActiveConnectionsByUserIdAsync(string userId);
}

