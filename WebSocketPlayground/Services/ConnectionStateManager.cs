using System.Text.Json;
using StackExchange.Redis;
using WebSocketPlayground.Models;

namespace WebSocketPlayground.Services;

public class ConnectionStateManager : IConnectionStateManager
{
    private readonly IConnectionMultiplexer _redis;
    private readonly IDatabase _db;
    private const string ConnectionKeyPrefix = "signalr:connection:";
    private const string ConflictKeyPrefix = "signalr:conflict:";
    private const string GracePeriodKeyPrefix = "signalr:grace:";
    private const string UserIndexPrefix = "signalr:user:";

    public ConnectionStateManager(IConnectionMultiplexer redis)
    {
        _redis = redis;
        _db = redis.GetDatabase();
    }

    public async Task<ConnectionState?> GetActiveConnectionAsync(Guid participationId)
    {
        var key = $"{ConnectionKeyPrefix}{participationId}";
        var value = await _db.StringGetAsync(key);
        
        if (value.IsNullOrEmpty)
            return null;

        return JsonSerializer.Deserialize<ConnectionState>(value!);
    }

    public async Task<bool> HasActiveConnectionForUserAndAssignmentAsync(Guid userId, Guid assignmentId)
    {
        var userKey = $"{UserIndexPrefix}{userId}:{assignmentId}";
        return await _db.KeyExistsAsync(userKey);
    }

    public async Task SetActiveConnectionAsync(ConnectionState connectionState, TimeSpan? expiration = null)
    {
        var key = $"{ConnectionKeyPrefix}{connectionState.ParticipationId}";
        var userKey = $"{UserIndexPrefix}{connectionState.UserId}:{connectionState.AssignmentId}";
        var value = JsonSerializer.Serialize(connectionState);
        
        await _db.StringSetAsync(key, value, expiration);
        await _db.StringSetAsync(userKey, connectionState.ParticipationId.ToString(), expiration);
    }

    public async Task RemoveActiveConnectionAsync(Guid participationId)
    {
        var connection = await GetActiveConnectionAsync(participationId);
        if (connection != null)
        {
            var key = $"{ConnectionKeyPrefix}{participationId}";
            var userKey = $"{UserIndexPrefix}{connection.UserId}:{connection.AssignmentId}";
            await _db.KeyDeleteAsync(key);
            await _db.KeyDeleteAsync(userKey);
        }
    }

    public async Task<GracePeriodState?> GetGracePeriodStateAsync(Guid participationId)
    {
        var key = $"{GracePeriodKeyPrefix}{participationId}";
        var value = await _db.StringGetAsync(key);
        
        if (value.IsNullOrEmpty)
            return null;

        return JsonSerializer.Deserialize<GracePeriodState>(value!);
    }

    public async Task SetGracePeriodStateAsync(GracePeriodState gracePeriodState, TimeSpan expiration)
    {
        var key = $"{GracePeriodKeyPrefix}{gracePeriodState.ParticipationId}";
        var value = JsonSerializer.Serialize(gracePeriodState);
        
        await _db.StringSetAsync(key, value, expiration);
    }

    public async Task RemoveGracePeriodStateAsync(Guid participationId)
    {
        var key = $"{GracePeriodKeyPrefix}{participationId}";
        await _db.KeyDeleteAsync(key);
    }

    public async Task<List<ConnectionState>> GetActiveConnectionsByUserIdAsync(Guid userId)
    {
        var server = _redis.GetServer(_redis.GetEndPoints().First());
        var pattern = $"{UserIndexPrefix}{userId}:*";
        var keys = server.Keys(pattern: pattern).ToList();
        
        var connections = new List<ConnectionState>();
        
        foreach (var key in keys)
        {
            var participationIdStr = await _db.StringGetAsync(key);
            if (!participationIdStr.IsNullOrEmpty && Guid.TryParse(participationIdStr!, out var participationId))
            {
                var connection = await GetActiveConnectionAsync(participationId);
                if (connection != null)
                {
                    connections.Add(connection);
                }
            }
        }
        
        return connections;
    }

    public async Task<ConflictState?> GetConflictStateAsync(Guid userId)
    {
        var key = $"{ConflictKeyPrefix}{userId}";
        var value = await _db.StringGetAsync(key);
        
        if (value.IsNullOrEmpty)
            return null;

        return JsonSerializer.Deserialize<ConflictState>(value!);
    }

    public async Task SetConflictStateAsync(ConflictState conflictState, TimeSpan expiration)
    {
        var key = $"{ConflictKeyPrefix}{conflictState.UserId}";
        var value = JsonSerializer.Serialize(conflictState);
        
        await _db.StringSetAsync(key, value, expiration);
    }

    public async Task RemoveConflictStateAsync(Guid userId)
    {
        var key = $"{ConflictKeyPrefix}{userId}";
        await _db.KeyDeleteAsync(key);
    }
}
