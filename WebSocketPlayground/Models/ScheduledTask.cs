namespace WebSocketPlayground.Models;

public class ScheduledTask
{
    public required string TaskId { get; set; }
    public required string TaskType { get; set; } // "GracePeriod" or "ConflictTimeout"
    public required DateTime ExecuteAt { get; set; }
    public required string Payload { get; set; } // JSON with participationId/userId and other context
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

public enum ScheduledTaskType
{
    GracePeriod,
    ConflictTimeout
}

public class GracePeriodTaskPayload
{
    public required Guid ParticipationId { get; set; }
    public required Guid UserId { get; set; }
    public required Guid AssignmentId { get; set; }
    public required string ConnectionId { get; set; }
}

public class ConflictTimeoutTaskPayload
{
    public required Guid UserId { get; set; }
    public required string OldConnectionId { get; set; }
    public required string NewConnectionId { get; set; }
}

