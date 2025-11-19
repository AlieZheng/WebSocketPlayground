namespace WebSocketPlayground.Models;

public enum DisconnectReason
{
    Disconnected,
    SwitchedAssignment,
    Timeout,
    GracePeriodExpired
}

public class StudentActivityEndedEvent
{
    public required Guid UserId { get; set; }
    public required Guid AssignmentId { get; set; }
    public required Guid ParticipationId { get; set; }
    public required string ConnectionId { get; set; }
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    public DisconnectReason Reason { get; set; }
}

