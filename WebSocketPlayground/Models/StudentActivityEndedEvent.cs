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
    public required string UserId { get; set; }
    public required string AssignmentId { get; set; }
    public required string AttemptId { get; set; }
    public required string ConnectionId { get; set; }
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    public DisconnectReason Reason { get; set; }
}

