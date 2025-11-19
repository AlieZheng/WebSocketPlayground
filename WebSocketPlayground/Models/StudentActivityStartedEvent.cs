namespace WebSocketPlayground.Models;

public class StudentActivityStartedEvent
{
    public required Guid UserId { get; set; }
    public required Guid AssignmentId { get; set; }
    public required Guid ParticipationId { get; set; }
    public required string ConnectionId { get; set; }
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
}



