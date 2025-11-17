namespace WebSocketPlayground.Models;

public class StudentActivityStartedEvent
{
    public required string UserId { get; set; }
    public required string AssignmentId { get; set; }
    public required string AttemptId { get; set; }
    public required string ConnectionId { get; set; }
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
}



