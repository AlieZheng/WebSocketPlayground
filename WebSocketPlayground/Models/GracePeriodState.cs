namespace WebSocketPlayground.Models;

public class GracePeriodState
{
    public required string ConnectionId { get; set; }
    public required string UserId { get; set; }
    public required string AssignmentId { get; set; }
    public required string AttemptId { get; set; }
    public DateTime DisconnectedAt { get; set; } = DateTime.UtcNow;
}

