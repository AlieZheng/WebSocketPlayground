namespace WebSocketPlayground.Models;

public class ConnectionState
{
    public required string ConnectionId { get; set; }
    public required string UserId { get; set; }
    public required string AssignmentId { get; set; }
    public required string AttemptId { get; set; }
    public DateTime ConnectedAt { get; set; } = DateTime.UtcNow;
    public bool IsPending { get; set; }
}

