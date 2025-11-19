namespace WebSocketPlayground.Models;

public class ConnectionState
{
    public required string ConnectionId { get; set; }
    public required Guid UserId { get; set; }
    public required Guid AssignmentId { get; set; }
    public required Guid ParticipationId { get; set; }
    public DateTime ConnectedAt { get; set; } = DateTime.UtcNow;
    public bool IsPending { get; set; }
}

