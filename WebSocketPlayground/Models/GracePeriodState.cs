namespace WebSocketPlayground.Models;

public class GracePeriodState
{
    public required string ConnectionId { get; set; }
    public required Guid UserId { get; set; }
    public required Guid AssignmentId { get; set; }
    public required Guid ParticipationId { get; set; }
    public DateTime DisconnectedAt { get; set; } = DateTime.UtcNow;
}

