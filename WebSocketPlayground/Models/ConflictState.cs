namespace WebSocketPlayground.Models;

public class ConflictState
{
    public required Guid UserId { get; set; }
    public required string OldConnectionId { get; set; }
    public required string NewConnectionId { get; set; }
    public required Guid OldAssignmentId { get; set; }
    public required Guid NewAssignmentId { get; set; }
    public required Guid OldParticipationId { get; set; }
    public required Guid NewParticipationId { get; set; }
    public DateTime ConflictDetectedAt { get; set; } = DateTime.UtcNow;
}

