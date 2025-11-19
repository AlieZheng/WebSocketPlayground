namespace WebSocketPlayground.Models;

public class ConflictState
{
    public required string UserId { get; set; }
    public required string OldConnectionId { get; set; }
    public required string NewConnectionId { get; set; }
    public required string OldAssignmentId { get; set; }
    public required string NewAssignmentId { get; set; }
    public required string OldParticipationId { get; set; }
    public required string NewParticipationId { get; set; }
    public DateTime ConflictDetectedAt { get; set; } = DateTime.UtcNow;
}

