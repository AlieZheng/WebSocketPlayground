namespace WebSocketPlayground.Models;

public class EndSessionCommand
{
    public required Guid UserId { get; set; }
    public required Guid AssignmentId { get; set; }
    public required string ConnectionId { get; set; }
}

