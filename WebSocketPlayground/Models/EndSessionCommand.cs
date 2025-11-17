namespace WebSocketPlayground.Models;

public class EndSessionCommand
{
    public required string UserId { get; set; }
    public required string AssignmentId { get; set; }
    public required string ConnectionId { get; set; }
}

