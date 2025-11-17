using WebSocketPlayground.Models;

namespace WebSocketPlayground.Services;

public interface IActivityEventPublisher
{
    Task PublishActivityStartedAsync(StudentActivityStartedEvent eventData, CancellationToken cancellationToken = default);
    Task PublishActivityEndedAsync(StudentActivityEndedEvent eventData, CancellationToken cancellationToken = default);
}

