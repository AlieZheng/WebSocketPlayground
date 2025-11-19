using KafkaFlow;
using KafkaFlow.Producers;
using WebSocketPlayground.Configuration;
using WebSocketPlayground.Models;

namespace WebSocketPlayground.Services;

public class ActivityEventPublisher : IActivityEventPublisher
{
    private readonly IProducerAccessor _producerAccessor;
    private readonly KafkaConfiguration _kafkaConfig;
    private readonly ILogger<ActivityEventPublisher> _logger;

    public ActivityEventPublisher(
        IProducerAccessor producerAccessor,
        KafkaConfiguration kafkaConfig,
        ILogger<ActivityEventPublisher> logger)
    {
        _producerAccessor = producerAccessor;
        _kafkaConfig = kafkaConfig;
        _logger = logger;
    }

    public async Task PublishActivityStartedAsync(StudentActivityStartedEvent eventData, CancellationToken cancellationToken = default)
    {
        try
        {
            var producer = _producerAccessor.GetProducer("activity-events-producer");
            
            await producer.ProduceAsync(
                _kafkaConfig.EventsTopic,
                eventData.UserId.ToString(), // Key
                eventData); // Value (will be serialized to JSON by KafkaFlow)

            _logger.LogInformation(
                "Published StudentActivityStarted event: UserId={UserId}, AssignmentId={AssignmentId}, ParticipationId={ParticipationId}",
                eventData.UserId, eventData.AssignmentId, eventData.ParticipationId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, 
                "Failed to publish StudentActivityStarted event: UserId={UserId}, AssignmentId={AssignmentId}",
                eventData.UserId, eventData.AssignmentId);
            throw;
        }
    }

    public async Task PublishActivityEndedAsync(StudentActivityEndedEvent eventData, CancellationToken cancellationToken = default)
    {
        try
        {
            var producer = _producerAccessor.GetProducer("activity-events-producer");
            
            await producer.ProduceAsync(
                _kafkaConfig.EventsTopic,
                eventData.UserId.ToString(), // Key
                eventData); // Value (will be serialized to JSON by KafkaFlow)

            _logger.LogInformation(
                "Published StudentActivityEnded event: UserId={UserId}, AssignmentId={AssignmentId}, ParticipationId={ParticipationId}, Reason={Reason}",
                eventData.UserId, eventData.AssignmentId, eventData.ParticipationId, eventData.Reason);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, 
                "Failed to publish StudentActivityEnded event: UserId={UserId}, AssignmentId={AssignmentId}",
                eventData.UserId, eventData.AssignmentId);
            throw;
        }
    }
}

