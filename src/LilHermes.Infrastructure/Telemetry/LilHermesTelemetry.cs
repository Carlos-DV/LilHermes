using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace LilHermes.Infrastructure.Telemetry
{
    /// <summary>
    /// Const for telemetry 
    /// </summary>
    public class LilHermesTelemetry
    {
        public ActivitySource ActivitySource { get; }
        public Meter Meter { get; }
        public string BatchActivity { get; }
        public string PublishActivity { get; }
        public string ConsumeActivity { get; }
        public string ProcessActivity { get; }
        public LilHermesTelemetry(string sourceName = "LilHermes")
        {
            ActivitySource = new ActivitySource(sourceName, "1.0.0");
            Meter = new Meter(sourceName, "1.0.0");
            BatchActivity = $"{sourceName}.Batch";
            PublishActivity = $"{sourceName}.Publish";
            ConsumeActivity = $"{sourceName}.Consume";
            ProcessActivity = $"{sourceName}.Process";
        }
        // Tags
        public const string MessagingSystem = "messaging.system";
        public const string MessagingDestination = "messaging.destination";
        public const string MessagingRoutingKey = "messaging.routing_key";
        public const string MessagingMessageId = "messaging.message_id";
        public const string MessagingCorrelationId = "messaging.correlation_id";
        public const string MessagingPayloadSize = "messaging.payload_size_bytes";
        public const string MessagingBatchCount = "messaging.batch.count";
        // Tags for RabbitMQ
        public const string RabbitMQExchange = "rabbitmq.exchange";
        public const string RabbitMQQueue = "rabbitmq.queue";
        public const string RabbitMQDeliveryTag = "rabbitmq.delivery_tag";
        // RabbitMQ
        public const string RabbitMQ = "rabbitmq";
        // Events
        public const string PublisedhOk = "Message published";
        public const string ConsumedOk = "Message consumed";
        public const string ProcessOk = "Message processed";
    }
}
