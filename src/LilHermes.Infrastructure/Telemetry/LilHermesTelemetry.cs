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

        // Metric instruments
        public Counter<long> MessagesPublished { get; }
        public Counter<long> MessagesConsumed { get; }
        public Counter<long> MessagesFailed { get; }
        public Histogram<double> PublishDurationMs { get; }

        /// <summary>
        /// Nombre por defecto del ActivitySource y del Meter de LilHermes
        /// </summary>
        public const string DefaultSourceName = "LilHermes";

        private static readonly string PackageVersion =
            typeof(LilHermesTelemetry).Assembly.GetName().Version.ToString(3);

        public LilHermesTelemetry(string sourceName = DefaultSourceName)
        {
            ActivitySource = new ActivitySource(sourceName, PackageVersion);
            Meter = new Meter(sourceName, PackageVersion);
            BatchActivity = $"{sourceName}.Batch";
            PublishActivity = $"{sourceName}.Publish";
            ConsumeActivity = $"{sourceName}.Consume";
            ProcessActivity = $"{sourceName}.Process";

            // Metrics
            MessagesPublished = Meter.CreateCounter<long>("messages_published", "messages", "Total messages published");
            MessagesConsumed = Meter.CreateCounter<long>("messages_consumed", "messages", "Total messages consumed");
            MessagesFailed = Meter.CreateCounter<long>("messages_failed", "messages", "Total messages that failed processing");
            PublishDurationMs = Meter.CreateHistogram<double>("publish_duration_ms", "ms", "Duration of publish operations");
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
        public const string PublishedOk = "Message published";
        public const string ConsumedOk = "Message consumed";
        public const string ProcessOk = "Message processed";
    }
}
