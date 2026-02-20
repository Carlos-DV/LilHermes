using System.Collections.Generic;
using LilHermes.Abstractions.Enums;

namespace LilHermes.Abstractions.Entities
{
    /// <summary>
    /// Class to configure a consumer
    /// </summary>
    public class ConsumerOptions
    {
        public string QueueName { get; set; } = "LilHermes-Queue-v0";
        public string ExchangeName { get; set; }
        public RabbitMQExchangeType ExchangeType { get; set; }
        public string[] RoutingKeys { get; set; }
        public AcknowledgeMode AcknowledgeMode { get; set; } = AcknowledgeMode.Auto;
        public ushort PrefetchCount { get; set; } = 100;
        public bool Durable { get; set; } = false;
        public bool Exclusive { get; set; } = false;
        public bool AutoDelete { get; set; } = false;
        public int MaxRetryCount { get; set; } = 3;
        public int RetryDelayMs { get; set; } = 1000;
        public bool EnableDLQ { get; set; } = false;
        public int MessageTTL { get; set; } = 30000;
        public Dictionary<string, object> Arguments { get; set; } = new Dictionary<string, object>();
    }
}