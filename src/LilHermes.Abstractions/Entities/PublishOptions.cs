using System.Collections.Generic;
using LilHermes.Abstractions.Enums;

namespace LilHermes.Abstractions.Entities
{
    public class PublishOptions
    {
        public string Exchange { get; set; }
        public RabbitMQExchangeType ExchangeType { get; set; }
        public bool Mandatory { get; set; } = false;
        public MessagePriority Priority { get; set; } = MessagePriority.Normal;
        public bool Persistent { get; set; } = true;
        public bool PublisherConfirmationsEnabled { get; set; } = true;
        public bool PublisherConfirmations {  get; set; } = true;
        public string ContentType { get; set; } = "application/json";
        public string ContentEncoding { get; set; } = "UTF-8";
        public Dictionary<string, object> Headers { get; set; } = new Dictionary<string, object>();
    }
}