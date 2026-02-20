using System;
using System.Collections.Generic;

namespace LilHermes.Abstractions.Entities
{
    /// <summary>
    /// Message wrapper to send rabbitmq
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class MessageContext<T> where T : class
    {
        /// <summary>
        /// Correlation ID for Distributed Tracing
        /// </summary>
        public string CorrelationId { get; set; }
        /// <summary>
        /// ID for message original
        /// </summary>
        public string MessageId { get; set; }
        /// <summary>
        /// Timestamp of when the message was created
        /// </summary>
        public DateTime Timestamp { get; set; }
        /// <summary>
        /// Message
        /// </summary>
        public T Data { get; set; }
        /// <summary>
        /// Metadata
        /// </summary>
        public Dictionary<string, string> Metadata { get; set; }
        /// <summary>
        /// Name of the service that sent the message
        /// </summary>
        public string SourceService { get; set; }
        public MessageContext() 
        {
            MessageId = Guid.NewGuid().ToString();
            CorrelationId = Guid.NewGuid().ToString();
            Timestamp = DateTime.Now;
            Metadata = new Dictionary<string, string>();
        }
        public MessageContext(T data) : this()
        {
            Data = data;
        }
    }
}