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
            Timestamp = DateTime.UtcNow;
            Metadata = new Dictionary<string, string>();
        }
        public MessageContext(T data) : this()
        {
            Data = data;
        }
    }

    /// <summary>
    /// Crea mensajes <see cref="MessageContext{T}"/> listos para publicar
    /// </summary>
    public static class MessageContext
    {
        /// <summary>
        /// Crea un mensaje con un CorrelationId nuevo y Timestamp en UTC
        /// </summary>
        /// <typeparam name="T">Tipo del contenido</typeparam>
        /// <param name="data">Contenido del mensaje</param>
        /// <param name="sourceService">Servicio que envía el mensaje</param>
        /// <param name="messageId">Id del mensaje; si es null o vacío se genera un GUID</param>
        /// <exception cref="ArgumentNullException"><paramref name="data"/> es null</exception>
        /// <exception cref="ArgumentException"><paramref name="sourceService"/> es null o vacío</exception>
        public static MessageContext<T> Create<T>(T data, string sourceService, string messageId = null) where T : class
        {
            if (data == null)
                throw new ArgumentNullException(nameof(data));
            if (string.IsNullOrWhiteSpace(sourceService))
                throw new ArgumentException("sourceService is required", nameof(sourceService));

            var context = new MessageContext<T>(data) { SourceService = sourceService };
            if (!string.IsNullOrEmpty(messageId))
                context.MessageId = messageId;
            return context;
        }
    }
}