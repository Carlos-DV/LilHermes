using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using RabbitMQ.Client;

namespace LilHermes.Infrastructure.Telemetry
{
    internal static class LilHermesTraceContextHelper
    {
        /// <summary>
        /// Extrae el traceparent del header de RabbitMQ (para mensajes recibidos)
        /// </summary>
        public static string ExtractTraceParent(IReadOnlyBasicProperties properties)
        {
            return ExtractHeader(properties, "traceparent");
        }

        /// <summary>
        /// Extrae el tracestate del header de RabbitMQ (para mensajes recibidos)
        /// </summary>
        public static string ExtractTraceState(IReadOnlyBasicProperties properties)
        {
            return ExtractHeader(properties, "tracestate");
        }

        /// <summary>
        /// Crea un ActivityContext desde los headers de RabbitMQ (para mensajes recibidos)
        /// </summary>
        public static ActivityContext? ExtractActivityContext(IReadOnlyBasicProperties properties)
        {
            var traceParent = ExtractTraceParent(properties);

            if (string.IsNullOrEmpty(traceParent))
                return null;

            var traceState = ExtractTraceState(properties);

            if (ActivityContext.TryParse(traceParent, traceState, out ActivityContext activityContext))
            {
                return activityContext;
            }

            return null;
        }

        /// <summary>
        /// Inyecta el traceparent en los headers de RabbitMQ (para mensajes a enviar)
        /// </summary>
        public static void InjectTraceContext(BasicProperties properties, Activity activity)
        {
            if (activity == null || properties == null)
                return;

            if (properties.Headers == null)
            {
                properties.Headers = new Dictionary<string, object>();
            }

            if (!string.IsNullOrEmpty(activity.Id))
            {
                properties.Headers["traceparent"] = Encoding.UTF8.GetBytes(activity.Id);
            }

            if (!string.IsNullOrEmpty(activity.TraceStateString))
            {
                properties.Headers["tracestate"] = Encoding.UTF8.GetBytes(activity.TraceStateString);
            }
        }

        /// <summary>
        /// Extrae un header de las propiedades de RabbitMQ (para mensajes recibidos)
        /// </summary>
        private static string ExtractHeader(IReadOnlyBasicProperties properties, string headerName)
        {
            if (properties?.Headers == null)
                return null;

            if (!properties.Headers.TryGetValue(headerName, out object headerValue))
                return null;

            try
            {
                if (headerValue is byte[] bytes)
                {
                    return Encoding.UTF8.GetString(bytes);
                }
                else if (headerValue is string str)
                {
                    return str;
                }
                else if (headerValue != null)
                {
                    return headerValue.ToString();
                }

                return null;
            }
            catch
            {
                return null;
            }
        }
    }
}