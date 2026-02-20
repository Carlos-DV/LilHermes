using LilHermes.Abstractions.Enums;

namespace LilHermes.Infrastructure.Utils
{
    public static class RabbitMQExchangeTypeExtensions
    {
        public static string ToRabbitString(this RabbitMQExchangeType type)
        {
            switch (type) 
            {
                case RabbitMQExchangeType.Topic: return ExchangeTypeNames.Topic;
                case RabbitMQExchangeType.Fanout: return ExchangeTypeNames.Fanout;
                case RabbitMQExchangeType.Headers: return ExchangeTypeNames.Headers;
                default: return ExchangeTypeNames.Direct;
            }
        }
    }
}