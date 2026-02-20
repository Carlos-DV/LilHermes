namespace LilHermes.Abstractions.Enums
{
    public static class ExchangeTypeNames
    {
        /// <summary>
        /// Exchange type used for AMQP direct exchanges.
        /// </summary>
        public const string Direct = "direct";

        /// <summary>
        /// Exchange type used for AMQP fanout exchanges.
        /// </summary>
        public const string Fanout = "fanout";

        /// <summary>
        /// Exchange type used for AMQP headers exchanges.
        /// </summary>
        public const string Headers = "headers";

        /// <summary>
        /// Exchange type used for AMQP topic exchanges.
        /// </summary>
        public const string Topic = "topic";
        
    }
}