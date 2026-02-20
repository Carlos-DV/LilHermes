namespace LilHermes.Infrastructure.Consumers
{
    internal class RabbitMQQueueStructure
    {
        public string MainQueue { get; set; }
        public string MainExchange { get; set; }
        public string RetryQueue { get; set; }
        public string RetryExchange { get; set; }
        public string ParkedQueue { get; set; }
        public string ParkedExchange { get; set; }
    }
}