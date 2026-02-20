namespace LilHermes.Abstractions.Entities
{
    /// <summary>
    /// Class master
    /// </summary>
    public class MessageBusOptions
    {
        public string SourceName { get; set; } = "LilHermes";
        public ConnectionOptions ConnectionOptions { get; set; } = new ConnectionOptions();
        public PublishOptions PublishOptions { get; set; } = new PublishOptions();
        public ConsumerOptions ConsumerOptions { get; set; } = new ConsumerOptions();
    }
}