using FluentAssertions;
using LilHermes.Abstractions.Entities;
using LilHermes.Abstractions.Enums;
using LilHermes.Infrastructure.Extensions;
using LilHermes.Infrastructure.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace LilHermes.IntegrationTests
{
    /// <summary>
    /// Con un exchange Direct, el mensaje enviado a reintento debe llegar a la cola .retry
    /// y volver a la cola principal (antes la binding "#" no coincidía y RabbitMQ lo descartaba).
    /// </summary>
    public class DlqDirectRetryTests : IAsyncLifetime
    {
        private ServiceProvider _serviceProvider = null!;
        private const string TEST_EXCHANGE = "dlq-direct-exchange";
        private const string TEST_QUEUE = "dlq-direct-queue";
        private const string TEST_ROUTING_KEY = "dlq.direct.test";

        public async Task InitializeAsync()
        {
            var services = new ServiceCollection();

            services.AddLogging(builder =>
            {
                builder.AddConsole();
                builder.SetMinimumLevel(LogLevel.Debug);
            });

            services.AddLilHermes(
                options =>
                {
                    options.SourceName = "LilHermes.DlqDirect";
                    options.ConnectionOptions.HostName = "127.0.0.1";
                    options.PublishOptions.Exchange = TEST_EXCHANGE;
                    options.PublishOptions.ExchangeType = RabbitMQExchangeType.Direct;
                    options.PublishOptions.Persistent = true;
                    options.ConsumerOptions.ExchangeName = TEST_EXCHANGE;
                    options.ConsumerOptions.ExchangeType = RabbitMQExchangeType.Direct;
                    options.ConsumerOptions.QueueName = TEST_QUEUE;
                    options.ConsumerOptions.RoutingKeys = [TEST_ROUTING_KEY];
                    options.ConsumerOptions.PrefetchCount = 1;
                    options.ConsumerOptions.EnableDLQ = true;
                    options.ConsumerOptions.MaxRetryCount = 3;
                    options.ConsumerOptions.MessageTTL = 500;
                }
            );

            _serviceProvider = services.BuildServiceProvider();

            // Parte de un broker limpio para que una ejecución anterior no afecte la binding
            await DeleteTopologyAsync();
        }

        public async Task DisposeAsync()
        {
            await _serviceProvider.GetRequiredService<IMessageConsumer>().StopConsumingAsync();
            await DeleteTopologyAsync();
            await _serviceProvider.DisposeAsync();
        }

        public class TestOrder
        {
            public int OrderId { get; set; }
        }

        [Fact]
        public async Task FailedMessage_WithDirectExchange_ShouldComeBackFromRetryQueue()
        {
            var publisher = _serviceProvider.GetRequiredService<IMessagePublisher>();
            var consumer = _serviceProvider.GetRequiredService<IMessageConsumer>();

            var attempts = 0;
            var retriedEvent = new TaskCompletionSource<bool>();

            await consumer.StartConsumingAsync<TestOrder>(async (order, deliveryTag) =>
            {
                if (Interlocked.Increment(ref attempts) == 1)
                    throw new InvalidOperationException("Falla forzada para enviar el mensaje a reintento");

                await consumer.AckAsync(deliveryTag);
                retriedEvent.TrySetResult(true);
            });

            var context = new MessageContext<TestOrder>
            {
                CorrelationId = Guid.NewGuid().ToString(),
                Data = new TestOrder { OrderId = 1 },
                MessageId = Guid.NewGuid().ToString(),
                SourceService = "lilHermes.IntegrationsTests",
                Timestamp = DateTime.UtcNow,
            };

            await publisher.PublishAsync(context, TEST_ROUTING_KEY);

            var retried = await Task.WhenAny(retriedEvent.Task, Task.Delay(10000)) == retriedEvent.Task;
            retried.Should().BeTrue("el mensaje debería volver de la cola .retry tras el TTL");
            attempts.Should().Be(2);
        }

        private async Task DeleteTopologyAsync()
        {
            var connectionManager = _serviceProvider.GetRequiredService<IMessageConnectionManager>();
            var connection = await connectionManager.GetConnectionAsync();

            await using var channel = await connection.CreateChannelAsync();
            await channel.QueueDeleteAsync(TEST_QUEUE, ifUnused: false, ifEmpty: false);
            await channel.QueueDeleteAsync($"{TEST_QUEUE}.retry", ifUnused: false, ifEmpty: false);
            await channel.QueueDeleteAsync($"{TEST_QUEUE}.error", ifUnused: false, ifEmpty: false);
            await channel.ExchangeDeleteAsync(TEST_EXCHANGE, ifUnused: false);
            await channel.ExchangeDeleteAsync($"{TEST_QUEUE}.retry.ex", ifUnused: false);
            await channel.ExchangeDeleteAsync($"{TEST_QUEUE}.error.ex", ifUnused: false);
        }
    }
}
