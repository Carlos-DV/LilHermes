//using FluentAssertions;
//using LilHermes.Domain.Enums;
//using LilHermes.Infrastructure.Extensions;
//using LilHermes.Infrastructure.Interfaces;
//using Microsoft.Extensions.DependencyInjection;
//using static LilHermes.IntegrationTests.EndToEndTests;

//namespace LilHermes.IntegrationTests
//{

//    public class BatchTest : IAsyncLifetime
//    {
//        private ServiceProvider _serviceProvider;
//        private const string TEST_EXCHANGE = "batch-exchange";
//        private const string TEST_QUEUE = "batch-queue";
//        private const string TEST_ROUTING_KEY = "batch.test";

//        public async Task InitializeAsync()
//        {

//            await Task.Delay(1);

//            var services = new ServiceCollection();

//            services.AddLilHermes(
//                options =>
//                {
//                    options.ConnectionOptions.HostName = "127.0.0.1";
//                    options.PublishOptions.Exchange = TEST_EXCHANGE;
//                    options.PublishOptions.ExchangeType = RabbitMQExchangeType.Topic;
//                    options.PublishOptions.RoutingKey = TEST_ROUTING_KEY;
//                    options.PublishOptions.Persistent = true;
//                    options.PublishOptions.PublisherConfirmationsEnabled = true;
//                    options.PublishOptions.PublisherConfirmations = true;
//                    options.ConsumerOptions.ExchangeName = TEST_EXCHANGE;
//                    options.ConsumerOptions.ExchangeType = RabbitMQExchangeType.Topic;
//                    options.ConsumerOptions.QueueName = TEST_QUEUE;
//                    options.ConsumerOptions.RoutingKeys = [TEST_ROUTING_KEY];
//                    options.ConsumerOptions.PrefetchCount = 1;
//                }
//            );

//            _serviceProvider = services.BuildServiceProvider();
//        }

//        public async Task DisposeAsync()
//        {
//            if (_serviceProvider != null)
//            {
//                await _serviceProvider.DisposeAsync();
//            }
//        }

//        [Fact]
//        public async Task PublishRangeAndConsume_ShouldWorkEndToEnd()
//        {
//            var publisher = _serviceProvider.GetRequiredService<IMessagePublisher>();
//            var consumer = _serviceProvider.GetRequiredService<IMessageConsumer>();

//            var receivedMessages = new List<TestOrder>();
//            var messageReceivedEvent = new TaskCompletionSource<bool>();


//            await consumer.StartConsumingAsync<TestOrder>(async (order, deliveryTag) =>
//            {
//                receivedMessages.Add(order);
//                await consumer.AckAsync(deliveryTag);
//                messageReceivedEvent.TrySetResult(true);
//            });


//            var testOrderRange = new List<TestOrder>()
//            {
//                new()
//                {
//                    OrderId = 2,
//                    CustomerName = "Ripple Ven",
//                    Amount = 15.0m
//                },
//                new()
//                {
//                    OrderId = 3,
//                    CustomerName = "Airi Ake"
//                }
//            };
//            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

//            Func<Task> act = async () =>
//            {
//                await publisher.PublishBatchAsync(testOrderRange, TEST_ROUTING_KEY, cts.Token);
//                await publisher.WaitForConfirmsAsync();
//            };

//            await act.Should().NotThrowAsync();

//            var received = await Task.WhenAny(messageReceivedEvent.Task, Task.Delay(5000)) == messageReceivedEvent.Task;

//            received.Should().BeTrue("el mensaje debería ser recibido en menos de 5 segundos");
//            receivedMessages.Should().HaveCountGreaterThan(1);

//            await consumer.StopConsumingAsync();
//        }
//    }
//}
