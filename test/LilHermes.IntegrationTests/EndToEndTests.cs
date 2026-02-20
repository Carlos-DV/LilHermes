using FluentAssertions;
using LilHermes.Abstractions.Enums;
using LilHermes.Abstractions.Entities;
using LilHermes.Infrastructure.Extensions;
using LilHermes.Infrastructure.Interfaces;
using LilHermes.Infrastructure.Telemetry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Exporter;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using System.Diagnostics;

namespace LilHermes.IntegrationTests
{
    public class EndToEndTests : IAsyncLifetime
    {
        private ServiceProvider _serviceProvider;
        private const string TEST_EXCHANGE = "e2e-exchange";
        private const string TEST_QUEUE = "e2e-queue";
        private const string TEST_ROUTING_KEY = "e2e.test";

        public async Task DisposeAsync()
        {
            _tracerProvider?.Dispose();
            if (_serviceProvider != null)
            {
                await _serviceProvider.DisposeAsync();
            }
        }
        private TracerProvider _tracerProvider;

        public async Task InitializeAsync()
        {

            await Task.Delay(1); 

            var services = new ServiceCollection();

            const string serviceName = "LilHermes.E2E";

            services.AddLogging(builder =>
            {
                builder.AddConsole();
                builder.SetMinimumLevel(LogLevel.Debug);
            });

            services.AddOpenTelemetry()
                .WithTracing(builder =>
                {
                    builder
                        .SetResourceBuilder(
                            ResourceBuilder.CreateDefault()
                                .AddService(serviceName))
                        .AddLilHermesInstrumentation(serviceName)
                        .AddConsoleExporter()
                        .SetSampler(new AlwaysOnSampler())
                        //.AddOtlpExporter(opt =>
                        //{
                        //    opt.Endpoint = new Uri("http://127.0.0.1:4317");
                        //    opt.Protocol = OpenTelemetry.Exporter.OtlpExportProtocol.Grpc;
                        //});
                        .AddOtlpExporter(opt =>
                        {
                            // Usamos el puerto HTTP y especificamos la ruta de traces
                            opt.Endpoint = new Uri("http://127.0.0.1:4318/v1/traces");
                            opt.Protocol = OtlpExportProtocol.HttpProtobuf;
                        });
                });


            services.AddLilHermes(
                options =>
                {
                    options.SourceName = serviceName;
                    options.ConnectionOptions.HostName = "127.0.0.1";
                    options.PublishOptions.Exchange = TEST_EXCHANGE;
                    options.PublishOptions.ExchangeType = RabbitMQExchangeType.Topic;
                    options.PublishOptions.Persistent = true;
                    options.PublishOptions.PublisherConfirmationsEnabled = true;
                    options.PublishOptions.PublisherConfirmations = true;
                    options.ConsumerOptions.ExchangeName = TEST_EXCHANGE;
                    options.ConsumerOptions.ExchangeType = RabbitMQExchangeType.Topic;
                    options.ConsumerOptions.QueueName = TEST_QUEUE;
                    options.ConsumerOptions.RoutingKeys = [TEST_ROUTING_KEY];
                    options.ConsumerOptions.PrefetchCount = 1;
                }
            );

            _serviceProvider = services.BuildServiceProvider();
            _tracerProvider = _serviceProvider.GetRequiredService<TracerProvider>();
        }

        public class TestOrder
        {
            public int OrderId { get; set; }
            public string CustomerName { get; set; } = string.Empty;
            public decimal Amount { get; set; }
        }


        [Fact]
        public async Task PublishAndConsume_ShouldWorkEndToEnd()
        {
            var telemetry = _serviceProvider.GetRequiredService<LilHermesTelemetry>();
            using var rootActivity = telemetry.ActivitySource.StartActivity("E2E_Test_Root");

            var publisher = _serviceProvider.GetRequiredService<IMessagePublisher>();
            var consumer = _serviceProvider.GetRequiredService<IMessageConsumer>();

            var receivedMessages = new List<TestOrder>();
            var messageReceivedEvent = new TaskCompletionSource<bool>();

            await consumer.StartConsumingAsync<TestOrder>(async (order, deliveryTag) =>
            {
                receivedMessages.Add(order);
                //await Task.Delay(2000);
                //throw new InvalidOperationException("Saldo insuficiente para procesar la orden.");
                await consumer.AckAsync(deliveryTag);

                messageReceivedEvent.TrySetResult(true);
            });

            var testOrder = new TestOrder
            {
                OrderId = 1,
                CustomerName = "Carlos DV",
                Amount = 75.15m
            };

            var context = new MessageContext<TestOrder>
            {
                CorrelationId = Guid.NewGuid().ToString(),
                Data = testOrder,
                MessageId = testOrder.OrderId.ToString(),
                SourceService = "lilHermes.IntegrationsTests",
                Timestamp = DateTime.UtcNow,
            };

            await publisher.PublishAsync(context, TEST_ROUTING_KEY);
            //await publisher.WaitForConfirmsAsync();

            var received = await Task.WhenAny(messageReceivedEvent.Task, Task.Delay(5000)) == messageReceivedEvent.Task;
            received.Should().BeTrue("el mensaje debería ser recibido en menos de 5 segundos");
            receivedMessages.Should().HaveCount(1);
            receivedMessages[0].OrderId.Should().Be(1);
            receivedMessages[0].CustomerName.Should().Be("Carlos DV");
            receivedMessages[0].Amount.Should().Be(75.15m);

            rootActivity?.AddEvent(new ActivityEvent("Flush disparado"));
            _tracerProvider.ForceFlush();

            await Task.Delay(TimeSpan.FromSeconds(5));
            await consumer.StopConsumingAsync();
        }
    }
}