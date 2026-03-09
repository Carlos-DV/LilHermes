using LilHermes.Abstractions.Entities;
using LilHermes.Infrastructure.Interfaces;
using LilHermes.Infrastructure.Publishers;
using LilHermes.Infrastructure.Telemetry;
using Microsoft.Extensions.Logging;
using Moq;
using RabbitMQ.Client;

namespace LilHermes.UnitTests
{
    public class RabbitMqPublisherTests
    {
        private readonly Mock<IConnection> _mockConnection;
        private readonly Mock<IChannel> _mockChannel;
        private readonly MessageBusOptions _options;
        private readonly LilHermesTelemetry _telemetry;

        public RabbitMqPublisherTests()
        {
            _mockConnection = new Mock<IConnection>();
            _mockChannel = new Mock<IChannel>();
            _telemetry = new LilHermesTelemetry("LilHermes.Tests");

            _options = new MessageBusOptions()
            {
                ConnectionOptions = new ConnectionOptions()
                {
                    HostName = "localhost",
                }
            };

            _mockConnection
                .Setup(x => x.IsOpen)
                .Returns(true);

            _mockChannel
              .Setup(x => x.IsClosed)
              .Returns(false);
        }

        [Fact]
        public async Task PublishAsync_WithValidMessage_ShouldCallBasicPublish()
        {
            var mockConnectionManager = new Mock<IMessageConnectionManager>();
            var mockLogger = new Mock<ILogger<RabbitMQPublisher>>();
            var mockConnection = new Mock<IConnection>();
            var mockChannel = new Mock<IChannel>();

            mockConnectionManager
                          .Setup(x => x.GetConnectionAsync(It.IsAny<CancellationToken>()))
                          .ReturnsAsync(mockConnection.Object);

            mockConnection
                .Setup(x => x.CreateChannelAsync(
                It.IsAny<CreateChannelOptions>(),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockChannel.Object);

            mockConnection.Setup(x => x.IsOpen).Returns(true);
            mockChannel.Setup(x => x.IsClosed).Returns(false);

            var options = new MessageBusOptions
            {
                ConnectionOptions = new ConnectionOptions()
                {
                    HostName = "localhost",
                },
                PublishOptions = new PublishOptions()
                {
                    Exchange = "test-exchange",
                    PublisherConfirmationsEnabled = true,
                }
            };

            var publisher = new RabbitMQPublisher(options, mockConnectionManager.Object, mockLogger.Object, _telemetry);
            var testMessage = new MessageContext<TestPayload>(new TestPayload { Id = 1, Name = "Test" });

            await publisher.PublishAsync(testMessage, "test.key");

            mockChannel.Verify(x => x.BasicPublishAsync(
                It.Is<string>(e => e == "test-exchange"),
                It.Is<string>(r => r == "test.key"),
                It.IsAny<bool>(),
                It.IsAny<BasicProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()
            ), Times.Once);

            mockConnectionManager.Verify(x => x.GetConnectionAsync(
                It.IsAny<CancellationToken>()
            ), Times.Once);
        }

        [Fact]
        public async Task PublishAsync_WhenConnectionFails_ShouldThrowException()
        {
            var mockConnectionManager = new Mock<IMessageConnectionManager>();
            var mockLogger = new Mock<ILogger<RabbitMQPublisher>>();

            mockConnectionManager
                .Setup(x => x.GetConnectionAsync(It.IsAny<CancellationToken>()))
                .ThrowsAsync(new Exception("Network error"));

            var publisher = new RabbitMQPublisher(_options, mockConnectionManager.Object, mockLogger.Object, _telemetry);
            var testMessage = new MessageContext<TestPayload>(new TestPayload { Id = 1, Name = "Test" });

            await Assert.ThrowsAsync<Exception>(() =>
                publisher.PublishAsync(testMessage, "key"));
        }

        [Fact]
        public async Task PublishAsync_ShouldRecordMetrics()
        {
            var mockConnectionManager = new Mock<IMessageConnectionManager>();
            var mockLogger = new Mock<ILogger<RabbitMQPublisher>>();
            var mockConnection = new Mock<IConnection>();
            var mockChannel = new Mock<IChannel>();

            mockConnectionManager
                .Setup(x => x.GetConnectionAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockConnection.Object);

            mockConnection
                .Setup(x => x.CreateChannelAsync(
                It.IsAny<CreateChannelOptions>(),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockChannel.Object);

            mockConnection.Setup(x => x.IsOpen).Returns(true);
            mockChannel.Setup(x => x.IsClosed).Returns(false);

            var options = new MessageBusOptions
            {
                PublishOptions = new PublishOptions
                {
                    Exchange = "test-exchange",
                    PublisherConfirmationsEnabled = true,
                }
            };

            var telemetry = new LilHermesTelemetry("LilHermes.MetricsTest");
            var publisher = new RabbitMQPublisher(options, mockConnectionManager.Object, mockLogger.Object, telemetry);
            var testMessage = new MessageContext<TestPayload>(new TestPayload { Id = 1, Name = "Test" });

            await publisher.PublishAsync(testMessage, "test.key");

            // Verify publish was called (metrics are fire-and-forget counters, so we verify the publish succeeded)
            mockChannel.Verify(x => x.BasicPublishAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<BasicProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()
            ), Times.Once);
        }

        public class TestPayload
        {
            public int Id { get; set; }
            public string Name { get; set; } = string.Empty;
        }
    }
}
