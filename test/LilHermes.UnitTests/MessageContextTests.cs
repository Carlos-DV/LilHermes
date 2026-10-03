using System.Text.Json;
using FluentAssertions;
using LilHermes.Abstractions.Entities;

namespace LilHermes.UnitTests
{
    public class MessageContextTests
    {
        public class Pedido
        {
            public int Id { get; set; }
            public string Cliente { get; set; } = string.Empty;
        }

        private static Pedido NuevoPedido() => new() { Id = 7, Cliente = "Carlos DV" };

        [Fact]
        public void Constructor_UsesUtcTimestamp()
        {
            var before = DateTime.UtcNow;
            var context = new MessageContext<Pedido>(NuevoPedido());
            var after = DateTime.UtcNow;

            context.Timestamp.Kind.Should().Be(DateTimeKind.Utc);
            context.Timestamp.Should().BeOnOrAfter(before).And.BeOnOrBefore(after);
        }

        [Fact]
        public void Create_AssignsDataSourceServiceAndMessageId()
        {
            var pedido = NuevoPedido();

            var context = MessageContext.Create(pedido, "servicio-pedidos", "pedido-7");

            context.Data.Should().BeSameAs(pedido);
            context.SourceService.Should().Be("servicio-pedidos");
            context.MessageId.Should().Be("pedido-7");
            context.Metadata.Should().BeEmpty();
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        public void Create_GeneratesMessageId_WhenNotProvided(string? messageId)
        {
            var context = MessageContext.Create(NuevoPedido(), "servicio-pedidos", messageId);

            Guid.TryParse(context.MessageId, out _).Should().BeTrue();
        }

        [Fact]
        public void Create_GeneratesNewCorrelationIdEachCall()
        {
            var first = MessageContext.Create(NuevoPedido(), "servicio-pedidos");
            var second = MessageContext.Create(NuevoPedido(), "servicio-pedidos");

            Guid.TryParse(first.CorrelationId, out _).Should().BeTrue();
            first.CorrelationId.Should().NotBe(second.CorrelationId);
        }

        [Fact]
        public void Create_UsesUtcTimestamp()
        {
            var before = DateTime.UtcNow;
            var context = MessageContext.Create(NuevoPedido(), "servicio-pedidos");
            var after = DateTime.UtcNow;

            context.Timestamp.Kind.Should().Be(DateTimeKind.Utc);
            context.Timestamp.Should().BeOnOrAfter(before).And.BeOnOrBefore(after);
        }

        [Fact]
        public void Create_Throws_WhenDataIsNull()
        {
            Action act = () => MessageContext.Create<Pedido>(null!, "servicio-pedidos");

            act.Should().Throw<ArgumentNullException>().WithParameterName("data");
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("  ")]
        public void Create_Throws_WhenSourceServiceIsMissing(string? sourceService)
        {
            Action act = () => MessageContext.Create(NuevoPedido(), sourceService!);

            act.Should().Throw<ArgumentException>().WithParameterName("sourceService");
        }

        [Fact]
        public void Create_RoundTripsThroughJson()
        {
            // Mismas opciones que usan RabbitMQPublisher (por defecto) y RabbitMQConsumer
            var consumerOptions = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
            var context = MessageContext.Create(NuevoPedido(), "servicio-pedidos", "pedido-7");

            var json = JsonSerializer.Serialize(context);
            var copy = JsonSerializer.Deserialize<MessageContext<Pedido>>(json, consumerOptions)!;

            copy.MessageId.Should().Be("pedido-7");
            copy.CorrelationId.Should().Be(context.CorrelationId);
            copy.SourceService.Should().Be("servicio-pedidos");
            copy.Data.Id.Should().Be(7);
            copy.Data.Cliente.Should().Be("Carlos DV");
            copy.Timestamp.Kind.Should().Be(DateTimeKind.Utc);
            copy.Timestamp.Should().Be(context.Timestamp);
        }
    }
}
