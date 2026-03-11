# LilHermes

Wrapper ligero de RabbitMQ para .NET Standard 2.0. Proporciona abstracciones de publicación y consumo de mensajes con soporte nativo de trazabilidad distribuida (OpenTelemetry) y Dead-Letter Queue (DLQ).

---

## Contenido

- [Requisitos](#requisitos)
- [Proyectos](#proyectos)
- [Instalación vía DI](#instalación-vía-di)
- [Configuración](#configuración)
- [Publicar mensajes](#publicar-mensajes)
- [Consumir mensajes](#consumir-mensajes)
- [Dead-Letter Queue](#dead-letter-queue)
- [OpenTelemetry](#opentelemetry)
- [Comandos de desarrollo](#comandos-de-desarrollo)
- [Tests](#tests)

---

## Requisitos

- .NET Standard 2.0 (aplicaciones .NET 6+, .NET Framework 4.6.1+)
- RabbitMQ corriendo y accesible
- (Opcional) Colector OTLP en `http://localhost:4318` para trazas

---

## Proyectos

| Paquete | Descripción |
|---|---|
| `LilHermes.Abstractions` | Contratos: `MessageBusOptions`, `MessageContext<T>`, enums |
| `LilHermes.Infrastructure` | Implementaciones: publisher, consumer, connection manager, telemetría |

---

## Instalación vía DI

Registra los servicios en `Program.cs` o en tu `Startup.cs`:

```csharp
// Publisher + Consumer (stack completo)
builder.Services.AddLilHermes(options =>
{
    options.ConnectionOptions.HostName = "localhost";
    options.ConnectionOptions.Username = "guest";
    options.ConnectionOptions.Password = "guest";

    options.PublishOptions.Exchange    = "mi-exchange";
    options.PublishOptions.ExchangeType = RabbitMQExchangeType.Direct;

    options.ConsumerOptions.QueueName  = "mi-cola";
    options.ConsumerOptions.ExchangeName = "mi-exchange";
});

// Solo publisher
builder.Services.AddLilHermesPublisher(options => { ... });

// Solo consumer
builder.Services.AddLilHermesConsumer(options => { ... });
```

---

## Configuración

### Conexión (`ConnectionOptions`)

| Propiedad | Default | Descripción |
|---|---|---|
| `HostName` | `127.0.0.1` | Host del broker |
| `Port` | `5672` | Puerto AMQP |
| `Username` / `Password` | `guest` | Credenciales |
| `VirtualHost` | `/` | VHost de RabbitMQ |
| `ConnectionUri` | — | URI completa (alternativa a host/port) |
| `EnabledCluster` | `false` | Activa modo clúster |
| `Endpoints` | — | Lista de nodos del clúster |
| `AutomaticRecoveryEnabled` | `true` | Reconexión automática |
| `NetworkRecoveryInterval` | `5s` | Intervalo entre reintentos de reconexión |

### Publicación (`PublishOptions`)

| Propiedad | Default | Descripción |
|---|---|---|
| `Exchange` | — | Nombre del exchange |
| `ExchangeType` | — | `Direct`, `Topic`, `Fanout`, `Headers` |
| `Persistent` | `true` | Mensajes persistentes en disco |
| `PublisherConfirmationsEnabled` | `true` | Confirmaciones del broker |
| `Priority` | `Normal` | `Low`, `Normal`, `High` |

### Consumo (`ConsumerOptions`)

| Propiedad | Default | Descripción |
|---|---|---|
| `QueueName` | `LilHermes-Queue-v0` | Nombre de la cola |
| `ExchangeName` | — | Exchange al que se vincula |
| `RoutingKeys` | — | Claves de enrutamiento |
| `AcknowledgeMode` | `Manual` | `Auto` o `Manual` |
| `PrefetchCount` | `100` | Mensajes prefetcheados por consumer |
| `MaxRetryCount` | `3` | Reintentos antes de enviar a DLQ |
| `RetryDelayMs` | `1000` | Delay entre reintentos (ms) |
| `EnableDLQ` | `false` | Habilita Dead-Letter Queue |
| `MessageTTL` | `30000` | TTL de mensajes en cola de retry (ms) |

---

## Publicar mensajes

Inyecta `IMessagePublisher` y construye un `MessageContext<T>`:

```csharp
public class MiServicio(IMessagePublisher publisher)
{
    public async Task EnviarPedidoAsync(Pedido pedido, CancellationToken ct)
    {
        var mensaje = new MessageContext<Pedido>(pedido)
        {
            SourceService = "servicio-pedidos",
            // CorrelationId y MessageId se generan automáticamente
        };

        await publisher.PublishAsync(mensaje, routingKey: "pedidos.nuevo", ct);
    }
}
```

### Publicación en lote

```csharp
var mensajes = pedidos.Select(p => new MessageContext<Pedido>(p));
await publisher.PublishBatchAsync(mensajes, routingKey: "pedidos.lote", ct);
```

---

## Consumir mensajes

### Modo simple (solo el payload)

```csharp
public class MiConsumerService(IMessageConsumer consumer) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await consumer.StartConsumingAsync<Pedido>(
            async (pedido, deliveryTag) =>
            {
                // Procesar pedido
                await ProcesarAsync(pedido);
                await consumer.AckAsync(deliveryTag);
            },
            ct);
    }
}
```

### Modo con contexto (acceso a metadatos y trazas)

```csharp
await consumer.StartConsumingWithContextAsync<Pedido>(
    async (context, deliveryTag) =>
    {
        var correlationId = context.CorrelationId;
        var pedido = context.Data;

        await ProcesarAsync(pedido);
        await consumer.AckAsync(deliveryTag);
    },
    ct);
```

---

## Dead-Letter Queue

Cuando `EnableDLQ = true`, LilHermes crea automáticamente la siguiente infraestructura en RabbitMQ:

```
[Exchange principal] → [Cola principal]
                              ↓ (fallo)
                       [Cola de retry] ← TTL → [Exchange principal]
                              ↓ (MaxRetryCount agotado)
                       [Cola parqueada (errores)]
```

Activa DLQ en la configuración del consumer:

```csharp
options.ConsumerOptions.EnableDLQ      = true;
options.ConsumerOptions.MaxRetryCount  = 3;
options.ConsumerOptions.RetryDelayMs   = 2000;
options.ConsumerOptions.MessageTTL     = 60000;
```

Los mensajes que superan `MaxRetryCount` quedan en la cola `{QueueName}-parked` para inspección manual.

---

## OpenTelemetry

LilHermes instrumenta automáticamente las operaciones de publicación y consumo.

### Registrar trazas

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddLilHermesInstrumentation()   // <-- añade el ActivitySource
        .AddOtlpExporter());
```

### Métricas disponibles

| Métrica | Tipo | Descripción |
|---|---|---|
| `messages_published` | Counter | Mensajes publicados |
| `messages_consumed` | Counter | Mensajes consumidos |
| `messages_failed` | Counter | Mensajes fallidos |
| `publish_duration_ms` | Histogram | Duración de publicación |

### Tags de trazas (OTel Semantic Conventions)

- `messaging.system`, `messaging.destination`, `messaging.routing_key`
- `messaging.message_id`, `messaging.correlation_id`
- `rabbitmq.exchange`, `rabbitmq.queue`

La propagación de contexto usa el estándar **W3C Trace Context** (`traceparent` / `tracestate`) a través de los headers AMQP.

---

## Comandos de desarrollo

```bash
# Compilar la solución
dotnet build LilHermes.sln

# Ejecutar todos los tests
dotnet test LilHermes.sln

# Solo tests unitarios
dotnet test test/LilHermes.UnitTests/LilHermes.UnitTests.csproj

# Solo tests de integración (requiere RabbitMQ + colector OTLP en localhost:4318)
dotnet test test/LilHermes.IntegrationTests/LilHermes.IntegrationTests.csproj

# Filtrar por nombre de test
dotnet test LilHermes.sln --filter "FullyQualifiedName~NombreDelTest"

# Generar paquetes NuGet
dotnet pack LilHermes.sln
```

---

## Tests

| Proyecto | Target | Requisitos |
|---|---|---|
| `LilHermes.UnitTests` | `net8.0` | Ninguno |
| `LilHermes.IntegrationTests` | `net8.0` | RabbitMQ + OTLP en `localhost:4318` |

Para levantar RabbitMQ rápidamente con Docker:

```bash
docker run -d --name rabbitmq \
  -p 5672:5672 -p 15672:15672 \
  rabbitmq:3-management
```

Panel de administración disponible en `http://localhost:15672` (guest/guest).

---

## Estructura del proyecto

```
LilHermes/
├── src/
│   ├── LilHermes.Abstractions/       # Contratos y modelos
│   │   ├── Entities/                 # MessageBusOptions, MessageContext<T>, etc.
│   │   └── Enums/                    # RabbitMQExchangeType, AcknowledgeMode, MessagePriority
│   └── LilHermes.Infrastructure/     # Implementaciones
│       ├── Connections/              # RabbitMQConnectionManager
│       ├── Publishers/               # RabbitMQPublisher
│       ├── Consumers/                # RabbitMQConsumer
│       ├── Extensions/               # LilHermesExtensions (DI)
│       └── Telemetry/                # ActivitySource, Meter, propagación W3C
└── test/
    ├── LilHermes.UnitTests/
    └── LilHermes.IntegrationTests/
```
