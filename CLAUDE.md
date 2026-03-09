# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

LilHermes is a lightweight C# wrapper library for RabbitMQ targeting .NET Standard 2.0. It provides publisher/consumer abstractions with OpenTelemetry distributed tracing and Dead-Letter Queue (DLQ) support.

## Commands

```bash
# Build
dotnet build LilHermes.sln

# Run all tests
dotnet test LilHermes.sln

# Run only unit tests
dotnet test test/LilHermes.UnitTests/LilHermes.UnitTests.csproj

# Run only integration tests (requires a running RabbitMQ + OTLP collector at localhost:4318)
dotnet test test/LilHermes.IntegrationTests/LilHermes.IntegrationTests.csproj

# Run a single test by name
dotnet test LilHermes.sln --filter "FullyQualifiedName~TestName"

# Build NuGet packages (auto-generated on build via GeneratePackageOnBuild)
dotnet pack LilHermes.sln
```

## Architecture

Two projects ship as NuGet packages (both `netstandard2.0`):

- **`LilHermes.Abstractions`** — pure contracts: `MessageBusOptions` (connection + publish + consume config), `MessageContext<T>` (message envelope with correlation/trace IDs), and enums (`RabbitMQExchangeType`, `AcknowledgeMode`, `MessagePriority`).
- **`LilHermes.Infrastructure`** — implementations wired via DI extension methods in `LilHermesExtensions`:
  - `AddLilHermes()` — registers publisher + consumer
  - `AddLilHermesPublisher()` — publisher only
  - `AddLilHermesConsumer()` — consumer only
  - `AddLilHermesInstrumentation()` — OpenTelemetry `ActivitySource` + `Meter`

### Key Infrastructure Classes

| Class | Responsibility |
|---|---|
| `RabbitMQConnectionManager` | Lazy connection pool with `SemaphoreSlim`, auto-recovery |
| `RabbitMQPublisher` | Single + batch publish, publisher confirmations, OTel tracing |
| `RabbitMQConsumer` | Async event-driven consumption, DLQ retry queues, JSON deserialization |
| `LilHermesTelemetry` | OTel `ActivitySource` and `Meter` definitions, semantic tag constants |

### Data Flow

1. Caller creates a `MessageContext<T>` with correlation/trace metadata and calls `IMessagePublisher.PublishAsync()`.
2. `RabbitMQPublisher` propagates OTel trace context into AMQP message headers and publishes to the configured exchange.
3. `RabbitMQConsumer` reads headers to restore trace context, deserializes `MessageContext<T>`, invokes the registered handler.
4. On handler failure, the message is routed to the DLQ retry queue (up to `MaxRetryCount`, default 3); after exhausting retries it lands in the dead-letter exchange.

### Testing Notes

- Unit test mocks (`RabbitMqPublisherTests`, `BatchTest`) are currently commented out; `EndToEndTests` is the active test and requires a live RabbitMQ broker and an OTLP HTTP exporter at `http://localhost:4318`.
- Test projects target `net8.0`; source projects target `netstandard2.0`.
