using LilHermes.Abstractions.Entities;
using LilHermes.Infrastructure.Interfaces;
using LilHermes.Infrastructure.Telemetry;
using LilHermes.Infrastructure.Utils;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace LilHermes.Infrastructure.Publishers
{
    public class RabbitMQPublisher : IMessagePublisher
    {
        private readonly MessageBusOptions _options;
        private readonly IMessageConnectionManager _connectionManager;
        private readonly SemaphoreSlim _channelLock = new SemaphoreSlim(1, 1);
        private IChannel _channel;
        private bool _exchangeDeclared = false;
        private readonly CreateChannelOptions _createChannelOptions;
        private readonly ILogger<RabbitMQPublisher> _logger;
        private readonly LilHermesTelemetry _telemetry;
        public RabbitMQPublisher(MessageBusOptions options, IMessageConnectionManager connectionManager, ILogger<RabbitMQPublisher> logger, LilHermesTelemetry telemetry)
        {
            _options = options;
            _connectionManager = connectionManager;
            _createChannelOptions = new CreateChannelOptions(
                publisherConfirmationsEnabled: _options.PublishOptions.PublisherConfirmationsEnabled, 
                publisherConfirmationTrackingEnabled: _options.PublishOptions.PublisherConfirmationsEnabled
            );
            _logger = logger;
            _telemetry = telemetry;
        }
        public async ValueTask DisposeAsync()
        {
            _channelLock?.Dispose();

            if (_channel != null)
            {
                await _channel.CloseAsync();
                _channel.Dispose();
                _channel = null;
            }

            GC.SuppressFinalize(this);
        }

        public Task PublishAsync<T>(MessageContext<T> message, string routingKey, CancellationToken cancellationToken = default) where T : class
        {
            if (message is IEnumerable || message is List<T>)
            {
                _logger.LogError("Use PublishBatchAsync for collections");
                throw new InvalidOperationException("Use PublishBatchAsync for collections");
            }

            return PublishBatchAsync(new[] { message }, routingKey, cancellationToken);
        }

        public async Task PublishBatchAsync<T>(IEnumerable<MessageContext<T>> messages, string routingKey, CancellationToken cancellationToken = default) where T : class
        {
            var messageList = messages.ToList();
            if (messageList.Count == 0) return;
            if (string.IsNullOrEmpty(routingKey)) throw new ArgumentNullException(nameof(routingKey));

            var maxRetries = _options.PublishOptions.MaxPublishRetries;

            for (var attempt = 1; attempt <= maxRetries; attempt++)
            {
                using (var batchActivity = _telemetry.ActivitySource.StartActivity(_telemetry.BatchActivity, ActivityKind.Internal))
                {
                    batchActivity?.SetTag(LilHermesTelemetry.MessagingBatchCount, messageList.Count);
                    await _channelLock.WaitAsync(cancellationToken);
                    try
                    {
                        await EnsureChannelAsync(cancellationToken);
                        var publishTasks = new List<Task>();

                        foreach (var message in messageList)
                        {
                            using (var activity = _telemetry.ActivitySource.StartActivity(_telemetry.PublishActivity, ActivityKind.Producer))
                            {
                                var json = JsonSerializer.Serialize(message);
                                var body = Encoding.UTF8.GetBytes(json);

                                activity?.SetTag(LilHermesTelemetry.MessagingSystem, LilHermesTelemetry.RabbitMQ);
                                activity?.SetTag(LilHermesTelemetry.MessagingDestination, _options.PublishOptions.Exchange);
                                activity?.SetTag(LilHermesTelemetry.MessagingRoutingKey, routingKey);
                                activity?.SetTag(LilHermesTelemetry.MessagingMessageId, message.MessageId);
                                activity?.SetTag(LilHermesTelemetry.MessagingCorrelationId, message.CorrelationId);
                                activity?.SetTag(LilHermesTelemetry.MessagingPayloadSize, body.Length);
                                activity?.SetTag(LilHermesTelemetry.RabbitMQExchange, _options.PublishOptions.ExchangeType.ToRabbitString());

                                var props = new BasicProperties
                                {
                                    Persistent = _options.PublishOptions.Persistent,
                                    MessageId = message.MessageId,
                                    CorrelationId = message.CorrelationId,
                                    Timestamp = new AmqpTimestamp(new DateTimeOffset(message.Timestamp).ToUnixTimeSeconds()),
                                    Headers = new Dictionary<string, object>()
                                };

                                if (activity != null)
                                    LilHermesTraceContextHelper.InjectTraceContext(props, activity);

                                ValueTask publishTask = _channel.BasicPublishAsync(
                                    exchange: _options.PublishOptions.Exchange,
                                    routingKey: routingKey,
                                    body: body,
                                    mandatory: false,
                                    basicProperties: props,
                                    cancellationToken: cancellationToken
                                );

                                publishTasks.Add(publishTask.AsTask());
                                activity?.SetStatus(ActivityStatusCode.Ok);
                                activity?.AddEvent(new ActivityEvent(LilHermesTelemetry.PublisedhOk));
                            }
                        }
                        await PublishToMB(publishTasks, cancellationToken);
                        return;
                    }
                    catch (AlreadyClosedException ex) when (attempt < maxRetries)
                    {
                        batchActivity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                        batchActivity?.AddExceptionCompat(ex);
                        _logger.LogWarning(ex, "Channel lost during publish, resetting. Retry {RetryAttempt}/{MaxRetries}", attempt, maxRetries);
                        _channel?.Dispose();
                        _channel = null;
                        _exchangeDeclared = false;
                    }
                    catch (Exception ex)
                    {
                        batchActivity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                        batchActivity?.AddExceptionCompat(ex);
                        _logger.LogError(ex, "An error occurred during publication");
                        throw;
                    }
                    finally
                    {
                        _channelLock.Release();
                    }
                }

                await Task.Delay(TimeSpan.FromSeconds(attempt), cancellationToken);
            }
        }

        private async Task EnsureChannelAsync(CancellationToken cancellationToken)
        {
            var connection = await _connectionManager.GetConnectionAsync(cancellationToken);

            if (_channel == null || _channel.IsClosed)
            {
                _channel?.Dispose();
                _channel = await connection.CreateChannelAsync(_createChannelOptions, cancellationToken);
                _exchangeDeclared = false;
            }

            if (!_exchangeDeclared)
            {
                await _channel.ExchangeDeclareAsync(
                    exchange: _options.PublishOptions.Exchange,
                    type: _options.PublishOptions.ExchangeType.ToRabbitString(),
                    durable: true,
                    cancellationToken: cancellationToken
                );
                _exchangeDeclared = true;
            }
        }

        private async Task PublishToMB(List<Task> publishTasks, CancellationToken cancellationToken)
        {
            if (publishTasks.Count == 0) return;
            try
            {
                await Task.WhenAll(publishTasks);
            }
            finally
            {
                publishTasks.Clear();
            }
        }
    }
}