using LilHermes.Abstractions.Entities;
using LilHermes.Infrastructure.Interfaces;
using LilHermes.Infrastructure.Telemetry;
using LilHermes.Infrastructure.Utils;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using System;
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
        private readonly ResiliencePipeline _retryPipeline;

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

            _retryPipeline = new ResiliencePipelineBuilder()
                .AddRetry(new RetryStrategyOptions
                {
                    MaxRetryAttempts = 3,
                    Delay = TimeSpan.FromMilliseconds(200),
                    BackoffType = DelayBackoffType.Exponential,
                    OnRetry = args =>
                    {
                        _logger.LogWarning("Publish retry attempt {AttemptNumber} after {Delay}ms",
                            args.AttemptNumber + 1, args.RetryDelay.TotalMilliseconds);
                        return default;
                    }
                })
                .Build();
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
            return PublishBatchAsync(new[] { message }, routingKey, cancellationToken);
        }

        public async Task PublishBatchAsync<T>(IEnumerable<MessageContext<T>> messages, string routingKey, CancellationToken cancellationToken = default) where T : class
        {
            if (!messages.Any()) return;
            if (string.IsNullOrEmpty(routingKey)) throw new ArgumentNullException(nameof(routingKey));

            var sw = Stopwatch.StartNew();

            using (var batchActivity = _telemetry.ActivitySource.StartActivity(_telemetry.BatchActivity, ActivityKind.Internal))
            {
                batchActivity?.SetTag(LilHermesTelemetry.MessagingBatchCount, messages.Count());
                await _channelLock.WaitAsync(cancellationToken);
                try
                {
                    await EnsureChannelAsync(cancellationToken);
                    var publishTasks = new List<Task>();

                    foreach (var message in messages)
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
                                ContentType = _options.PublishOptions.ContentType,
                                ContentEncoding = _options.PublishOptions.ContentEncoding,
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
                            _telemetry.MessagesPublished.Add(1);
                            activity?.SetStatus(ActivityStatusCode.Ok);
                            activity?.AddEvent(new ActivityEvent(LilHermesTelemetry.PublishedOk));
                        }
                    }
                    await PublishToMB(publishTasks, cancellationToken);
                }
                catch(Exception ex)
                {
                    batchActivity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                    batchActivity?.AddExceptionCompat(ex);
                    throw;
                }
                finally
                {
                    _channelLock.Release();
                    sw.Stop();
                    _telemetry.PublishDurationMs.Record(sw.Elapsed.TotalMilliseconds);
                }
            };
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
                await _retryPipeline.ExecuteAsync(async ct =>
                {
                    await Task.WhenAll(publishTasks);
                }, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred during message publication");
                throw;
            }
            finally
            {
                publishTasks.Clear();
            }
        }
    }
}
