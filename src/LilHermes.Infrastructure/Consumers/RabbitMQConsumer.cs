using LilHermes.Abstractions.Entities;
using LilHermes.Infrastructure.Interfaces;
using LilHermes.Infrastructure.Telemetry;
using LilHermes.Infrastructure.Utils;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace LilHermes.Infrastructure.Consumers
{
    public class RabbitMQConsumer : IMessageConsumer
    {
        private readonly MessageBusOptions _options;
        private readonly IMessageConnectionManager _connectionManager;
        private IChannel _channel;
        private AsyncEventingBasicConsumer _consumer;
        private string _consumerTag;
        private bool _isConfigured = false;
        private readonly ILogger<RabbitMQConsumer> _logger;
        private readonly LilHermesTelemetry _telemetry;
        private readonly RabbitMQQueueStructure _queueStructure;

        private static readonly JsonSerializerOptions _jsonOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            NumberHandling = JsonNumberHandling.AllowReadingFromString
        };

        public RabbitMQConsumer(MessageBusOptions options, IMessageConnectionManager connectionManager, ILogger<RabbitMQConsumer> logger, LilHermesTelemetry telemetry)
        {
            _options = options;
            _connectionManager = connectionManager;
            _logger = logger;
            _telemetry = telemetry;

            var mainQ = _options.ConsumerOptions.QueueName;
            var mainEx = _options.ConsumerOptions.ExchangeName;

            _queueStructure = new RabbitMQQueueStructure()
            {
                MainQueue = mainQ,
                MainExchange = mainEx,
                RetryQueue = $"{mainQ}.retry",
                RetryExchange = $"{mainQ}.retry.ex",
                ParkedQueue = $"{mainQ}.error",
                ParkedExchange = $"{mainQ}.error.ex"
            };
        }

        public async Task AckAsync(ulong deliveryTag)
        {
            if (_channel != null)
            {
                await _channel.BasicAckAsync(deliveryTag, multiple: false);
                _logger.LogInformation("The message was accepted");
            }
        }

        private async Task NackAsync(ulong deliveryTag, bool requeue = false)
        {
            if (_channel != null)
            {
                await _channel.BasicNackAsync(deliveryTag, multiple: false, requeue: requeue);
                _logger.LogWarning("A message could not be accepted");
            }
        }

        public async Task StartConsumingAsync<T>(Func<T, ulong, Task> messageHandler) where T : class
        {
            if (messageHandler == null)
                throw new ArgumentNullException(nameof(messageHandler));

            await EnsureChannelAsync();
            await ConfigureQueueAsync();

            _consumer = new AsyncEventingBasicConsumer(_channel);

            _consumer.ReceivedAsync += async (sender, args) =>
            {
                var parentContext = LilHermesTraceContextHelper.ExtractActivityContext(args.BasicProperties);

                using (var activity = _telemetry.ActivitySource.StartActivity(_telemetry.ConsumeActivity, ActivityKind.Consumer, parentContext ?? default))
                {
                    try
                    {
                        var body = args.Body.ToArray();
                        var json = Encoding.UTF8.GetString(body);

                        var context = JsonSerializer.Deserialize<MessageContext<T>>(json, _jsonOptions);
                        var message = context?.Data;

                        if (context == null)
                        {
                            _logger.LogError("Error deserializing context");
                            throw new ArgumentException("Error deserializing context");
                        }

                        if (message == null)
                        {
                            _logger.LogError("Error deserializing message");
                            throw new ArgumentException("Error deserializing message");
                        }

                        _logger.LogInformation("Message received: {MessageId}", context.MessageId);

                        // Tags
                        activity?.SetTag(LilHermesTelemetry.MessagingSystem, LilHermesTelemetry.RabbitMQ);
                        activity?.SetTag(LilHermesTelemetry.RabbitMQQueue, _options.ConsumerOptions.QueueName);
                        activity?.SetTag(LilHermesTelemetry.MessagingPayloadSize, body.Length);
                        activity?.SetTag(LilHermesTelemetry.MessagingMessageId, context.MessageId);
                        activity?.SetTag(LilHermesTelemetry.MessagingCorrelationId, context.CorrelationId);

                        using (var processActivity = _telemetry.ActivitySource.StartActivity(_telemetry.ProcessActivity, ActivityKind.Internal))
                        {
                            try
                            {
                                processActivity?.SetTag(LilHermesTelemetry.MessagingCorrelationId, context.CorrelationId);
                                processActivity?.SetTag(LilHermesTelemetry.MessagingMessageId, context.MessageId);
                                await messageHandler(message, args.DeliveryTag);
                                processActivity?.SetStatus(ActivityStatusCode.Ok);
                                processActivity?.AddEvent(new ActivityEvent(LilHermesTelemetry.ProcessOk));
                            }
                            catch (Exception ex)
                            {
                                processActivity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                                processActivity?.AddExceptionCompat(ex);
                                _logger.LogError(ex, "An error occurred in the business logic: {ErrorMessage}", ex.Message);
                                throw;
                            }
                        }

                        activity?.SetStatus(ActivityStatusCode.Ok);
                        activity?.AddEvent(new ActivityEvent(LilHermesTelemetry.ConsumedOk));
                    }
                    catch (Exception ex)
                    {

                        if (_options.ConsumerOptions.EnableDLQ)
                        {
                            long deathCount = GetDeathCount(args.BasicProperties.Headers);
                            if (deathCount >= 3)
                            {
                                _logger.LogCritical("Excessive retries (3). Moved to Parked.");
                                await PublishToParkedAsync(args);
                                await AckAsync(args.DeliveryTag);
                            }
                            else
                            {
                                _logger.LogWarning("Error. Retrying attempt {RetryCount}", deathCount + 1);
                                await NackAsync(args.DeliveryTag, requeue: false);
                            }
                        }
                        else
                        {
                            await NackAsync(args.DeliveryTag, true);
                        }

                        activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                        activity?.AddExceptionCompat(ex);
                        _logger.LogError(ex, "Error processing message: {ErrorMessage}", ex.Message);
                    }
                }
            };

            _consumerTag = await _channel.BasicConsumeAsync(queue: _options.ConsumerOptions.QueueName, autoAck: false, consumer: _consumer);
        }

        public async Task StopConsumingAsync()
        {
            if (_channel != null && !string.IsNullOrEmpty(_consumerTag))
            {
                try
                {
                    await _channel.BasicCancelAsync(_consumerTag);
                }
                catch { }
            }
            _consumerTag = null;
            _consumer = null;
        }

        private async Task EnsureChannelAsync()
        {
            var connection = await _connectionManager.GetConnectionAsync();

            if (_channel == null || _channel.IsClosed)
            {
                _channel?.Dispose();
                _channel = await connection.CreateChannelAsync();
                _isConfigured = false;
            }
        }

        private async Task ConfigureQueueAsync()
        {
            if (_isConfigured) return;

            await _channel.ExchangeDeclareAsync(_options.ConsumerOptions.ExchangeName, _options.ConsumerOptions.ExchangeType.ToRabbitString(), durable: true);

            Dictionary<string, object> queueArgs = null;

            if (_options.ConsumerOptions.EnableDLQ)
            {
                // Queue parked
                await _channel.ExchangeDeclareAsync(_queueStructure.ParkedExchange, ExchangeType.Direct, durable: true);
                await _channel.QueueDeclareAsync(_queueStructure.ParkedQueue, durable: true, exclusive: false, autoDelete: false);
                await _channel.QueueBindAsync(_queueStructure.ParkedQueue, _queueStructure.ParkedExchange, "parked");

                // Queue retry
                var retryArgs = new Dictionary<string, object>
                {
                    { "x-dead-letter-exchange", _queueStructure.MainExchange },
                    { "x-message-ttl", _options.ConsumerOptions.MessageTTL }
                };
                await _channel.ExchangeDeclareAsync(_queueStructure.RetryExchange, _options.ConsumerOptions.ExchangeType.ToRabbitString(), durable: true);
                await _channel.QueueDeclareAsync(_queueStructure.RetryQueue, durable: true, exclusive: false, autoDelete: false, arguments: retryArgs);
                await _channel.QueueBindAsync(_queueStructure.RetryQueue, _queueStructure.RetryExchange, "#");

                queueArgs = new Dictionary<string, object>
                {
                    { "x-dead-letter-exchange", _queueStructure.RetryExchange }
                };
            }

            await _channel.QueueDeclareAsync(_options.ConsumerOptions.QueueName, durable: true, exclusive: false, autoDelete: false, arguments: queueArgs);
            await _channel.BasicQosAsync(0, _options.ConsumerOptions.PrefetchCount, false);

            // Bindings
            foreach (var routingKey in _options.ConsumerOptions.RoutingKeys)
            {
                await _channel.QueueBindAsync(_options.ConsumerOptions.QueueName, _options.ConsumerOptions.ExchangeName, routingKey);
            }

            _isConfigured = true;
        }

        private long GetDeathCount(IDictionary<string, object> headers)
        {
            if (headers != null && headers.ContainsKey("x-death"))
            {
                var deathList = (IList<object>)headers["x-death"];
                if (deathList.Count > 0)
                {
                    var deathEntry = (IDictionary<string, object>)deathList[0];
                    return (long)deathEntry["count"];
                }
            }
            return 0;
        }

        private async Task PublishToParkedAsync(BasicDeliverEventArgs args, CancellationToken ct = default)
        {
            var props = new BasicProperties
            {
                Persistent = args.BasicProperties.Persistent,
                MessageId = args.BasicProperties.MessageId,
                CorrelationId = args.BasicProperties.CorrelationId,
                Timestamp = args.BasicProperties.Timestamp,
                Headers = args.BasicProperties.Headers,
            };

            await _channel.BasicPublishAsync(
                exchange: _queueStructure.ParkedExchange,
                routingKey: "parked",
                mandatory: false,
                basicProperties: props,
                body: args.Body,
                cancellationToken: ct
            );
        }
    }
}