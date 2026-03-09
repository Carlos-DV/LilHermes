using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LilHermes.Abstractions.Entities;
using LilHermes.Infrastructure.Interfaces;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace LilHermes.Infrastructure.Connections
{
    public class RabbitMQConnectionManager : IMessageConnectionManager
    {
        private readonly MessageBusOptions _options;
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1);
        private readonly ConnectionFactory _factory;
        private IConnection _connection;
        private readonly ILogger<RabbitMQConnectionManager> _logger;

        public RabbitMQConnectionManager(MessageBusOptions options, ILogger<RabbitMQConnectionManager> logger)
        {
            _options = options;
            _factory = new ConnectionFactory()
            {
                HostName = _options.ConnectionOptions.HostName,
                Port = _options.ConnectionOptions.Port,
                UserName = _options.ConnectionOptions.Username,
                Password = _options.ConnectionOptions.Password,
                VirtualHost = _options.ConnectionOptions.VirtualHost,
                ClientProvidedName = _options.ConnectionOptions.ClientProvidedName,
                AutomaticRecoveryEnabled = _options.ConnectionOptions.AutomaticRecoveryEnabled,
                NetworkRecoveryInterval = _options.ConnectionOptions.NetworkRecoveryInterval,
                RequestedHeartbeat = _options.ConnectionOptions.RequestedHeartbeat,
                RequestedChannelMax = _options.ConnectionOptions.RequestedChannelMax,
                RequestedFrameMax = _options.ConnectionOptions.RequestedFrameMax
            };

            if (!string.IsNullOrEmpty(_options.ConnectionOptions.ConnectionUri))
            {
                _factory.Uri = new Uri(_options.ConnectionOptions.ConnectionUri);
            }

            _logger = logger;
        }

        public async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
        {
            await _lock.WaitAsync(cancellationToken);
            try
            {
                if(_connection == null || !_connection.IsOpen)
                {
                    _connection?.Dispose();

                    if (_options.ConnectionOptions.EnabledCluster && _options.ConnectionOptions.Endpoints.Any())
                    {
                        var endpoints = _options.ConnectionOptions.Endpoints
                            .Select(e =>
                            {
                                var parts = e.Split(':');
                                var host = parts[0];
                                var port = parts.Length > 1 && int.TryParse(parts[1], out var p) ? p : _options.ConnectionOptions.Port;
                                return new AmqpTcpEndpoint(host, port);
                            })
                            .ToList();

                        _connection = await _factory.CreateConnectionAsync(endpoints, cancellationToken);
                    }
                    else
                    {
                        _connection = await _factory.CreateConnectionAsync(cancellationToken);
                    }

                    _logger.LogInformation("Connection to RabbitMQ successfully established");
                }
                return _connection;
            }
            finally
            {
                _lock.Release();
            }
        }

        public async ValueTask DisposeAsync()
        {
            _lock?.Dispose();

            if (_connection != null)
            {
                await _connection.CloseAsync();
                _connection.Dispose();
                _connection = null;
            }

            GC.SuppressFinalize(this);
        }
    }
}