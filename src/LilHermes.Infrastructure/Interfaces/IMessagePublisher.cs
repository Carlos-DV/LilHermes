using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using LilHermes.Abstractions.Entities;

namespace LilHermes.Infrastructure.Interfaces
{
    public interface IMessagePublisher : IAsyncDisposable
    {
        Task PublishAsync<T>(MessageContext<T> message, string routingKey, CancellationToken cancellationToken = default) where T : class;
        Task PublishBatchAsync<T>(IEnumerable<MessageContext<T>> messages, string routingKey, CancellationToken cancellationToken = default) where T : class;
    }
}