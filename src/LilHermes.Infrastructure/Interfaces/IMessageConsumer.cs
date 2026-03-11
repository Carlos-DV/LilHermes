using System;
using System.Threading;
using System.Threading.Tasks;
using LilHermes.Abstractions.Entities;

namespace LilHermes.Infrastructure.Interfaces
{
    public interface IMessageConsumer : IAsyncDisposable
    {
        Task StartConsumingAsync<T>(Func<T, ulong, Task> messageHandler, CancellationToken cancellationToken = default) where T : class;
        Task StartConsumingWithContextAsync<T>(Func<MessageContext<T>, ulong, Task> messageHandler, CancellationToken cancellationToken = default) where T : class;
        Task StopConsumingAsync();
        Task AckAsync(ulong deliveryTag);
    }
}
