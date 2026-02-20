using System;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace LilHermes.Infrastructure.Interfaces
{
    public interface IMessageConnectionManager : IAsyncDisposable
    {
        Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default);
    }
}