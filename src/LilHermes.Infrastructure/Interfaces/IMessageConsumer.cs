using System;
using System.Threading.Tasks;

namespace LilHermes.Infrastructure.Interfaces
{
    public interface IMessageConsumer
    {
        Task StartConsumingAsync<T>(Func<T, ulong, Task> messageHandler) where T : class;
        Task StopConsumingAsync();
        Task AckAsync(ulong deliveryTag);
    }
}
