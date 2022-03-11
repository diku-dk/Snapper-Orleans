using Orleans;
using System.Threading.Tasks;

namespace Concurrency.Interface.Configuration
{
    public interface ILocalConfigGrain : IGrainWithIntegerKey
    {
        Task ConfigLocalEnv();
        Task SetIOCount();
        Task<long> GetIOCount();
    }
}