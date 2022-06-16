using Orleans;
using System.Threading.Tasks;

namespace Concurrency.Interface.Configuration
{
    public interface IGlobalConfigGrain : IGrainWithIntegerKey
    {
        Task ConfigGlobalEnv();
        Task SetIOCount();
        Task<long> GetIOCount();

        Task CheckGC();
    }
}