using Orleans;
using System.Threading.Tasks;

namespace Concurrency.Interface
{
    public interface IConfigurationManagerGrain : IGrainWithIntegerKey
    {
        Task Initialize();
        Task SetIOCount();
        Task<long> GetIOCount();
    }
}
