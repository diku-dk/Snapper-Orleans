using Orleans;
using Orleans.Concurrency;
using System.Threading.Tasks;

namespace Persist.Interfaces
{
    public interface IPersistGrain : IGrainWithIntegerKey
    {
        Task Write(byte[] value);

        Task<long> GetIOCount();

        Task SetIOCount();
    }
}
