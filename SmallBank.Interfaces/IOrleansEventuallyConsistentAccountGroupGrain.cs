using Utilities;
using Orleans.Concurrency;
using System.Threading.Tasks;

namespace SmallBank.Interfaces
{
    public interface IOrleansEventuallyConsistentAccountGroupGrain : Orleans.IGrainWithIntegerKey
    {
        [AlwaysInterleave]
        Task<TransactionResult> StartTransaction(string startFunc, object funcInput);
    }
}
