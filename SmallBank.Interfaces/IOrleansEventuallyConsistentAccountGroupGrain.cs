using Orleans.Transactions;
using Orleans.Concurrency;
using System.Threading.Tasks;
using Utilities;

namespace SmallBank.Interfaces
{
    public interface IOrleansEventuallyConsistentAccountGroupGrain : Orleans.IGrainWithIntegerKey
    {
        [AlwaysInterleave]
        Task<TransactionResult> StartTransaction(string startFunc, object funcInput);
    }
}
