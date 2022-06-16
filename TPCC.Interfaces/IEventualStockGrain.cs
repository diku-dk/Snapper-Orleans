using Orleans.Transactions;
using Orleans.Concurrency;
using System.Threading.Tasks;
using Utilities;

namespace TPCC.Interfaces
{
    public interface IEventualStockGrain : Orleans.IGrainWithIntegerKey
    {
        [AlwaysInterleave]
        Task<TransactionResult> StartTransaction(string startFunc, object funcInput);
    }
}
