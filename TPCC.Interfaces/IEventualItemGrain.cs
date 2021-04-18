using Utilities;
using Orleans.Concurrency;
using System.Threading.Tasks;

namespace TPCC.Interfaces
{
    public interface IEventualItemGrain : Orleans.IGrainWithIntegerKey
    {
        [AlwaysInterleave]
        Task<TransactionResult> StartTransaction(string startFunc, object funcInput);
    }
}
