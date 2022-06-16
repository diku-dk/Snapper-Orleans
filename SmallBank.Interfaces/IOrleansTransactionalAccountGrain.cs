using Orleans;
using Utilities;
using System.Threading.Tasks;

namespace SmallBank.Interfaces
{
    public interface IOrleansTransactionalAccountGrain : IGrainWithIntegerKey
    {
        [Transaction(TransactionOption.CreateOrJoin)]
        Task<TransactionResult> StartTransaction(string startFunc, object funcInput);

        Task SetIOCount();
        Task<long> GetIOCount();
    }
}
