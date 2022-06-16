using Orleans;
using Orleans.Transactions;
using System.Threading.Tasks;
using Utilities;

namespace SmallBank.Interfaces
{
    public interface IOrleansTransactionalAccountGroupGrain : IGrainWithIntegerKey
    {
        [Transaction(TransactionOption.CreateOrJoin)]
        Task<TransactionResult> StartTransaction(string startFunc, object funcInput);

        Task SetIOCount();
        Task<long> GetIOCount();
    }
}
