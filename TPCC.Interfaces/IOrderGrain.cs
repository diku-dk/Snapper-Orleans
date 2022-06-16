using Utilities;
using Concurrency.Interface;
using System.Threading.Tasks;
using Orleans.Transactions;

namespace TPCC.Interfaces
{
    public interface IOrderGrain : ITransactionExecutionGrain
    {
        Task<TransactionResult> Init(MyTransactionContext context, object funcInput);
        Task<TransactionResult> AddNewOrder(MyTransactionContext context, object funcInput);
    }
}