using Utilities;
using Concurrency.Interface;
using System.Threading.Tasks;
using Orleans.Transactions;

namespace TPCC.Interfaces
{
    public interface IStockGrain : ITransactionExecutionGrain
    {
        Task<TransactionResult> Init(MyTransactionContext context, object funcInput);
        Task<TransactionResult> UpdateStock(MyTransactionContext context, object funcInput);
    }
}