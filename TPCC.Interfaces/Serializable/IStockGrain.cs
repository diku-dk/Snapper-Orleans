using Utilities;
using Concurrency.Interface.TransactionExecution;
using System.Threading.Tasks;

namespace TPCC.Interfaces
{
    public interface IStockGrain : ITransactionExecutionGrain
    {
        Task<TransactionResult> Init(TransactionContext context, object funcInput);
        Task<TransactionResult> UpdateStock(TransactionContext context, object funcInput);
    }
}