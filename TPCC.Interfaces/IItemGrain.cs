using Utilities;
using Concurrency.Interface;
using System.Threading.Tasks;

namespace TPCC.Interfaces
{
    public interface IItemGrain : ITransactionExecutionGrain
    {
        Task<TransactionResult> Init(TransactionContext context, object funcInput);
        Task<TransactionResult> GetItemsPrice(TransactionContext context, object funcInput);   // an invalid I_ID get price -1
    }
}