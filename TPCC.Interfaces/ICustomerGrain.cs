using Utilities;
using Concurrency.Interface;
using System.Threading.Tasks;

namespace TPCC.Interfaces
{
    public interface ICustomerGrain : ITransactionExecutionGrain
    {
        Task<TransactionResult> Init(TransactionContext ctx, object funcInput);
        Task<TransactionResult> NewOrder(TransactionContext ctx, object funcInput);
    }
}