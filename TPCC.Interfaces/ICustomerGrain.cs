using Utilities;
using Concurrency.Interface;
using System.Threading.Tasks;
using Orleans.Transactions;

namespace TPCC.Interfaces
{
    public interface ICustomerGrain : ITransactionExecutionGrain
    {
        Task<TransactionResult> Init(MyTransactionContext ctx, object funcInput);
        Task<TransactionResult> NewOrder(MyTransactionContext ctx, object funcInput);
    }
}