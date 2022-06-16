using Orleans.Transactions;
using Concurrency.Interface;
using System.Threading.Tasks;
using Utilities;

namespace TPCC.Interfaces
{
    public interface IWarehouseGrain : ITransactionExecutionGrain
    {
        Task<TransactionResult> Init(MyTransactionContext context, object funcInput);
        Task<TransactionResult> GetWTax(MyTransactionContext context, object funcInput);
    }
}