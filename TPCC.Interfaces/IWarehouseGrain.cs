using Utilities;
using Concurrency.Interface;
using System.Threading.Tasks;

namespace TPCC.Interfaces
{
    public interface IWarehouseGrain : ITransactionExecutionGrain
    {
        Task<TransactionResult> Init(TransactionContext context, object funcInput);
        Task<TransactionResult> GetWTax(TransactionContext context, object funcInput);
    }
}