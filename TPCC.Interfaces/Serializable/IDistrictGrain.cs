using Utilities;
using Concurrency.Interface.TransactionExecution;
using System.Threading.Tasks;

namespace TPCC.Interfaces
{
    public interface IDistrictGrain : ITransactionExecutionGrain
    {
        Task<TransactionResult> Init(TransactionContext ctx, object funcInput);
        Task<TransactionResult> GetDTax(TransactionContext ctx, object funcInput);
    }
}