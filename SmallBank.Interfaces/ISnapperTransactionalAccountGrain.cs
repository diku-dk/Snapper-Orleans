using Utilities;
using Concurrency.Interface.TransactionExecution;
using System.Threading.Tasks;

namespace SmallBank.Interfaces
{
    public interface ISnapperTransactionalAccountGrain : ITransactionExecutionGrain
    {
        Task<TransactionResult> Init(TransactionContext context, object funcInput);
        Task<TransactionResult> Balance(TransactionContext context, object funcInput);
        Task<TransactionResult> MultiTransfer(TransactionContext context, object funcInput);
        Task<TransactionResult> Deposit(TransactionContext context, object funcInput);
    }
}