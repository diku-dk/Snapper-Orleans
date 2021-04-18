using Utilities;
using Concurrency.Interface;
using System.Threading.Tasks;

namespace SmallBank.Interfaces
{
    public interface ICustomerAccountGroupGrain : ITransactionExecutionGrain
    {
        Task<TransactionResult> TransactSaving(TransactionContext context, object funcInput);
        Task<TransactionResult> DepositChecking(TransactionContext context, object funcInput);
        Task<TransactionResult> WriteCheck(TransactionContext context, object funcInput);
        Task<TransactionResult> Balance(TransactionContext context, object funcInput);
        Task<TransactionResult> MultiTransfer(TransactionContext context, object funcInput);
        Task<TransactionResult> Init(TransactionContext context, object funcInput);
    }
}