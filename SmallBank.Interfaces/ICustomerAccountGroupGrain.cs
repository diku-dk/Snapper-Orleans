using Utilities;
using Concurrency.Interface;
using System.Threading.Tasks;
using Orleans.Transactions;

namespace SmallBank.Interfaces
{
    public interface ICustomerAccountGroupGrain : ITransactionExecutionGrain
    {
        Task<TransactionResult> Init(MyTransactionContext context, object funcInput);
        Task<TransactionResult> GetBalance(MyTransactionContext context, object funcInput);
        Task<TransactionResult> MultiTransfer(MyTransactionContext context, object funcInput);
        Task<TransactionResult> Deposit(MyTransactionContext context, object funcInput);
        Task<TransactionResult> MultiTransferNoDeadlock(MyTransactionContext context, object funcInput);
        Task<TransactionResult> MultiTransferWithNOOP(MyTransactionContext context, object funcInput);
        Task<TransactionResult> DepositWithNOOP(MyTransactionContext context, object funcInput);
    }
}