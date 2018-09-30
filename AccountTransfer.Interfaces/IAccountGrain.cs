using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Interface;
using Concurrency.Utilities;
using Orleans;

namespace AccountTransfer.Interfaces
{
    public interface IAccountGrain : ITransactionExecutionGrain
    {
        Task<int> GetBalance();
        Task<FunctionResult> Withdraw(FunctionInput fin);
        Task<FunctionResult> Deposit(FunctionInput fin);


    }
}
