using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Interface;
using Utilities;
using Orleans;

namespace AccountTransfer.Interfaces
{
    public interface IAccountGrain : ITransactionExecutionGrain
    {
        Task<FunctionResult> GetBalance(FunctionInput fin);
        Task<FunctionResult> Withdraw(FunctionInput fin);
        Task<FunctionResult> Deposit(FunctionInput fin); 

        Task<int> ActivateGrain();
    }
}
