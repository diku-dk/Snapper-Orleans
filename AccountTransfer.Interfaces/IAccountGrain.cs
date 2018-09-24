using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Interface;
using Orleans;

namespace AccountTransfer.Interfaces
{
    public interface IAccountGrain : ITransactionExecutionGrain
    {
        Task<int> GetBalance();        
    }
}
