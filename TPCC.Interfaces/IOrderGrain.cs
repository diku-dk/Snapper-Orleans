using Utilities;
using Concurrency.Interface;
using System.Threading.Tasks;

namespace TPCC.Interfaces
{
    public interface IOrderGrain : ITransactionExecutionGrain
    {
        Task<FunctionResult> Init(FunctionInput functionInput);
        Task<FunctionResult> AddNewOrder(FunctionInput functionInput);
    }
}