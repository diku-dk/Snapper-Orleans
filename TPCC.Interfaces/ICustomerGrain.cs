using Utilities;
using Concurrency.Interface;
using System.Threading.Tasks;

namespace TPCC.Interfaces
{
    public interface ICustomerGrain : ITransactionExecutionGrain
    {
        Task<FunctionResult> Init(FunctionInput functionInput);
        Task<FunctionResult> NewOrder(FunctionInput functionInput);
    }
}
