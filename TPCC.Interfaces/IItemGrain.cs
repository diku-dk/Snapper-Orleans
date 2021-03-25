using Utilities;
using Concurrency.Interface;
using System.Threading.Tasks;

namespace TPCC.Interfaces
{
    public interface IItemGrain : ITransactionExecutionGrain
    {
        Task<FunctionResult> Init(FunctionInput functionInput);
        Task<FunctionResult> GetItemsPrice(FunctionInput functionInput);   // an invalid I_ID get price -1
    }
}
