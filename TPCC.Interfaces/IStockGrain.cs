using Utilities;
using Concurrency.Interface;
using System.Threading.Tasks;

namespace TPCC.Interfaces
{
    public interface IStockGrain : ITransactionExecutionGrain
    {
        Task<FunctionResult> Init(FunctionInput functionInput);
        Task<FunctionResult> UpdateStock(FunctionInput functionInput);
    }
}
