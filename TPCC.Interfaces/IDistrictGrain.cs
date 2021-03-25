using Utilities;
using Concurrency.Interface;
using System.Threading.Tasks;

namespace TPCC.Interfaces
{
    public interface IDistrictGrain : ITransactionExecutionGrain
    {
        Task<FunctionResult> Init(FunctionInput functionInput);
        Task<FunctionResult> GetDTax(FunctionInput functionInput);
    }
}
