using Orleans;
using Utilities;
using System.Threading.Tasks;

namespace SmallBank.Interfaces
{
    public interface IOrleansTransactionalAccountGroupGrain : Orleans.IGrainWithIntegerKey
    {
        [Transaction(TransactionOption.CreateOrJoin)]
        Task<TransactionResult> StartTransaction(string startFunction, FunctionInput inputs);
    }
}
