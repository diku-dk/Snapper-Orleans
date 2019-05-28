using System;
using Orleans;
using Utilities;
using System.Threading.Tasks;

namespace SmallBank.Interfaces
{
    public interface IOrleansTransactionalAccountGroupGrain : Orleans.IGrainWithIntegerKey, Orleans.IGrainWithGuidKey
    {
        [Transaction(TransactionOption.CreateOrJoin)]
        Task<FunctionResult> StartTransaction(String startFunction, FunctionInput inputs);
    }
}
