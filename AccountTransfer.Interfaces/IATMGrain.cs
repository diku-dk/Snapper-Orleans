using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Interface;
using Concurrency.Utilities;
using Orleans;
using Orleans.Concurrency;

namespace AccountTransfer.Interfaces
{
    public interface IATMGrain : ITransactionExecutionGrain
    {
        Task<String> getPromise();
        Task setpromise();

        [AlwaysInterleave]
        Task<int> testReentrance(int i);

        [AlwaysInterleave]
        Task setpromise(int i);

        Task<FunctionResult> Transfer(FunctionInput input);

        Task<FunctionResult> TransferOneToMulti(FunctionInput input);
    }
}
