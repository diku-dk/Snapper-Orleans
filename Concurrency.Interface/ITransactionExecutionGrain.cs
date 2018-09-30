using Concurrency.Utilities;
using Orleans.Concurrency;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Concurrency.Interface
{
    public interface ITransactionExecutionGrain : Orleans.IGrainWithIntegerKey, Orleans.IGrainWithGuidKey
    {

        /*
         * Client calls this function to submit a determinictic transaction to the transaction coordinator.
         */
        [AlwaysInterleave]
        Task<FunctionResult> StartTransaction(Dictionary<Guid, int> grainToAccessTimes, String startFunction, FunctionInput inputs);

        /*  
         * Client calls this function to submit a non-determinictic transaction to the transaction coordinator.
         */
        [AlwaysInterleave]
        Task<FunctionResult> StartTransaction(String startFunction, FunctionInput inputs);

        /*
         * Receive batch schedule from the coordinator.
         */
        [AlwaysInterleave]
        Task ReceiveBatchSchedule(BatchSchedule schedule);

        /*
         * Called by other grains to execute a function.
         */
        [AlwaysInterleave]
        Task<FunctionResult> Execute(FunctionCall call);

        [AlwaysInterleave]
        Task<bool> Prepare(long tid);

        Task Commit(long tid);

        Task Abort(long tid);

        Task ActivateGrain();
    }
}
