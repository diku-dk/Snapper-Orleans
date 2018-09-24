using Concurrency.Utilities;
using Orleans.Concurrency;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Concurrency.Interface
{
    public interface ITransactionExecutionGrain : Orleans.IGrainWithIntegerKey
    {

        /*
         * Client calls this function to submit a determinictic transaction to the transaction coordinator.
         */
        [AlwaysInterleave]
        Task<List<object>> StartTransaction(Dictionary<ITransactionExecutionGrain, int> grainToAccessTimes, String startFunction, List<object> inputs);

        /*  
         * Client calls this function to submit a non-determinictic transaction to the transaction coordinator.
         */
        [AlwaysInterleave]
        Task<List<object>> StartTransaction(String startFunction, List<object> inputs);

        /*
         * Receive batch schedule from the coordinator.
         */
        [AlwaysInterleave]
        Task ReceiveBatchSchedule(BatchSchedule schedule);

        /*
         * Called by other grains to execute a function.
         */
        [AlwaysInterleave]
        Task<List<object>> Execute(FunctionCall call);


        [AlwaysInterleave]
        Task<bool> Prepare(long tid);

        Task Commit(long tid);

        Task Abort(long tid);

        Task ActivateGrain();

    }
}
