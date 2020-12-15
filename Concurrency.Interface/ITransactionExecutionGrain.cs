using System;
using Utilities;
using Orleans.Concurrency;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Concurrency.Interface
{
    public interface ITransactionExecutionGrain : Orleans.IGrainWithIntegerKey
    {
        /*
         * Client calls this function to submit a determinictic transaction to the transaction coordinator.
         */
        [AlwaysInterleave]
        Task<TransactionResult> StartTransaction(Dictionary<int, int> grainAccessInformation, string startFunction, FunctionInput inputs);

        /*  
         * Client calls this function to submit a non-determinictic transaction to the transaction coordinator.
         */
        [AlwaysInterleave]
        Task<TransactionResult> StartTransaction(string startFunction, FunctionInput inputs);

        /*
         * Receive batch schedule from the coordinator.
         */
        [AlwaysInterleave]
        Task ReceiveBatchSchedule(DeterministicBatchSchedule schedule);

        /*
         * Called by other grains to execute a function.
         */
        [AlwaysInterleave]
        Task<FunctionResult> Execute(FunctionCall call);

        [AlwaysInterleave]
        Task<bool> Prepare(int tid);

        [AlwaysInterleave]
        Task Commit(int tid);

        [AlwaysInterleave]
        Task Abort(int tid);

        [AlwaysInterleave]
        Task<int> WaitForBatchCommit(int bid);

        [AlwaysInterleave]
        Task AckBatchCommit(int bid);
    }
}
