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
        Task<TransactionResult> StartTransaction(string startFunc, object funcInput, Dictionary<int, Tuple<string, int>> grainAccessInfo);

        /*  
         * Client calls this function to submit a non-determinictic transaction to the transaction coordinator.
         */
        [AlwaysInterleave]
        Task<TransactionResult> StartTransaction(string startFunc, object funcInput);

        /*
         * Receive batch schedule from the coordinator.
         */
        [AlwaysInterleave]
        Task ReceiveBatchSchedule(DeterministicBatchSchedule schedule);

        /*
         * Called by other grains to execute a function.
         */
        [AlwaysInterleave]
        Task<FunctionResult> Execute(FunctionCall call, TransactionContext ctx);

        [AlwaysInterleave]
        Task<Tuple<bool, int, int, bool>> Prepare(int tid, bool doLogging);    // <vote, maxBeforeBid, minAfterBid, isConsecutive>

        [AlwaysInterleave]
        Task Commit(int tid, int maxBeforeBid, bool doLogging);

        [AlwaysInterleave]
        Task Abort(int tid);

        [AlwaysInterleave]
        Task<int> WaitForBatchCommit(int bid);

        [AlwaysInterleave]
        Task AckBatchCommit(int bid);
    }
}