using System;
using Utilities;
using System.Threading.Tasks;
using System.Collections.Generic;
using Orleans.Transactions;

namespace Concurrency.Interface
{
    public interface ITransactionExecutionGrain : Orleans.IGrainWithIntegerKey
    {
        /// <summary>
        /// Use this interface to submit a PACT to Snapper.
        /// </summary>
        Task<TransactionResult> StartTransaction(string startFunc, object funcInput, Dictionary<int, Tuple<string, int>> grainAccessInfo);

        /// <summary>
        /// Use this interface to submit an ACT to Snapper.
        /// </summary>
        Task<TransactionResult> StartTransaction(string startFunc, object funcInput);

        /// <summary>
        /// Use this interface to submit an ACT to Snapper. This interface is only used for getting breakdown transaction latency.
        /// </summary>
        Task<TransactionResult> StartTransactionAndGetTime(string startFunc, object funcInput);

        /// <summary>
        /// Use this interface to send a sub-batch from a coordinator to a grain.
        /// </summary>
        Task ReceiveBatchSchedule(DeterministicBatchSchedule schedule);

        /// <summary>
        /// Use this interface to invoke a function on another grain.
        /// </summary>
        Task<FunctionResult> Execute(FunctionCall call, MyTransactionContext ctx);

        /// <summary>
        /// Use this interface to send a 2PC Prepare message to a participant grain.
        /// </summary>
        Task<bool> Prepare(int tid, bool doLogging);

        /// <summary>
        /// Use this interface to send a 2PC Commit message to a participant grain.
        /// </summary>
        Task Commit(int tid, int maxBeforeBid, bool doLogging);

        /// <summary>
        /// Use this interface to send a 2PC Abort message to a participant grain.
        /// </summary>
        Task Abort(int tid);

        /// <summary>
        /// Use this interface to wait for the commit of a specific batch. This interface is only used for hybrid execution.
        /// </summary>
        Task<int> WaitForBatchCommit(int bid);

        /// <summary>
        /// Use this interface to send a BatchCommit message from a coordinator to a grain.
        /// </summary>
        Task AckBatchCommit(int bid);
    }
}