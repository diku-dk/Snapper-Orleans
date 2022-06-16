using Utilities;
using System.Threading.Tasks;
using System.Collections.Generic;
using System;

namespace Concurrency.Interface.TransactionExecution
{
    public interface ITransactionExecutionGrain : Orleans.IGrainWithIntegerKey
    {
        // PACT
        Task<TransactionResult> StartTransaction(string startFunc, object funcInput, List<int> grainAccessInfo, List<string> grainClassName);
        Task<Tuple<object, DateTime>> ExecuteDet(FunctionCall call, TransactionContext ctx);
        Task ReceiveBatchSchedule(LocalSubBatch batch);
        Task AckBatchCommit(long bid);

        // ACT
        Task<TransactionResult> StartTransaction(string startFunc, object funcInput);
        Task<Tuple<NonDetFuncResult, DateTime>> ExecuteNonDet(FunctionCall call, TransactionContext ctx);
        Task<bool> Prepare(long tid, bool isReader);
        Task Commit(long tid, long maxBeforeLocalBid, long maxBeforeGlobalBid);
        Task Abort(long tid);

        // hybrid execution
        Task WaitForBatchCommit(long bid);

        Task CheckGC();
    }
}