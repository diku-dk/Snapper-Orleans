using Utilities;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Concurrency.Interface.TransactionExecution
{
    public interface ITransactionExecutionGrain : Orleans.IGrainWithIntegerKey
    {
        // PACT
        Task<TransactionResult> StartTransaction(string startFunc, object funcInput, List<int> grainAccessInfo, List<string> grainClassName);
        Task<object> ExecuteDet(FunctionCall call, TransactionContext ctx);
        Task ReceiveBatchSchedule(LocalSubBatch batch);
        Task AckBatchCommit(int bid);

        // ACT
        Task<TransactionResult> StartTransaction(string startFunc, object funcInput);
        Task<NonDetFuncResult> ExecuteNonDet(FunctionCall call, TransactionContext ctx);
        Task<bool> Prepare(int tid, bool isReader);
        Task Commit(int tid, int maxBeforeLocalBid, int maxBeforeGlobalBid);
        Task Abort(int tid);

        // hybrid execution
        Task WaitForBatchCommit(int bid);

        Task CheckGC();
    }
}