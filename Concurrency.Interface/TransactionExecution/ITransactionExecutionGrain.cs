using Utilities;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Concurrency.Interface.TransactionExecution
{
    public interface ITransactionExecutionGrain : Orleans.IGrainWithIntegerKey
    {
        // transaction execution
        Task<NonDetFuncResult> ExecuteNonDet(FunctionCall call, TransactionContext ctx);

        // 
        Task<object> ExecuteDet(FunctionCall call, TransactionContext ctx);

        // PACT
        Task<TransactionResult> StartTransaction(string startFunc, object funcInput, List<int> grainAccessInfo, List<string> grainClassName);
        Task ReceiveBatchSchedule(LocalSubBatch batch);
        Task AckBatchCommit(int bid);

        // ACT
        Task<TransactionResult> StartTransaction(string startFunc, object funcInput);
        Task<bool> Prepare(int tid, bool isReader);
        Task Commit(int tid, int maxBeforeBid);
        Task Abort(int tid);

        // hybrid execution
        Task<int> WaitForBatchCommit(int bid);

        Task CheckGC();
    }
}