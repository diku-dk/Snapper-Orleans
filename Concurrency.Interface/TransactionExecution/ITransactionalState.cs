using System.Threading.Tasks;

namespace Concurrency.Interface.TransactionExecution
{
    public interface ITransactionalState<TState>
    {
        void CheckGC();

        // PACT
        TState DetOp();

        // ACT
        Task<TState> NonDetRead(long tid);
        Task<TState> NonDetReadWrite(long tid);
        Task<bool> Prepare(long tid, bool isReader);
        void Commit(long tid);
        void Abort(long tid);
        TState GetPreparedState(long tid);
        TState GetCommittedState(long tid);
    }
}