using System.Threading.Tasks;

namespace Concurrency.Interface.TransactionExecution
{
    public interface ITransactionalState<TState>
    {
        void CheckGC();

        // PACT
        TState DetOp();

        // ACT
        Task<TState> NonDetRead(int tid);
        Task<TState> NonDetReadWrite(int tid);
        Task<bool> Prepare(int tid, bool isReader);
        void Commit(int tid);
        void Abort(int tid);
        TState GetPreparedState(int tid);
        TState GetCommittedState(int tid);
    }
}