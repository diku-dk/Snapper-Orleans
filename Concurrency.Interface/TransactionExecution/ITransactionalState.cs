using System.Threading.Tasks;

namespace Concurrency.Interface.TransactionExecution
{
    public interface ITransactionalState<TState>
    {
        // PACT
        TState detOp();

        // ACT
        Task<TState> nonDetRead(int tid);
        Task<TState> nonDetReadWrite(int tid);
        Task<bool> Prepare(int tid, bool isReader);
        void Commit(int tid);
        void Abort(int tid);
        TState GetPreparedState(int tid);
        TState GetCommittedState(int tid);
    }
}