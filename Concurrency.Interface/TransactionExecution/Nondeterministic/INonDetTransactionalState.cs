using System.Threading.Tasks;

namespace Concurrency.Interface.TransactionExecution.Nondeterministic
{
    public interface INonDetTransactionalState<TState>
    {
        Task<TState> Read(int tid, TState committedState);
        Task<TState> ReadWrite(int tid, TState committedState);
        Task<bool> Prepare(int tid, bool isWriter);
        void Commit(int tid, TState committedState);
        void Abort(int tid);
        TState GetPreparedState(int tid);
    }
}