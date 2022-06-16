using System.Threading.Tasks;

namespace Concurrency.Interface.TransactionExecution.Nondeterministic
{
    public interface INonDetTransactionalState<TState>
    {
        void CheckGC();
        Task<TState> Read(long tid, TState committedState);
        Task<TState> ReadWrite(long tid, TState committedState);
        Task<bool> Prepare(long tid, bool isWriter);
        void Commit(long tid, TState committedState);
        void Abort(long tid);
        TState GetPreparedState(long tid);
    }
}