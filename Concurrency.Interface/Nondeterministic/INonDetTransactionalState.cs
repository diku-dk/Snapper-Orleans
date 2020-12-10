using Utilities;
using Orleans.Concurrency;
using System.Threading.Tasks;

namespace Concurrency.Interface.Nondeterministic
{
    public enum ConcurrencyType { S2PL, TIMESTAMP };
    public interface INonDetTransactionalState<TState>
    {
        Task<TState> Read(TransactionContext ctx, CommittedState<TState> committedState);

        Task<TState> ReadWrite(TransactionContext ctx, CommittedState<TState> committedState);

        [AlwaysInterleave]
        Task<bool> Prepare(int tid);
                
        void Commit(int tid, CommittedState<TState> committedState);
        
        void Abort(int tid);

        TState GetPreparedState(int tid);
    }
}
