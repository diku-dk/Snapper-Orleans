using Utilities;
using Orleans.Concurrency;
using System.Threading.Tasks;

namespace Concurrency.Interface
{    
    public interface ITransactionalState<TState>
    {
        Task<TState> Read(TransactionContext ctx);

        Task<TState> ReadWrite(TransactionContext ctx);

        [AlwaysInterleave]
        Task<bool> Prepare(int tid);

        [AlwaysInterleave]
        Task Commit(int tid);

        [AlwaysInterleave]
        Task Abort(int tid);

        TState GetPreparedState(int tid);

        TState GetCommittedState(int tid);
    }
}
