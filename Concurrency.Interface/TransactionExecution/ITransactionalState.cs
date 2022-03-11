using Utilities;
using Orleans.Concurrency;
using System.Threading.Tasks;

namespace Concurrency.Interface.TransactionExecution
{
    public interface ITransactionalState<TState>
    {
        [AlwaysInterleave]
        Task<TState> Read(TransactionContext ctx);

        [AlwaysInterleave]
        Task<TState> ReadWrite(TransactionContext ctx);

        [AlwaysInterleave]
        Task<bool> Prepare(int tid, bool isWriter);

        [AlwaysInterleave]
        Task Commit(int tid);

        [AlwaysInterleave]
        Task Abort(int tid);

        [AlwaysInterleave]
        TState GetPreparedState(int tid);

        [AlwaysInterleave]
        TState GetCommittedState(int tid);
    }
}