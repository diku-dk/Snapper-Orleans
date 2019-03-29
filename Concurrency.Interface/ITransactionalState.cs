using Orleans.Concurrency;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Utilities;

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
