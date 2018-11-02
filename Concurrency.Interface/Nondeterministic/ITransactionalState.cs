using Orleans.Concurrency;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Concurrency.Interface.Nondeterministic
{
    public interface ITransactionalState<TState>
    {

        Task<TState> Read(long tid);

        Task<TState> ReadWrite(long tid);

        Task Write(long tid);

        [AlwaysInterleave]
        Task<bool> Prepare(long tid);

        [AlwaysInterleave]
        Task Commit(long tid);

        [AlwaysInterleave]
        Task Abort(long tid);

        TState GetPreparedState(long tid);

        TState GetCommittedState(long tid);
    }
}
