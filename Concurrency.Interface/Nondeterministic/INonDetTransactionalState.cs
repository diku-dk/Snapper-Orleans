﻿using Utilities;
using Orleans.Concurrency;
using System.Threading.Tasks;

namespace Concurrency.Interface.Nondeterministic
{
    public interface INonDetTransactionalState<TState>
    {
        [AlwaysInterleave]
        Task<TState> Read(MyTransactionContext ctx, CommittedState<TState> committedState);

        [AlwaysInterleave]
        Task<TState> ReadWrite(MyTransactionContext ctx, CommittedState<TState> committedState);

        [AlwaysInterleave]
        Task<bool> Prepare(int tid);

        [AlwaysInterleave]
        void Commit(int tid, CommittedState<TState> committedState);

        [AlwaysInterleave]
        void Abort(int tid);

        [AlwaysInterleave]
        TState GetPreparedState(int tid);
    }
}