using Orleans.Concurrency;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Utilities;

namespace Concurrency.Interface.Nondeterministic
{
    public interface INonDetTransactionalState<TState>
    {
        Task<TState> Read(TransactionContext ctx, TState committedState);

        Task<TState> ReadWrite(TransactionContext ctx, TState committedState);

        [AlwaysInterleave]
        Task<bool> Prepare(long tid);
                
        TState Commit(long tid);
        
        void Abort(long tid);

        TState GetPreparedState(long tid);
    }
}
