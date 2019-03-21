using Orleans.Concurrency;
using Utilities;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Concurrency.Interface.Deterministic
{
    public interface IDetTransactionalState<TState>
    {

        Task<TState> Read(TransactionContext ctx, TState committedState);

        Task<TState> ReadWrite(TransactionContext ctx, TState committedState);
    }
}
