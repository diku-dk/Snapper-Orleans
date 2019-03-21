using Concurrency.Interface.Nondeterministic;
using Concurrency.Interface.Deterministic;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Utilities;

namespace Concurrency.Implementation.Deterministic
{
    public class DeterministicTransactionalState<TState> : IDetTransactionalState<TState> where TState : ICloneable, new()
    {
        public Task<TState> Read(TransactionContext ctx, TState state)
        {
            return Task.FromResult(state);
        }

        public Task<TState> ReadWrite(TransactionContext ctx, TState state)
        {
            return Task.FromResult(state);
        }
    }
}
