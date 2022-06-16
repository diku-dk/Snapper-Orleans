using System;
using Utilities;
using System.Threading.Tasks;
using Concurrency.Interface.Deterministic;

namespace Concurrency.Implementation.Deterministic
{
    public class DeterministicTransactionalState<TState> : IDetTransactionalState<TState> where TState : ICloneable, new()
    {
        public Task<TState> Read(MyTransactionContext ctx, TState state)
        {
            return Task.FromResult(state);
        }

        public Task<TState> ReadWrite(MyTransactionContext ctx, TState state)
        {
            return Task.FromResult(state);
        }
    }
}