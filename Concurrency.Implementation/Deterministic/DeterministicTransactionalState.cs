using Concurrency.Interface.Nondeterministic;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Concurrency.Implementation.Deterministic
{
    public class DeterministicTransactionalState<TState> : ITransactionalState<TState> where TState : ICloneable, new()
    {
        private TState state;

        public DeterministicTransactionalState(TState s)
        {
            state = s;
        }
        public Task Abort(long tid)
        {
            throw new NotImplementedException();
        }

        public Task Commit(long tid)
        {
            throw new NotImplementedException();
        }

        public Task<bool> Prepare(long tid)
        {
            throw new NotImplementedException();
        }

        public Task<TState> Read(long tid)
        {
            return Task.FromResult(state);
        }

        public Task<TState> ReadWrite(long tid)
        {
            return Task.FromResult(state);
        }

        public Task Write(long tid)
        {
            throw new NotImplementedException();
        }

        public TState GetPreparedState(long tid)
        {
            throw new NotSupportedException();
        }

        public TState GetCommittedState(long tid)
        {
            throw new NotSupportedException();
        }
    }
}
