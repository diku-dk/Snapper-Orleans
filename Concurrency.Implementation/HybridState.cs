using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Concurrency.Interface.Nondeterministic;
using Utilities;

namespace Concurrency.Implementation
{
    public class HybridState<TState> : ITransactionalState<TState> where TState : ICloneable, new()
    {        
        private ITransactionalState<TState> deterministicConcurrencyControl;
        private ITransactionalState<TState> nonDeterministicConcurrencyControl;
        private TState myState;

        public HybridState(TState state)
        {
            this.myState = state;            
            deterministicConcurrencyControl = new Deterministic.DeterministicTransactionalState<TState>(state);
            //nonDeterministicConcurrencyControl = new Nondeterministic.S2PLTransactionalState<TState>(state);
            nonDeterministicConcurrencyControl = new Nondeterministic.TimestampTransactionalState<TState>(state);
        }
        Task ITransactionalState<TState>.Abort(long tid)
        {
            //XXX: Should not be called for deterministic transactions
            return nonDeterministicConcurrencyControl.Abort(tid);                        
        }

        Task ITransactionalState<TState>.Commit(long tid)
        {
            //XXX: Should not be called for deterministic transactions                        
            return nonDeterministicConcurrencyControl.Commit(tid);            
        }

        TState ITransactionalState<TState>.GetCommittedState(long bid)
        {
            //XXX: Should not be called for non-deterministic transactions                        
            return deterministicConcurrencyControl.GetCommittedState(bid);
        }

        TState ITransactionalState<TState>.GetPreparedState(long tid)
        {
            //XXX: Should not be called for deterministic transactions
            return nonDeterministicConcurrencyControl.GetPreparedState(tid);
        }

        Task<bool> ITransactionalState<TState>.Prepare(long tid)
        {
            //XXX: Should not be called for deterministic transactions
            return nonDeterministicConcurrencyControl.Prepare(tid);            
        }

        Task<TState> ITransactionalState<TState>.Read(TransactionContext ctx)
        {
            if (ctx.isDeterministic)
            {
                return deterministicConcurrencyControl.Read(ctx);
            }
            else
            {
                return nonDeterministicConcurrencyControl.Read(ctx);
            }
        }

        Task<TState> ITransactionalState<TState>.ReadWrite(TransactionContext ctx)
        {
            if (ctx.isDeterministic)
            {
                return deterministicConcurrencyControl.ReadWrite(ctx);
            }
            else
            {
                return nonDeterministicConcurrencyControl.ReadWrite(ctx);
            }
        }

        Task ITransactionalState<TState>.Write(TransactionContext ctx)
        {
            if (ctx.isDeterministic)
            {
                return deterministicConcurrencyControl.Write(ctx);
            }
            else
            {
                return nonDeterministicConcurrencyControl.Write(ctx);
            }
        }
    }
}
