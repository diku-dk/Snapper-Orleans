using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Concurrency.Interface.Deterministic;
using Concurrency.Interface.Nondeterministic;
using Utilities;

namespace Concurrency.Implementation
{
    public class HybridState<TState> : ITransactionalState<TState> where TState : ICloneable, new()
    {        
        private IDetTransactionalState<TState> detStateManager;
        private INonDetTransactionalState<TState> nonDetStateManager;
        private TState myState;

        public HybridState(TState state)
        {
            this.myState = state;            
            detStateManager = new Deterministic.DeterministicTransactionalState<TState>();
            //nonDetStateManager = new Nondeterministic.S2PLTransactionalState<TState>();
            nonDetStateManager = new Nondeterministic.TimestampTransactionalState<TState>();
        }
        Task ITransactionalState<TState>.Abort(int tid)
        {            
            nonDetStateManager.Abort(tid);
            return Task.CompletedTask;                        
        }

        Task ITransactionalState<TState>.Commit(int tid)
        {            
            myState = nonDetStateManager.Commit(tid);
            return Task.CompletedTask;
        }

        TState ITransactionalState<TState>.GetCommittedState(int bid)
        {
            return myState;
        }

        TState ITransactionalState<TState>.GetPreparedState(int tid)
        {            
            return nonDetStateManager.GetPreparedState(tid);
        }

        Task<bool> ITransactionalState<TState>.Prepare(int tid)
        {
            return nonDetStateManager.Prepare(tid);            
        }

        Task<TState> ITransactionalState<TState>.Read(TransactionContext ctx)
        {
            if (ctx.isDeterministic)
            {
                return detStateManager.Read(ctx, myState);
            }
            else
            {
                return nonDetStateManager.Read(ctx, myState);
            }
        }

        Task<TState> ITransactionalState<TState>.ReadWrite(TransactionContext ctx)
        {
            if (ctx.isDeterministic)
            {
                return detStateManager.ReadWrite(ctx, myState);
            }
            else
            {
                return nonDetStateManager.ReadWrite(ctx, myState);
            }
        }
    }
}
