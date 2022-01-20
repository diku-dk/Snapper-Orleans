using System;
using Utilities;
using Concurrency.Interface;
using System.Threading.Tasks;
using Concurrency.Interface.Deterministic;
using Concurrency.Interface.Nondeterministic;

namespace Concurrency.Implementation
{
    public class HybridState<TState> : ITransactionalState<TState> where TState : ICloneable, new()
    {
        private IDetTransactionalState<TState> detStateManager;
        private INonDetTransactionalState<TState> nonDetStateManager;
        private CommittedState<TState> myState;

        // when execution grain is initialized, its hybrid state is initialized
        public HybridState(CCType type = CCType.S2PL) : this(new TState(), type)
        {
            ;
        }

        public HybridState(TState state, CCType type = CCType.S2PL)
        {
            this.myState = new CommittedState<TState>(state);
            // detStateManager: it's read and write operation will return the state directly
            // nonDetStateManager: return the state under the locking protocol
            detStateManager = new Deterministic.DeterministicTransactionalState<TState>();
            switch (type)
            {
                case CCType.S2PL:
                    nonDetStateManager = new Nondeterministic.S2PLTransactionalState<TState>();
                    break;
                case CCType.TS:
                    nonDetStateManager = new Nondeterministic.TimestampTransactionalState<TState>();
                    break;
            }
        }

        Task ITransactionalState<TState>.Abort(int tid)
        {
            try
            {
                nonDetStateManager.Abort(tid);
            }
            catch (Exception e)
            {
                Console.WriteLine($"\n Exception(Abort)::transaction {tid} exception {e.Message}");
            }
            return Task.CompletedTask;
        }

        Task ITransactionalState<TState>.Commit(int tid)
        {
            try
            {
                nonDetStateManager.Commit(tid, myState);
            }
            catch (Exception e)
            {
                Console.WriteLine($"\n Exception(Commit)::transaction {tid} exception {e.Message}");
            }
            return Task.CompletedTask;
        }

        TState ITransactionalState<TState>.GetCommittedState(int bid)
        {
            return myState.GetState();
        }

        TState ITransactionalState<TState>.GetPreparedState(int tid)
        {
            return nonDetStateManager.GetPreparedState(tid);
        }

        Task<bool> ITransactionalState<TState>.Prepare(int tid, bool isWriter)
        {
            return nonDetStateManager.Prepare(tid, isWriter);
        }

        Task<TState> ITransactionalState<TState>.Read(TransactionContext ctx)
        {
            if (ctx.isDet) return detStateManager.Read(ctx, myState.GetState());
            else return nonDetStateManager.Read(ctx, myState);
        }

        Task<TState> ITransactionalState<TState>.ReadWrite(TransactionContext ctx)
        {
            if (ctx.isDet) return detStateManager.ReadWrite(ctx, myState.GetState());
            else return nonDetStateManager.ReadWrite(ctx, myState);
        }
    }
}