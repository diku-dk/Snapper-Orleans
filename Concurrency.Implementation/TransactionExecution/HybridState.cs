using System;
using Utilities;
using System.Threading.Tasks;
using Concurrency.Interface.TransactionExecution;
using Concurrency.Interface.TransactionExecution.Nondeterministic;
using Concurrency.Implementation.TransactionExecution.Nondeterministic;

namespace Concurrency.Implementation.TransactionExecution
{
    public class HybridState<TState> : ITransactionalState<TState> where TState : ICloneable, new()
    {
        private TState committedState;
        private INonDetTransactionalState<TState> nonDetStateManager;
        
        // when execution grain is initialized, its hybrid state is initialized
        public HybridState() : this(new TState())
        {
            ;
        }

        public HybridState(TState state)
        {
            committedState = state;
            if (Constants.ccType == CCType.S2PL) nonDetStateManager = new S2PLTransactionalState<TState>();
            else if (Constants.ccType == CCType.TS) nonDetStateManager = new TimestampTransactionalState<TState>();
            else throw new Exception("HybridState: Unknown CC type");
        }

        public void CheckGC()
        {
            nonDetStateManager.CheckGC();
        }

        public TState DetOp()
        {
            return committedState;
        }

        public TState GetCommittedState(int bid)
        {
            return committedState;
        }

        public async Task<TState> NonDetRead(int tid)
        {
            return await nonDetStateManager.Read(tid, committedState);
        }

        public async Task<TState> NonDetReadWrite(int tid)
        {
            return await nonDetStateManager.ReadWrite(tid, committedState);
        }

        public Task<bool> Prepare(int tid, bool isReader)
        {
            return nonDetStateManager.Prepare(tid, isReader);
        }

        public void Commit(int tid)
        {
            nonDetStateManager.Commit(tid, committedState);
        }

        public void Abort(int tid)
        {
            nonDetStateManager.Abort(tid);
        }

        public TState GetPreparedState(int tid)
        {
            return nonDetStateManager.GetPreparedState(tid);
        }
    }
}