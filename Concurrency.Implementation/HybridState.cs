using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Concurrency.Interface.Nondeterministic;

namespace Concurrency.Implementation
{
    public class HybridState<TState> : ITransactionalState<TState> where TState : ICloneable, new()
    {        
        private ITransactionalState<TState> deterministicConcurrencyControl;
        private ITransactionalState<TState> nonDeterministicConcurrencyControl;
        private TState myState;
        private Dictionary<long, bool> myTxnTypesMap;

        public HybridState(TState state, Dictionary<long, bool> txnTypes)
        {
            this.myState = state;
            this.myTxnTypesMap = txnTypes;
            deterministicConcurrencyControl = new Deterministic.DeterministicTransactionalState<TState>(state);
            //nonDeterministicConcurrencyControl = new Nondeterministic.S2PLTransactionalState<TState>(state);
            nonDeterministicConcurrencyControl = new Nondeterministic.TimestampTransactionalState<TState>(state);
        }
        Task ITransactionalState<TState>.Abort(long tid)
        {
            if(this.myTxnTypesMap[tid])
            {
                return deterministicConcurrencyControl.Abort(tid);
            } else
            {
                return nonDeterministicConcurrencyControl.Abort(tid);
            }            
        }

        Task ITransactionalState<TState>.Commit(long tid)
        {
            if (this.myTxnTypesMap[tid])
            {
                return deterministicConcurrencyControl.Commit(tid);
            }
            else
            {
                return nonDeterministicConcurrencyControl.Commit(tid);
            }
        }

        TState ITransactionalState<TState>.GetCommittedState(long tid)
        {
            if (this.myTxnTypesMap[tid])
            {
                return deterministicConcurrencyControl.GetCommittedState(tid);
            }
            else
            {
                return nonDeterministicConcurrencyControl.GetCommittedState(tid);
            }
        }

        TState ITransactionalState<TState>.GetPreparedState(long tid)
        {
            if (this.myTxnTypesMap[tid])
            {
                return deterministicConcurrencyControl.GetPreparedState(tid);
            }
            else
            {
                return nonDeterministicConcurrencyControl.GetPreparedState(tid);
            }
        }

        Task<bool> ITransactionalState<TState>.Prepare(long tid)
        {
            if (this.myTxnTypesMap[tid])
            {
                return deterministicConcurrencyControl.Prepare(tid);
            }
            else
            {
                return nonDeterministicConcurrencyControl.Prepare(tid);
            }
        }

        Task<TState> ITransactionalState<TState>.Read(long tid)
        {
            if (this.myTxnTypesMap[tid])
            {
                return deterministicConcurrencyControl.Read(tid);
            }
            else
            {
                return nonDeterministicConcurrencyControl.Read(tid);
            }
        }

        Task<TState> ITransactionalState<TState>.ReadWrite(long tid)
        {
            if (this.myTxnTypesMap[tid])
            {
                return deterministicConcurrencyControl.ReadWrite(tid);
            }
            else
            {
                return nonDeterministicConcurrencyControl.ReadWrite(tid);
            }
        }

        Task ITransactionalState<TState>.Write(long tid)
        {
            if (this.myTxnTypesMap[tid])
            {
                return deterministicConcurrencyControl.Write(tid);
            }
            else
            {
                return nonDeterministicConcurrencyControl.Write(tid);
            }
        }
    }
}
