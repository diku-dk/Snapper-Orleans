using Concurrency.Interface.Nondeterministic;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Utilities;

namespace Concurrency.Implementation.Nondeterministic
{
    public class S2PLTransactionalState<TState> : INonDetTransactionalState<TState> where TState : ICloneable, new()
    {
        // In-memory version of the persistent state.        
        private TState activeState;
        private bool lockTaken;        
        private int lockTakenByTid;        
        private SemaphoreSlim stateLock;        

        public S2PLTransactionalState()
        {
            lockTaken = false;
            lockTakenByTid = 0;            
            stateLock = new SemaphoreSlim(1);
        }

        private async Task<TState> AccessState(int tid, TState committedState)
        {
            if (lockTaken)
            {
                if (lockTakenByTid == tid)
                {
                    //Do nothing since this is another interleaved execution;
                }
                else
                {   //Check the wait-die protocol
                    if (tid < lockTakenByTid)
                    {
                        //The following request is for queuing of transactions rather than a critical section
                        await stateLock.WaitAsync();
                        lockTaken = true;
                        lockTakenByTid = tid;
                        activeState = (TState)committedState.Clone();
                    }
                    else
                    {
                        //abort the transaction                        
                        throw new Exception($"Txn {tid} is aborted to avoid deadlock since its tid is larger than txn {lockTakenByTid} that holds the lock");                        
                    }
                }
            }
            else
            {
                await stateLock.WaitAsync(); // This should never block but required to make subsequent waits block
                lockTaken = true;
                lockTakenByTid = tid;
                activeState = (TState)committedState.Clone();
            }
            return activeState;
        }

        public Task<TState> Read(TransactionContext ctx, TState committedState)
        {
            // Use a single lock for now
            return AccessState(ctx.transactionID, committedState);
        }

        public Task<TState> ReadWrite(TransactionContext ctx, TState committedState)
        {
            // Use a single lock right now
            return AccessState(ctx.transactionID, committedState);
        }
                
        public Task<bool> Prepare(int tid)
        {            
            return Task.FromResult(lockTaken && lockTakenByTid == tid);            
        }

        private void CleanUp()
        {
            lockTaken = false;
            lockTakenByTid = 0;            
            stateLock.Release();
        }

        public TState Commit(int tid)
        {
            if (lockTaken && lockTakenByTid == tid)
            {                
                CleanUp();
            } else
            {
                //Nothing really to do but should not have received the commit call
            }
            return activeState;
        }

        public void Abort(int tid)
        {
            if (lockTaken && lockTakenByTid == tid)
            {
                CleanUp();
            }
        }

        public TState GetPreparedState(int tid)
        {
            return activeState;
        }
    }
}
