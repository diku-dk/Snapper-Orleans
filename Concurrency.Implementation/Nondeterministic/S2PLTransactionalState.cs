using Concurrency.Interface.Nondeterministic;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Concurrency.Implementation.Nondeterministic
{
    public class S2PLTransactionalState<TState> : ITransactionalState<TState> where TState : ICloneable, new()
    {
        // In-memory version of the persistent state.
        private TState committedState;
        private TState activeState;
        private bool lockTaken;
        private long lockTakenByTid;        
        private SemaphoreSlim stateLock;

        public S2PLTransactionalState(TState s)
        {
            committedState = s;
            lockTaken = false;
            lockTakenByTid = 0;
            stateLock = new SemaphoreSlim(1);
        }
        private async Task<TState> AccessState(long tid)
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
                        throw new Exception("Abort the transaction");                        
                    }
                }
            }
            else
            {
                stateLock.Wait(); // This should never block but required to make subsequent waits block
                lockTaken = true;
                lockTakenByTid = tid;
                activeState = (TState)committedState.Clone();
            }
            return activeState;
        }

        public Task<TState> Read(long tid)
        {
            // Use a single lock for now
            return AccessState(tid);
        }

        public Task<TState> ReadWrite(long tid)
        {
            // Use a single lock right now
            return AccessState(tid);
        }

        public Task Write(long tid)
        {
            // Use a single lock right now
            return AccessState(tid);
        }
        public Task<bool> Prepare(long tid)
        {
            //Nothing to do here until logging is integrated
            return Task.FromResult(true);
        }

        private void CleanUp()
        {
            lockTaken = false;
            lockTakenByTid = 0;
            activeState = default(TState);
            stateLock.Release();
        }

        public Task Commit(long tid)
        {
            committedState = activeState;            
            CleanUp();
            return Task.CompletedTask;
        }

        public Task Abort(long tid)
        {
            CleanUp();
            return Task.CompletedTask;
        }
    }
}
