using Concurrency.Interface.Nondeterministic;
using Concurrency.Interface;
using System;
using System.Collections.Generic;
using System.Diagnostics;
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
        private bool writeLockTaken;        
        private int writeLockTakenByTid;        
        private SemaphoreSlim writeSemaphore;
        private SemaphoreSlim readSemaphore;
        private SortedSet<int> readers;
        private SortedSet<int> writers;
        private SortedSet<int> aborters;

        public S2PLTransactionalState()
        {
            writeLockTaken = false;
            writeLockTakenByTid = -1;            
            writeSemaphore = new SemaphoreSlim(1);
            readSemaphore = new SemaphoreSlim(1);
            readers = new SortedSet<int>();
            writers = new SortedSet<int>();
            aborters = new SortedSet<int>();
        }

        private async Task<TState> AccessState(int tid, TState committedState)
        {
            if (writeLockTaken)
            {
                if (writeLockTakenByTid == tid)
                {
                    //Do nothing since this is another interleaved execution;
                }
                else
                {   //Check the wait-die protocol
                    if (tid < writeLockTakenByTid)
                    {
                        //The following request is for queuing of transactions rather than a critical section
                        await writeSemaphore.WaitAsync();
                        writeLockTaken = true;
                        writeLockTakenByTid = tid;
                        activeState = (TState)committedState.Clone();
                    }
                    else
                    {
                        //abort the transaction
                        aborters.Add(tid);
                        throw new DeadlockAvoidanceException($"Txn {tid} is aborted to avoid deadlock since its tid is larger than txn {writeLockTakenByTid} that holds the lock");                        
                    }
                }
            }
            else
            {
                await writeSemaphore.WaitAsync(); // This should never block but required to make subsequent waits block
                writeLockTaken = true;
                writeLockTakenByTid = tid;
                activeState = (TState)committedState.Clone();
            }
            return activeState;
        }

        public async Task<TState> Read(TransactionContext ctx, CommittedState<TState> committedState)
        {
            var tid = ctx.transactionID;
            if(aborters.Contains(tid))
            {
                throw new Exception($"{tid} has aborted, should go to abort phase");
            }
            if(writeLockTaken)
            {                
                if(writeLockTakenByTid == tid)
                {
                    //No lock downgrade, just return the active copy
                    Debug.Assert(activeState != null);
                    return activeState;
                } else
                {
                    if (tid < writeLockTakenByTid)
                    {
                        readers.Add(tid);
                        //Wait for writer
                        await readSemaphore.WaitAsync();                        
                        return committedState.GetState();
                    } else
                    {
                        aborters.Add(tid);
                        throw new DeadlockAvoidanceException($"Reader txn {tid} is aborted to avoid deadlock since its tid is larger than txn {writeLockTakenByTid} that holds the write lock");
                    }                 
                }
            } else
            {
                readers.Add(tid);
                Debug.Assert(writers.Count == 0);
                if (readers.Count == 1)
                {
                    //First reader downs the semaphore if there are no writers waiting
                    await writeSemaphore.WaitAsync(); //This should not block but is used to block subsequent writers                    
                }                                
                return committedState.GetState();
            }
        }

        public async Task<TState> ReadWrite(TransactionContext ctx, CommittedState<TState> committedState)
        {
            var tid = ctx.transactionID;
            if (aborters.Contains(tid))
            {
                throw new Exception($"{tid} has aborted, should go to abort phase");
            }
            else if (readers.Contains(tid))
            {
                aborters.Add(tid);
                throw new NotImplementedException($"{tid} requests lock upgrade. Not supported in S2PL protocol.");
            }
            if (writeLockTaken)
            {
                if (writeLockTakenByTid == tid)
                {
                    //Do nothing since this is another interleaved execution;
                    Debug.Assert(activeState != null);
                }
                else
                {   //Check the wait-die protocol
                    if (tid < writeLockTakenByTid)
                    {
                        writers.Add(tid);
                        //Wait for other writer
                        await writeSemaphore.WaitAsync();                        
                        writeLockTaken = true;
                        writeLockTakenByTid = tid;
                        activeState = (TState)committedState.GetState().Clone();
                    }
                    else
                    {
                        //abort the transaction                        
                        aborters.Add(tid);
                        throw new DeadlockAvoidanceException($"Writer txn {tid} is aborted to avoid deadlock since its tid is larger than txn {writeLockTakenByTid} that holds the write lock");
                    }
                }
            }
            else
            {                
                //Check for readers
                if(readers.Count == 0)
                {
                    writers.Add(tid);
                    await writeSemaphore.WaitAsync(); // This should never block but required to make subsequent writers block
                    await readSemaphore.WaitAsync(); //This should never block but required to make subsequent readers block                    
                    writeLockTaken = true;
                    writeLockTakenByTid = tid;
                } else
                {
                    if(tid < readers.Max)
                    {
                        //Wait for readers to release the lock
                        writers.Add(tid);
                        await writeSemaphore.WaitAsync();                        
                        writeLockTaken = true;
                        writeLockTakenByTid = tid;                        
                    } else
                    {
                        aborters.Add(tid);
                        throw new DeadlockAvoidanceException($"Writer txn {tid} is aborted to avoid deadlock since its tid is larger than txn {readers.Max} that holds the read lock");
                    }
                }
                activeState = (TState)committedState.GetState().Clone();
            }
            return activeState;
        }
                
        public Task<bool> Prepare(int tid)
        {            
            if(aborters.Contains(tid))
            {
                Debug.Assert(!writers.Contains(tid) && !readers.Contains(tid));
                return Task.FromResult(false);
            } else if(writeLockTaken && writeLockTakenByTid == tid && writers.Contains(tid))
            {
                Debug.Assert(!readers.Contains(tid));
                return Task.FromResult(true);
            } else if(readers.Contains(tid))
            {
                Debug.Assert(!writers.Contains(tid));
                return Task.FromResult(true);
            } else
            {
                //This code path must not be triggered
                Debug.Assert(false);
                return Task.FromResult(false);
            }
        }

        private void CleanUpAndSignal(int tid)
        {
            if(aborters.Contains(tid))
            {
                aborters.Remove(tid);
            }
            else if (writeLockTaken && writeLockTakenByTid == tid && writers.Contains(tid))
            {
                writeLockTaken = false;
                writeLockTakenByTid = -1;
                writers.Remove(tid);
                //Privilege readers over writers (XXX: Not fair queuing, can cause starvation)
                if (readers.Count > 0)
                {
                    //Release all readers
                    //Assumes one transaction can only have one outstanding call to Read/ReadWrite
                    readSemaphore.Release(readers.Count);
                    //Console.WriteLine($"writer releases sem, readers count = {readers.Count}, read sem value = {result}");
                }
                else
                {
                    readSemaphore.Release();
                    writeSemaphore.Release();
                    //Console.WriteLine($"writer releases sem, readers count = {readers.Count}, write sem value = {result}");
                }
            }
            else if (readers.Contains(tid))
            {
                readers.Remove(tid);
                if (readers.Count == 0)
                {
                    //Release one writer                    
                    writeSemaphore.Release();
                    //Console.WriteLine($"reader releases sem, readers count = {readers.Count}, write sem value = {result}");
                }
            } else
            {
                Debug.Assert(false);
            }
        }

        public void Commit(int tid, CommittedState<TState> committedState)
        {            
            var reader = readers.Contains(tid);
            if(!reader)
                committedState.SetState(activeState);
            CleanUpAndSignal(tid);      
        }

        public void Abort(int tid)
        {
            CleanUpAndSignal(tid);
        }

        public TState GetPreparedState(int tid)
        {
            return activeState;
        }
    }
}
