using Concurrency.Interface.Nondeterministic;
using Concurrency.Interface;
using System;
using System.Collections.Generic;
using System.Diagnostics;
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

        // SemaphoreSlim can limit the number of threads that can concurrently get access to this state
        private SemaphoreSlim writeSemaphore;
        private SemaphoreSlim readSemaphore;
        private SortedSet<int> readers;
        private SortedSet<int> writers;
        private SortedSet<int> aborters;

        // transaction who gets the semophrore will only release it when aborts or commits
        public S2PLTransactionalState()
        {
            writeLockTaken = false;
            writeLockTakenByTid = -1;   // the writer tid who currently takes the write lock
            // initially only one thread can get the semaphore
            // every time a thread gets the semaphore, the count - 1
            // every time a thread releases the semaphore, the count + 1
            // when the count = 0, no thread can get the semaphore until somebody releases the semaphore
            writeSemaphore = new SemaphoreSlim(1, 1);
            readSemaphore = new SemaphoreSlim(0);
            readers = new SortedSet<int>();        // transactions that wait to read
            writers = new SortedSet<int>();
            aborters = new SortedSet<int>();
        }

        // TODO: a transaction can only read the same grain one time (otherwise cannot guarantee repeatable read), but can write the grain multiple times
        // when there are no writers, the reader gets the write semophore, all subsequent writes are blocked, all subsequent reads can read without semophore
        // if the write lock is held by another txn, the reader needs to wait to get read semophore
        public async Task<TState> Read(TransactionContext ctx, CommittedState<TState> committedState)
        {
            var tid = ctx.transactionID;
            if (aborters.Contains(tid)) throw new Exception($"{tid} has aborted, should go to abort phase");
            if (writeLockTaken)
            {
                if (writeLockTakenByTid == tid)
                {
                    Debug.Assert(activeState != null);
                    return activeState;                  //No lock downgrade, just return the active copy
                }
                else
                {
                    // deadlock prevention: wait-die
                    // Ti wants the lock that Tj holds, if Ti < Tj, Ti waits for Tj
                    if (tid < writeLockTakenByTid)
                    {
                        readers.Add(tid);
                        await readSemaphore.WaitAsync(-1);    // wait for read lock
                        Console.WriteLine($"reader {tid} gets read semaphore. ");
                        return committedState.GetState();   // read committed data
                    }
                    else   // if Ti > Tj, abort Ti
                    {
                        aborters.Add(tid);
                        throw new DeadlockAvoidanceException($"Reader txn {tid} is aborted to avoid deadlock since its tid is larger than txn {writeLockTakenByTid} that holds the write lock");
                    }
                }
            }
            else
            {
                // this else branch will happen when:
                // (1) the grain is in initial state, then tid comes
                // (2) last RW / RO txn finished, but the write semaphore is still unavailable, readers.count > 0, the subsequent readers can just read the state committed by that RW txn
                // (3) last RO txn finished, the write semaphore is released, readers.count = 0, then tid comes
                // (4) a reader has held the write semaphore, then tid comes
                var case1 = writeSemaphore.CurrentCount == 1 && readSemaphore.CurrentCount == 0 && writers.Count == 0 && readers.Count == 0;
                var case24 = writeSemaphore.CurrentCount == 0 && readers.Count > 0;
                var case3 = writeSemaphore.CurrentCount == 1 && readers.Count == 0;
                if (!(case1 || case24 || case3))
                {
                    Console.WriteLine($"writeSemaphore.CurrentCount = {writeSemaphore.CurrentCount}, readSemaphore.CurrentCount = {readSemaphore.CurrentCount}");
                    Console.WriteLine($"readers.Count = {readers.Count}, writers.Count = {writers.Count}");
                    foreach (var w in writers) Console.WriteLine($"writers include: {w}");
                }
                Debug.Assert(case1 || case24 || case3);

                readers.Add(tid);
                // if tid gets the write semaphore, the subsequent writer cannot do write, but the subsequent readers can just read
                // at the same time, there might be some writers wait for the write semophore
                // but tid will get the write semaphore immediately since it's already proceeded
                if (readers.Count == 1)
                {
                    await writeSemaphore.WaitAsync(-1);
                    Console.WriteLine($"reader {tid} gets write semaphore. ");
                }
                else Console.WriteLine($"reader {tid} reads without semaphore");
                return committedState.GetState();   // read committed data
            }
        }

        // When there are no readers, the writer gets the read semophore, all subsequent reads are blocked
        // every writer should try to get write semophore
        public async Task<TState> ReadWrite(TransactionContext ctx, CommittedState<TState> committedState)
        {
            var tid = ctx.transactionID;
            if (aborters.Contains(tid)) throw new Exception($"{tid} has aborted, should go to abort phase");
            else if (readers.Contains(tid))
            {
                aborters.Add(tid);
                throw new NotImplementedException($"{tid} requests lock upgrade. Not supported in S2PL protocol.");
            }
            if (writeLockTaken)
            {
                // return activeState directly
                if (writeLockTakenByTid == tid) Debug.Assert(activeState != null);
                else  //Check the wait-die protocol
                {
                    // Ti wants the lock that Tj holds, if Ti < Tj, Ti waits for Tj
                    if (tid < writeLockTakenByTid)
                    {
                        writers.Add(tid);
                        Console.WriteLine($"writer {tid} waits for write semaphore. ");
                        await writeSemaphore.WaitAsync(-1);  // Wait for other writer
                        writeLockTaken = true;
                        writeLockTakenByTid = tid;
                        Console.WriteLine($"writer {tid} gets write semaphore. ");
                        // It's the first time tid accesses the state, so give it a committed version (read committed)
                        // the state is committed by the last writer who releases the write semaphore
                        activeState = (TState)committedState.GetState().Clone();
                    }
                    else   // if Ti > Tj, abort Ti
                    {
                        aborters.Add(tid);
                        throw new DeadlockAvoidanceException($"Writer txn {tid} is aborted to avoid deadlock since its tid is larger than txn {writeLockTakenByTid} that holds the write lock");
                    }
                }
            }
            else
            {
                // this else branch will happen when:
                // (1) the grain is in initial state, then tid comes
                // (2) last RW / RO txn finished, but the write semaphore is still unavailable, readers.count > 0, then tid comes
                // (3) last RO txn finished, the write semaphore is released, readers.count = 0, then tid comes
                // (4) a reader has held the write semaphore, then tid comes
                var case1 = writeSemaphore.CurrentCount == 1 && readSemaphore.CurrentCount == 0 && writers.Count == 0 && readers.Count == 0;
                var case24 = writeSemaphore.CurrentCount == 0 && readers.Count > 0;
                var case3 = writeSemaphore.CurrentCount == 1 && readers.Count == 0;
                if (!(case1 || case24 || case3))
                {
                    Console.WriteLine($"writeSemaphore.CurrentCount = {writeSemaphore.CurrentCount}, readSemaphore.CurrentCount = {readSemaphore.CurrentCount}");
                    Console.WriteLine($"readers.Count = {readers.Count}, writers.Count = {writers.Count}");
                    foreach (var w in writers) Console.WriteLine($"writers include: {w}");
                }
                //Debug.Assert(case1 || case24 || case3);

                // it's possible that at the same time some writers are waiting for the write semaphore
                if (readers.Count == 0)  // which means there is no reader wait to read (when a reader wants to read, it will ask for write semophore)
                {
                    writers.Add(tid);
                    Console.WriteLine($"writer {tid} waits for write semaphore. ");
                    await writeSemaphore.WaitAsync(-1); // tid surely can get the write semophore, but it will block subsequent writers
                    writeLockTaken = true;
                    writeLockTakenByTid = tid;
                    Console.WriteLine($"writer {tid} gets write semaphore. ");
                    //await readSemaphore.WaitAsync(-1);  // tid will block subsequent readers
                }
                else  // readers.Count > 0
                {
                    // (1) last finished writer is holding the write semaphore (2) a reader is holding the write semaphore
                    // all readers actually read the state committed by last writer
                    if (tid < readers.Max)  // we can think the Max reader holds write semophore for all readers
                    {
                        // deadlock prevention: wait-die
                        // Ti wants the lock that Tj holds, if Ti < Tj, Ti waits for Tj
                        writers.Add(tid);
                        Console.WriteLine($"writer {tid} waits for write semaphore. ");
                        await writeSemaphore.WaitAsync(-1);  // Wait for readers to release the write semophore
                        writeLockTaken = true;
                        writeLockTakenByTid = tid;
                        Console.WriteLine($"writer {tid} gets write semaphore. ");
                    }
                    else   // if Ti > Tj, abort Ti
                    {
                        aborters.Add(tid);
                        throw new DeadlockAvoidanceException($"Writer txn {tid} is aborted to avoid deadlock since its tid is larger than txn {readers.Max} that holds the read lock");
                    }
                }
                // when writer gets the semophore, set the current committed state as active, this active state is mutable
                activeState = (TState)committedState.GetState().Clone();
            }
            return activeState;
        }

        public Task<bool> Prepare(int tid)
        {
            Debug.Assert(!aborters.Contains(tid));     // if it's in aborters, must have an exception, will not come to Prepare phase
            Debug.Assert(!(writers.Contains(tid) && readers.Contains(tid)));
            if (writers.Contains(tid))  // if tid is a RW transaction
            {
                Debug.Assert(writeLockTaken && writeLockTakenByTid == tid);
                return Task.FromResult(true);
            }
            else     // if tid is a read-only transaction
            {
                Debug.Assert(readers.Contains(tid));
                return Task.FromResult(true);
            }
        }

        private void CleanUpAndSignal(int tid)
        {
            Debug.Assert(!(writers.Contains(tid) && aborters.Contains(tid)));
            Debug.Assert(!(writers.Contains(tid) && readers.Contains(tid)));
            if (writers.Contains(tid))
            {
                Debug.Assert(writeLockTaken && writeLockTakenByTid == tid);
                writeLockTaken = false;
                writeLockTakenByTid = -1;
                writers.Remove(tid);
                // TODO: Privilege readers over writers can cause starvation
                // If a reader holds the write semophore, and there keep coming more readers, then writer has to wait for all of them
                if (readers.Count > 0)
                {
                    readSemaphore.Release(readers.Count); // enable all blocked readers to read
                    Console.WriteLine($"writer {tid} releases {readers.Count} read semaphores. ");
                }
                else  // readers.Count == 0
                {
                    //readSemaphore.Release();
                    writeSemaphore.Release(); // so next reader can ask for the write semophore, the subsequent readers can just read without any semophore
                    Console.WriteLine($"reader {tid} releases write semaphore. ");
                }
            }
            else   // tid can be in readers and aborters, eg. txn requires read lock then wants to upgrade
            {
                if (readers.Contains(tid))
                {
                    readers.Remove(tid);
                    if (readers.Count == 0)
                    {
                        writeSemaphore.Release();  // so next write can ask for the write semophore, the subsequent readers need to wait for read semophore
                        Console.WriteLine($"{tid} releases write semaphore. ");
                    }
                }

                if (aborters.Contains(tid)) aborters.Remove(tid);
            }
        }

        public void Commit(int tid, CommittedState<TState> committedState)
        {
            if (!readers.Contains(tid)) committedState.SetState(activeState);  // tid is a RW transaction, need to update the state
            CleanUpAndSignal(tid);
        }

        public void Abort(int tid)
        {
            // if tid is a RW transaction, just discard its activeState
            CleanUpAndSignal(tid);
        }

        public TState GetPreparedState(int tid)
        {
            return activeState;
        }
    }
}
