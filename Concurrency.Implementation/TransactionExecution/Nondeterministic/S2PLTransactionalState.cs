using System;
using System.Linq;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Interface.TransactionExecution.Nondeterministic;

namespace Concurrency.Implementation.TransactionExecution.Nondeterministic
{
    public class S2PLTransactionalState<TState> : INonDetTransactionalState<TState> where TState : ICloneable, new()
    {
        private TState activeState;     // In-memory version of the persistent state

        private SortedDictionary<long, Tuple<bool, TaskCompletionSource<bool>>> waitinglist;   // bool: isReader
        private SortedSet<long> concurrentReaders; // minWorkReader used to decide if a writer can be added after concurrent working readers
        private long maxWaitWriter;   // decide if a reader can be added before waiting writers

        // transaction who gets the semophore will only release it when aborts or commits
        public S2PLTransactionalState()
        {
            var descendingComparer = Comparer<long>.Create((x, y) => y.CompareTo(x));
            waitinglist = new SortedDictionary<long, Tuple<bool, TaskCompletionSource<bool>>>(descendingComparer);
            concurrentReaders = new SortedSet<long>();
            maxWaitWriter = -1;
        }

        public void CheckGC()
        {
            if (waitinglist.Count != 0) Console.WriteLine($"S2PLTransactionalState: waitinglist.Count = {waitinglist.Count}");
            if (concurrentReaders.Count != 0) Console.WriteLine($"S2PLTransactionalState: concurrentReaders.Count = {concurrentReaders.Count}");
        }

        public async Task<TState> Read(long tid, TState committedState)
        {
            if (waitinglist.Count == 0)  // if nobody is reading or writing the grain
            {
                var mylock = new TaskCompletionSource<bool>();
                mylock.SetResult(true);
                waitinglist.Add(tid, new Tuple<bool, TaskCompletionSource<bool>>(true, mylock));
                Debug.Assert(concurrentReaders.Count == 0);
                concurrentReaders.Add(tid);
                return committedState;
            }
            if (concurrentReaders.Count > 0)  // there are multiple readers reading the grain now
            {
                if (waitinglist.ContainsKey(tid))   // tid wants to read again
                {
                    Debug.Assert(waitinglist[tid].Item1 && waitinglist[tid].Item2.Task.IsCompleted);   // tid must be a reader
                    return committedState;
                }
                var mylock = new TaskCompletionSource<bool>();
                if (tid > maxWaitWriter)    // check if this reader can be put in front of the waiting writers
                {
                    mylock.SetResult(true);
                    waitinglist.Add(tid, new Tuple<bool, TaskCompletionSource<bool>>(true, mylock));
                    concurrentReaders.Add(tid);
                    return committedState;
                }
                // otherwise, the reader need to be added after the waiting writer
                waitinglist.Add(tid, new Tuple<bool, TaskCompletionSource<bool>>(true, mylock));
                await mylock.Task;
                return committedState;
            }
            // otherwise, right now there is only one writer working
            Debug.Assert(!waitinglist.First().Value.Item1 && waitinglist.First().Value.Item2.Task.IsCompleted);
            if (waitinglist.ContainsKey(tid))    // tid has been added as a writer before
            {
                Debug.Assert(tid == waitinglist.First().Key);
                return activeState;
            }
            if (tid < waitinglist.First().Key)
            {
                var mylock = new TaskCompletionSource<bool>();
                waitinglist.Add(tid, new Tuple<bool, TaskCompletionSource<bool>>(true, mylock));
                await mylock.Task;
                return committedState;
            }
            throw new DeadlockAvoidanceException($"txn {tid} is aborted to avoid deadlock. ");
        }

        public async Task<TState> ReadWrite(long tid, TState committedState)
        {
            if (waitinglist.Count == 0)    // if nobody is reading or writing the grain
            {
                var mylock = new TaskCompletionSource<bool>();
                mylock.SetResult(true);
                waitinglist.Add(tid, new Tuple<bool, TaskCompletionSource<bool>>(false, mylock));
                activeState = (TState)committedState.Clone();
                return activeState;
            }
            if (waitinglist.ContainsKey(tid))
            {
                Debug.Assert(waitinglist[tid].Item2.Task.IsCompleted);  // tid must be reading or writing the grain right now
                if (waitinglist[tid].Item1)    // if tid has been added as a reader before
                {
                    Debug.Assert(concurrentReaders.Count > 0);     // right now there must be readers working
                    throw new DeadlockAvoidanceException($"txn {tid} is aborted because lock upgrade is not allowed. ");
                }
                return activeState;  // if tid has been added as a writer before, this writer must be working now
            }
            if (concurrentReaders.Count > 0)  // right now there are multiple readers reading
            {
                if (tid < concurrentReaders.Min)
                {
                    var mylock = new TaskCompletionSource<bool>();
                    waitinglist.Add(tid, new Tuple<bool, TaskCompletionSource<bool>>(false, mylock));
                    maxWaitWriter = Math.Max(maxWaitWriter, tid);
                    await mylock.Task;
                    activeState = (TState)committedState.Clone();
                    return activeState;
                }
                throw new DeadlockAvoidanceException($"txn {tid} is aborted to avoid deadlock. ");
            }
            // otherwise, if there is a writer working
            if (tid < waitinglist.First().Key)
            {
                var mylock = new TaskCompletionSource<bool>();
                waitinglist.Add(tid, new Tuple<bool, TaskCompletionSource<bool>>(false, mylock));
                await mylock.Task;
                activeState = (TState)committedState.Clone();
                return activeState;
            }
            throw new DeadlockAvoidanceException($"txn {tid} is aborted because txn {waitinglist.First().Key} is writing now. ");
        }

        public Task<bool> Prepare(long tid, bool isReader)
        {
            Debug.Assert(waitinglist.ContainsKey(tid) && isReader == waitinglist[tid].Item1);
            if (isReader) CleanUpAndSignal(tid);   // release the read lock directly
            return Task.FromResult(true);
        }

        private void CleanUpAndSignal(long tid)
        {
            if (!waitinglist.ContainsKey(tid)) return;   // which means tis has been aborted before do any read write on this grain
            var isReader = waitinglist[tid].Item1;
            waitinglist.Remove(tid);
            if (isReader)
            {
                concurrentReaders.Remove(tid);
                if (concurrentReaders.Count == 0) Debug.Assert(waitinglist.Count == 0 || !waitinglist.First().Value.Item1);
            }
            if (concurrentReaders.Count == 0)
            {
                maxWaitWriter = -1;
                if (waitinglist.Count == 0) return;
                if (waitinglist.First().Value.Item1)   // if next waiting transaction is a reader
                {
                    Debug.Assert(!isReader);   // tid must be a writer
                    var tasks = new List<TaskCompletionSource<bool>>();
                    foreach (var txn in waitinglist)   // there may be multiple readers waiting
                    {
                        if (!txn.Value.Item1) break;
                        else
                        {
                            concurrentReaders.Add(txn.Key);
                            tasks.Add(txn.Value.Item2);
                        }
                    }
                    for (int i = 0; i < tasks.Count; i++) tasks[i].SetResult(true);
                }
                else waitinglist.First().Value.Item2.SetResult(true);
            }
        }

        public void Commit(long tid, TState committedState)
        {
            var isReader = waitinglist[tid].Item1;
            if (!isReader) committedState = activeState;  // tid is a RW transaction, need to update the state
            CleanUpAndSignal(tid);
        }

        public void Abort(long tid)
        {
            // if tid is a RW transaction, just discard its activeState
            CleanUpAndSignal(tid);
        }

        public TState GetPreparedState(long tid)
        {
            return activeState;
        }
    }
}
