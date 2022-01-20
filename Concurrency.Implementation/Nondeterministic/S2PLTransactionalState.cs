using System;
using Utilities;
using System.Linq;
using System.Diagnostics;
using Concurrency.Interface;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Interface.Nondeterministic;

namespace Concurrency.Implementation.Nondeterministic
{
    public class S2PLTransactionalState<TState> : INonDetTransactionalState<TState> where TState : ICloneable, new()
    {
        private TState activeState;     // In-memory version of the persistent state

        private SortedDictionary<int, Tuple<bool, TaskCompletionSource<bool>>> waitinglist;   // bool: isReader
        private SortedSet<int> concurrentReaders; // minWorkReader used to decide if a writer can be added after concurrent working readers
        private int maxWaitWriter;   // decide if a reader can be added before waiting writers

        // transaction who gets the semophrore will only release it when aborts or commits
        public S2PLTransactionalState()
        {
            var descendingComparer = Comparer<int>.Create((x, y) => y.CompareTo(x));
            waitinglist = new SortedDictionary<int, Tuple<bool, TaskCompletionSource<bool>>>(descendingComparer);
            concurrentReaders = new SortedSet<int>();
            maxWaitWriter = -1;
        }

        public async Task<TState> Read(TransactionContext ctx, CommittedState<TState> committedState)
        {
            var tid = ctx.tid;
            if (waitinglist.Count == 0)  // if nobody is reading or writing the grain
            {
                var mylock = new TaskCompletionSource<bool>();
                mylock.SetResult(true);
                waitinglist.Add(tid, new Tuple<bool, TaskCompletionSource<bool>>(true, mylock));
                Debug.Assert(concurrentReaders.Count == 0);
                concurrentReaders.Add(tid);
                return committedState.GetState();
            }
            if (concurrentReaders.Count > 0)  // there are multiple readers reading the grain now
            {
                if (waitinglist.ContainsKey(tid))   // tid wants to read again
                {
                    Debug.Assert(waitinglist[tid].Item1 && waitinglist[tid].Item2.Task.IsCompleted);   // tid must be a reader
                    return committedState.GetState();
                }
                var mylock = new TaskCompletionSource<bool>();
                if (tid > maxWaitWriter)    // check if this reader can be put in front of the waiting writers
                {
                    mylock.SetResult(true);
                    waitinglist.Add(tid, new Tuple<bool, TaskCompletionSource<bool>>(true, mylock));
                    concurrentReaders.Add(tid);
                    return committedState.GetState();
                }
                // otherwise, the reader need to be added after the waiting writer
                waitinglist.Add(tid, new Tuple<bool, TaskCompletionSource<bool>>(true, mylock));
                await mylock.Task;
                return committedState.GetState();
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
                return committedState.GetState();
            }
            throw new DeadlockAvoidanceException($"txn {tid} is aborted to avoid deadlock. ");
        }

        public async Task<TState> ReadWrite(TransactionContext ctx, CommittedState<TState> committedState)
        {
            var tid = ctx.tid;
            if (waitinglist.Count == 0)    // if nobody is reading or writing the grain
            {
                var mylock = new TaskCompletionSource<bool>();
                mylock.SetResult(true);
                waitinglist.Add(tid, new Tuple<bool, TaskCompletionSource<bool>>(false, mylock));
                activeState = (TState)committedState.GetState().Clone();
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
                    activeState = (TState)committedState.GetState().Clone();
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
                activeState = (TState)committedState.GetState().Clone();
                return activeState;
            }
            throw new DeadlockAvoidanceException($"txn {tid} is aborted because txn {waitinglist.First().Key} is writing now. ");
        }

        public Task<bool> Prepare(int tid, bool isWriter)
        {
            Debug.Assert(waitinglist.ContainsKey(tid) && isWriter == !waitinglist[tid].Item1);
            if (isWriter == false) CleanUpAndSignal(tid);   // commit read-only transaction directly
            return Task.FromResult(true);
        }

        public Task<bool> Prepare(int tid)
        {
            Debug.Assert(waitinglist.ContainsKey(tid));
            return Task.FromResult(true);
        }

        private void CleanUpAndSignal(int tid)
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

        public void Commit(int tid, CommittedState<TState> committedState)
        {
            var isReader = waitinglist[tid].Item1;
            if (!isReader) committedState.SetState(activeState);  // tid is a RW transaction, need to update the state
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
