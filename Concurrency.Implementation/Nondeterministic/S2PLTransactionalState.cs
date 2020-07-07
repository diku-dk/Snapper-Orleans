using Concurrency.Interface.Nondeterministic;
using Concurrency.Interface;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Utilities;
using System.Linq;

namespace Concurrency.Implementation.Nondeterministic
{
    public class S2PLTransactionalState<TState> : INonDetTransactionalState<TState> where TState : ICloneable, new()
    {
        // In-memory version of the persistent state.
        private TState activeState;

        private SortedList<int, Tuple<bool, TaskCompletionSource<bool>>> waitinglist;   // bool: isReader
        private SortedSet<int> concurrentReaders;
        private bool isReaderWorking;

        // transaction who gets the semophrore will only release it when aborts or commits
        public S2PLTransactionalState()
        {
            var descendingComparer = Comparer<int>.Create((x, y) => y.CompareTo(x));
            waitinglist = new SortedList<int, Tuple<bool, TaskCompletionSource<bool>>>(descendingComparer);
            concurrentReaders = new SortedSet<int>();
            isReaderWorking = false;
        }

        public async Task<TState> Read(TransactionContext ctx, CommittedState<TState> committedState)
        {
            var tid = ctx.transactionID;
            if (waitinglist.Count == 0)  // if nobody is reading or writing the grain
            {
                var mylock = new TaskCompletionSource<bool>();
                mylock.SetResult(true);
                waitinglist.Add(tid, new Tuple<bool, TaskCompletionSource<bool>>(true, mylock));
                concurrentReaders.Add(tid);
                isReaderWorking = true;
                return committedState.GetState();
            }
            if (isReaderWorking)
            {
                if (waitinglist.ContainsKey(tid))
                {
                    Debug.Assert(waitinglist[tid].Item1);   // tid must be a reader
                    return committedState.GetState();
                }
                var min = concurrentReaders.Min;
                if (waitinglist[min].Item1)
                {
                    var mylock = new TaskCompletionSource<bool>();
                    mylock.SetResult(true);
                    waitinglist.Add(tid, new Tuple<bool, TaskCompletionSource<bool>>(true, mylock));
                    concurrentReaders.Add(tid);
                    return committedState.GetState();
                }
                // if there has been a reader upgraded to a writer
                if (tid < min)
                {
                    var mylock = new TaskCompletionSource<bool>();
                    waitinglist.Add(tid, new Tuple<bool, TaskCompletionSource<bool>>(true, mylock));
                    await mylock.Task;
                    return committedState.GetState();
                }
                else
                {
                    var mylock = new TaskCompletionSource<bool>();
                    mylock.SetResult(true);
                    waitinglist.Add(tid, new Tuple<bool, TaskCompletionSource<bool>>(true, mylock));
                    concurrentReaders.Add(tid);
                    return committedState.GetState();
                }
            }
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
            var tid = ctx.transactionID;
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
                Debug.Assert(waitinglist[tid].Item2.Task.IsCompleted == true);  // tid must be reading or writing the grain right now
                if (waitinglist[tid].Item1)    // if tid has been added as a reader before
                {
                    Debug.Assert(isReaderWorking && concurrentReaders.Contains(tid));
                    if (tid == concurrentReaders.Min)  // there might be readers reading with tid concurrently
                    {
                        concurrentReaders.Remove(tid);
                        waitinglist.Remove(tid);
                        var mylock = new TaskCompletionSource<bool>();
                        waitinglist.Add(tid, new Tuple<bool, TaskCompletionSource<bool>>(false, mylock));
                        if (concurrentReaders.Count > 0) await mylock.Task;
                        else
                        {
                            isReaderWorking = false;
                            mylock.SetResult(true);
                        } 
                        activeState = (TState)committedState.GetState().Clone();
                        return activeState;
                    }
                    throw new DeadlockAvoidanceException($"txn {tid} is aborted to avoid deadlock. ");
                }
                return activeState;            // if tid has been added as a writer before
            }
            if (isReaderWorking)
            {
                if (tid < concurrentReaders.Min)
                {
                    var mylock = new TaskCompletionSource<bool>();
                    waitinglist.Add(tid, new Tuple<bool, TaskCompletionSource<bool>>(false, mylock));
                    await mylock.Task;
                    activeState = (TState)committedState.GetState().Clone();
                    return activeState;
                }
                throw new DeadlockAvoidanceException($"txn {tid} is aborted to avoid deadlock. ");
            }
            if (tid < waitinglist.First().Key)
            {
                var mylock = new TaskCompletionSource<bool>();
                waitinglist.Add(tid, new Tuple<bool, TaskCompletionSource<bool>>(false, mylock));
                await mylock.Task;
                activeState = (TState)committedState.GetState().Clone();
                return activeState;
            }
            throw new DeadlockAvoidanceException($"txn {tid} is aborted to avoid deadlock. ");
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
                if (waitinglist.Count == 0) return;
                if (waitinglist.First().Value.Item1)   // if next waiting transaction is a reader
                {
                    isReaderWorking = true;
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
                else   // if next waiting transaction is a writer
                {
                    isReaderWorking = false;
                    waitinglist.First().Value.Item2.SetResult(true);
                }
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
