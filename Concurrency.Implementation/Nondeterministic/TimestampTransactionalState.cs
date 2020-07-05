/*
using Concurrency.Interface.Nondeterministic;
using Concurrency.Interface;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Utilities;

namespace Concurrency.Implementation.Nondeterministic
{
    public class TimestampTransactionalState<TState> : INonDetTransactionalState<TState> where TState : ICloneable, new()
    {

        private int readTs;
        private int writeTs;
        private int commitTransactionId;

        //The dependancy list of transactions, node_{i} depends on node_{i-1}.
        private DLinkedList<TransactionStateInfo> transactionList;
        //Key: transaction id
        private Dictionary<int, Node<TransactionStateInfo>> transactionMap;
        //Read Operations
        private Dictionary<int, int> readDependencyMap;


        public TimestampTransactionalState()
        {
            readTs = -1;
            writeTs = -1;
            commitTransactionId = -1;

            transactionList = new DLinkedList<TransactionStateInfo>();
            transactionMap = new Dictionary<int, Node<TransactionStateInfo>>();
            readDependencyMap = new Dictionary<int, int>();

        }
        public Task<TState> ReadWrite(TransactionContext ctx, CommittedState<TState> committedState)
        {

            int rts, wts, depTid;
            TState state;
            var tid = ctx.transactionID;
            if (transactionMap.ContainsKey(tid))
            {
                return Task.FromResult<TState>(transactionMap[tid].data.state);
            }
            //Traverse the transaction list from the tail, find the first unaborted transaction and read its state.
            Node<TransactionStateInfo> lastNode = findLastNonAbortedTransaction();
            if (lastNode != null)
            {
                TransactionStateInfo dependState = lastNode.data;
                state = dependState.state;
                rts = dependState.rts;
                wts = dependState.wts;
                depTid = dependState.tid;
            }
            else
            {
                state = committedState.GetState();
                rts = readTs;
                wts = writeTs;
                depTid = commitTransactionId;
            }
            //check read timestamp
            if (tid < wts)
                throw new DeadlockAvoidanceException($"Transaction {tid} is aborted as its timestamp is smaller than write timestamp {wts}.");
            //update read timestamp;
            rts = Math.Max(rts, tid);

            //check write timestamp
            if (tid < rts)
                throw new DeadlockAvoidanceException($"Transaction {tid} is aborted as its timestamp is smaller than read timestamp {rts}.");

            //check the read operation map
            if (readDependencyMap.ContainsKey(tid))
            {
                int prevReadTs = readDependencyMap[tid];
                if (prevReadTs < wts)
                    throw new DeadlockAvoidanceException($"Transaction {tid} is aborted as its read operion read version {prevReadTs}, which is smaller than write timestamp {wts}.");
            }

            TState copy = (TState)state.Clone();
            TransactionStateInfo info = new TransactionStateInfo(tid, depTid, rts, tid, Status.Executing, copy);
            Node<TransactionStateInfo> node = transactionList.Append(info);
            transactionMap.Add(tid, node);
            return Task.FromResult<TState>(copy);
        }

        public Task<TState> Read(TransactionContext ctx, CommittedState<TState> committedState)
        {
            //If there is a readwrite() from the same transaction before this read operation
            if (transactionMap.ContainsKey(ctx.transactionID) == true)
            {
                return Task.FromResult<TState>(transactionMap[ctx.transactionID].data.state);
            }

            if (readDependencyMap.ContainsKey(ctx.transactionID) == false)
            {
                Node<TransactionStateInfo> lastNode = findLastNonAbortedTransaction();
                if (lastNode == null)
                {
                    if (ctx.transactionID < this.commitTransactionId)
                        throw new DeadlockAvoidanceException($"Txn {ctx.transactionID} is aborted to since the tid of this Read operation is smaller than the committed tid {commitTransactionId}");
                }
                else if (ctx.transactionID < lastNode.data.tid)
                    throw new DeadlockAvoidanceException($"Txn {ctx.transactionID} is aborted to since the tid of this Read operation is smaller than the last write {lastNode.data.tid}");
                readDependencyMap.Add(ctx.transactionID, this.commitTransactionId);
            }
            else
            {
                if (readDependencyMap[ctx.transactionID] != this.commitTransactionId)
                    throw new DeadlockAvoidanceException($"Txn {ctx.transactionID} is aborted to avoid reading inconsistent committed states");
            }
            return Task.FromResult<TState>(committedState.GetState());
        }

        public async Task<bool> Prepare(int tid)
        {
            if (readDependencyMap.ContainsKey(tid)) return true;
            Debug.Assert(transactionMap.ContainsKey(tid));
            //if (transactionMap.ContainsKey(tid) == false)
            //return false;
            //Vote "yes" if it depends commited state.
            int depTid = transactionMap[tid].data.depTid;
            if (depTid <= this.commitTransactionId)
                return true;
            else
            {
                TransactionStateInfo depTxInfo = transactionMap[depTid].data;
                await depTxInfo.ExecutionPromise.Task;

                if (depTxInfo.status.Equals(Status.Committed))
                    return true;
                else
                    return false;
            }
        }

        private Node<TransactionStateInfo> findLastNonAbortedTransaction()
        {
            Node<TransactionStateInfo> lastNode = transactionList.tail;
            while (lastNode != null)
            {
                if (lastNode.data.status.Equals(Status.Aborted))
                    lastNode = lastNode.prev;
                else
                    break;
            }
            return lastNode;
        }

        //Clean committed/aborted transactions before the committed transaction.
        private void CleanUp(Node<TransactionStateInfo> node)
        {
            Node<TransactionStateInfo> curNode = this.transactionList.head;
            while (curNode != null)
            {
                if (curNode.data.tid == node.data.tid)
                    return;
                if (curNode.data.tid < node.data.tid && (curNode.data.status.Equals(Status.Aborted) || curNode.data.status.Equals(Status.Committed)))
                {
                    transactionList.Remove(curNode);
                    transactionMap.Remove(curNode.data.tid);
                }
                curNode = curNode.next;
            }
        }

        public TState GetPreparedState(int tid)
        {
            return this.transactionMap[tid].data.state;
        }

        public void Commit(int tid, CommittedState<TState> committedState)
        {
            //Commit read-only transactions
            if (readDependencyMap.ContainsKey(tid))
            {
                readDependencyMap.Remove(tid);
                return;
            }

            //Commit read-write transactions
            Node<TransactionStateInfo> node = transactionMap[tid];
            node.data.status = Status.Committed;
            //Set the promise of transaction tid, such that transactions depending on it can prepare.
            node.data.ExecutionPromise.SetResult(true);

            //Update commit information
            this.commitTransactionId = tid;
            this.readTs = node.data.rts;
            this.writeTs = node.data.wts;

            //Clean the transaction list
            CleanUp(node);
            committedState.SetState(node.data.state);
        }

        public void Abort(int tid)
        {
            //Abort read-only transactions
            if (readDependencyMap.ContainsKey(tid))
            {
                readDependencyMap.Remove(tid);
                return;
            }

            if (transactionMap.ContainsKey(tid))
            {
                Node<TransactionStateInfo> node = transactionMap[tid];
                node.data.status = Status.Aborted;
                //Set the promise of transaction tid, such that transactions depending on it can prepare.
                node.data.ExecutionPromise.SetResult(true);
            }
        }

        public enum Status
        {
            Executing,
            Aborted,
            Committed
        }

        private class TransactionStateInfo
        {

            public int tid { get; set; }
            public Status status { get; set; }

            public TState state { get; set; }
            public int rts { get; set; }
            public int wts { get; set; }

            public int depTid { get; set; }

            public TaskCompletionSource<Boolean> ExecutionPromise { get; set; }

            public TransactionStateInfo(int tid, int depTid, int rts, int wts, Status status, TState copy)
            {
                this.tid = tid;
                this.depTid = depTid;
                this.status = status;
                this.state = copy;
                this.rts = rts;
                this.wts = wts;
                ExecutionPromise = new TaskCompletionSource<Boolean>();
            }

            public TransactionStateInfo(int tid, Status status)
            {
                this.tid = tid;
                this.status = status;
                ExecutionPromise = new TaskCompletionSource<Boolean>();
            }
        }
    }
}
*/


using Concurrency.Interface.Nondeterministic;
using Concurrency.Interface;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Utilities;

namespace Concurrency.Implementation.Nondeterministic
{
    public class TimestampTransactionalState<TState> : INonDetTransactionalState<TState> where TState : ICloneable, new()
    {
        // readTs, writeTs and commitTransactionId are the info of the last committed state
        // if writeTs != commitTransactioinId, then the commitTransactioinId must be a read-only transaction
        private int readTs;                // the tid who last reads the state
        private int writeTs;               // the tid who last updates the state
        private int commitTransactionId;   // the tid who lst commits the state

        // The dependancy list of transactions, node_{i} depends on node_{i-1}
        // every time an update is applied, add a node to transactionList
        // transactiion List only includes un-committed transactions
        private DLinkedList<TransactionStateInfo> transactionList;

        // Key: tid, transactionMap includes all versions of the sate, every time an update is applied, add the new version to the map
        // transactionMap includes all un-committed read-write transactions
        // transactionMap has the same content as transactionList, the map is used to do retrieval
        private Dictionary<int, Node<TransactionStateInfo>> transactionMap;

        // the "key" tid reads the state that was written by the "value" tid
        // the map includes all un-committed read-only transactions and some un-committed read-write transactions
        private Dictionary<int, int> readDependencyMap;

        // our implementation will do rollback by only updating readTs, writeTs and commitTransactionId when a transaction commit
        // instead of do in-place update and then rollback by UNDO
        public TimestampTransactionalState()
        {
            readTs = -1;
            writeTs = -1;
            commitTransactionId = -1;

            transactionList = new DLinkedList<TransactionStateInfo>();
            transactionMap = new Dictionary<int, Node<TransactionStateInfo>>();
            readDependencyMap = new Dictionary<int, int>();

        }

        public Task<TState> ReadWrite(TransactionContext ctx, CommittedState<TState> committedState)
        {
            int rts, wts, depTid;
            TState state;
            var tid = ctx.transactionID;

            // added by Yijian, it's better to decide to abort a transaction before 2PC
            if (tid < commitTransactionId) throw new DeadlockAvoidanceException($"Txn {tid} is aborted to enforce commit-in-timestamp-order");

            // tid may have applied a ReadWrite before the current ReadWrite operation
            // eg.MultiTransfer from grain A to A, withdraw do ReadWrite first, then deposit will do ReadWrite again
            if (transactionMap.ContainsKey(tid))   // tid has written the object before (we need to enforce read-your-write)
                return Task.FromResult<TState>(transactionMap[tid].data.state);  // the state will be updated later

            // transaction Status: Executing, Aborted, Committed
            // Traverse the transaction list from the tail, find the first unaborted tid and read its state.
            // this tid could be aborted in the future
            Node<TransactionStateInfo> lastNode = findLastNonAbortedTransaction();
            if (lastNode != null)
            {
                var dependState = lastNode.data;
                state = dependState.state;
                rts = dependState.rts;
                wts = dependState.wts;
                depTid = dependState.tid;
                Debug.Assert(wts == depTid);    // added by Yijian
            }
            else
            {
                state = committedState.GetState();
                rts = readTs;
                wts = writeTs;
                depTid = commitTransactionId;
                Debug.Assert(depTid >= wts);    // added by Yijian
            }

            if (tid < wts)   // a more recent transaction has written the state (so the current transaction cannot read this state)
                throw new DeadlockAvoidanceException($"Transaction {tid} is aborted as its timestamp is smaller than write timestamp {wts}.");

            if (tid < rts)   // a more recent transaction has depended on the old state (so the current transaction cannot write this state)
                throw new DeadlockAvoidanceException($"Transaction {tid} is aborted as its timestamp is smaller than read timestamp {rts}.");

            // tid may have applied a Read before the current ReadWrite operation
            if (readDependencyMap.ContainsKey(tid))  // which means this transaction has read a version of the state before (we need to enforce repeatable read)
            {
                int prevReadTs = readDependencyMap[tid];
                // the case "prevReadTs < wts" will not appear here
                // T1.RW  T3.R  T2.RW will not happen, because T2 cannot RW the object when T2 < rts = T3
                // "prevReadTs > wts" is possible, eg. T0.RW  T1.RW  T3.R  T0.commit  T1.abort  T3.RW, in this case, prevReadTs = T1, wts = T0
                Debug.Assert(prevReadTs >= wts);   // added by Yijian
                if (prevReadTs != wts) throw new DeadlockAvoidanceException($"Transaction {tid} is aborted as its read operion read version {prevReadTs}, which is smaller than write timestamp {wts}.");
            }

            // now can read and write
            rts = Math.Max(rts, tid);  // update the read timestamp, which means the current transaction has read the state

            TState copy = (TState)state.Clone();
            TransactionStateInfo info = new TransactionStateInfo(tid, depTid, rts, tid, Status.Executing, copy);
            Node<TransactionStateInfo> node = transactionList.Append(info);  // add the node directly to the tail
            transactionMap.Add(tid, node);                                   // it means tid creates a new version of the state
            return Task.FromResult<TState>(copy);                            // the "copy" will be updated later
        }

        public Task<TState> Read(TransactionContext ctx, CommittedState<TState> committedState)
        {
            var tid = ctx.transactionID;

            // added by Yijian, it's better to decide to abort a transaction before 2PC
            if (tid < commitTransactionId) throw new DeadlockAvoidanceException($"Txn {tid} is aborted to enforce commit-in-timestamp-order");

            // if tid has written the state before (we need to enforce read-your-write)
            if (transactionMap.ContainsKey(tid)) return Task.FromResult<TState>(transactionMap[tid].data.state);

            if (readDependencyMap.ContainsKey(tid))  // if tid has read a version before current Read operation (we need to enforce repeatable read)
            {
                var prevReadTs = readDependencyMap[tid];   // prevReadTs must be a transaction who created a new version

                // if prevReadTs < writeTs <= commitTransactionId < tid, tid read prevReadTs version, then wrieTs cannot write since writeTs < rts
                Debug.Assert(prevReadTs >= writeTs);

                // eg. T0.RW  T0.commit  T2.RW  T3.R  T4.RW  T3.R
                if (prevReadTs > writeTs)  // this case is added by Yijian
                {
                    if (transactionMap.ContainsKey(prevReadTs) && !transactionMap[prevReadTs].data.status.Equals(Status.Aborted))
                        return Task.FromResult<TState>(transactionMap[prevReadTs].data.state);
                    else throw new DeadlockAvoidanceException($"Txn {tid} is aborted because the state it read before has aborted.");
                }
                return Task.FromResult<TState>(committedState.GetState());   // prevReadTs = writeTs
            }
            else
            {
                Node<TransactionStateInfo> lastNode = findLastNonAbortedTransaction();
                if (lastNode == null)    // directly read the current known committed state
                {
                    readDependencyMap.Add(tid, writeTs);
                    return Task.FromResult<TState>(committedState.GetState());
                }
                else    // lastNode != null
                {
                    var wts = lastNode.data.wts;
                    if (tid < wts)   // a more recent transaction has written the state, tid cannot read / pendend on a more recent transaction
                        throw new DeadlockAvoidanceException($"Txn {tid} is aborted to since it cannot read from {lastNode.data.tid}. ");
                    else
                    {
                        Debug.Assert(tid > wts);
                        readDependencyMap.Add(tid, wts);
                        return Task.FromResult<TState>(transactionMap[wts].data.state);
                    }
                }
            }
        }

        public async Task<bool> Prepare(int tid)   // check if the dependent transaction has committed
        {
            Debug.Assert(readDependencyMap.ContainsKey(tid) || transactionMap.ContainsKey(tid)); // 有问题！！！！
            var depTid = -1;
            if (!transactionMap.ContainsKey(tid)) depTid = readDependencyMap[tid];   // tid is a read-only transaction
            else depTid = transactionMap[tid].data.depTid;    // tid is a read-write transaction
            if (depTid <= commitTransactionId) return true;
            else
            {
                TransactionStateInfo depTxInfo = transactionMap[depTid].data;
                await depTxInfo.ExecutionPromise.Task;

                if (depTxInfo.status.Equals(Status.Committed)) return true;
                else return false;
            }
        }

        private Node<TransactionStateInfo> findLastNonAbortedTransaction()
        {
            Node<TransactionStateInfo> lastNode = transactionList.tail;
            while (lastNode != null)
            {
                if (lastNode.data.status.Equals(Status.Aborted)) lastNode = lastNode.prev;
                else break;
            }
            return lastNode;
        }

        // Clean transactions that are before the committed transaction
        private void CleanUp(Node<TransactionStateInfo> node)   // node represents a newly committed transaction
        {
            var curNode = transactionList.head;
            while (curNode != null)
            {
                // in transactionList, all nodes have ascending tid
                if (curNode.data.tid >= node.data.tid) return;
                Debug.Assert(!curNode.data.status.Equals(Status.Executing));  // 有问题！！！
                transactionList.Remove(curNode);
                transactionMap.Remove(curNode.data.tid);
                curNode = curNode.next;
            }
        }

        public TState GetPreparedState(int tid)
        {
            // tid must be a read-write transaction
            return transactionMap[tid].data.state;
        }

        public void Commit(int tid, CommittedState<TState> committedState)
        {
            Debug.Assert(readDependencyMap.ContainsKey(tid) || transactionMap.ContainsKey(tid));
            if (readDependencyMap.ContainsKey(tid)) readDependencyMap.Remove(tid);
            if (!transactionMap.ContainsKey(tid)) return;   // which means tis is a read-only transaction

            // Commit read-write transactions (tid's all dependent transactions have already committed)
            Node<TransactionStateInfo> node = transactionMap[tid];
            node.data.status = Status.Committed;
            // Set the promise of transaction tid, such that transactions depending on it can prepare.
            node.data.ExecutionPromise.SetResult(true);

            // Update commit information
            commitTransactionId = tid;
            readTs = node.data.rts;
            writeTs = node.data.wts;

            // Clean the transaction list
            CleanUp(node);
            committedState.SetState(node.data.state);
        }

        public void Abort(int tid)
        {
            // It's possible that tid is not in readDependencyMap and transactionMap
            if (readDependencyMap.ContainsKey(tid)) readDependencyMap.Remove(tid);

            // (1) tis is a read-only transaction
            // (2) tid didn't succeed to writte any version
            if (!transactionMap.ContainsKey(tid)) return;

            // Abort read-write transactions
            Node<TransactionStateInfo> node = transactionMap[tid];
            node.data.status = Status.Aborted;
            //Set the promise of transaction tid, such that transactions depending on it can prepare.
            node.data.ExecutionPromise.SetResult(true);
        }

        public enum Status
        {
            Executing,
            Aborted,
            Committed
        }

        private class TransactionStateInfo
        {

            public int tid { get; set; }
            public Status status { get; set; }

            public TState state { get; set; }
            public int rts { get; set; }
            public int wts { get; set; }

            public int depTid { get; set; }

            public TaskCompletionSource<Boolean> ExecutionPromise { get; set; }

            public TransactionStateInfo(int tid, int depTid, int rts, int wts, Status status, TState copy)
            {
                this.tid = tid;
                this.depTid = depTid;
                this.status = status;
                this.state = copy;
                this.rts = rts;
                this.wts = wts;
                ExecutionPromise = new TaskCompletionSource<Boolean>();
            }

            public TransactionStateInfo(int tid, Status status)
            {
                this.tid = tid;
                this.status = status;
                ExecutionPromise = new TaskCompletionSource<Boolean>();
            }
        }
    }
}
