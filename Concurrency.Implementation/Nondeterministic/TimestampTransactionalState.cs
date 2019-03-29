using Concurrency.Interface.Nondeterministic;
using System;
using System.Collections.Generic;
using System.Text;
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
        public Task<TState> ReadWrite(TransactionContext ctx, TState committedState)
        {
            int rts, wts, depTid;
            TState state;
            var tid = ctx.transactionID;

            if (transactionMap.ContainsKey(tid))
            {
                if(transactionMap[tid].data.status == Status.Aborted)
                    throw new Exception($"ReadWrite: Transaction {tid} has been aborted.");
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
                state = committedState;
                rts = readTs;
                wts = writeTs;
                depTid = commitTransactionId;
            }

            //check read timestamp
            if (tid < wts)
            {
                //transactionMap.Add(tid, new Node<TransactionStateInfo>(new TransactionStateInfo(tid, Status.Aborted)));
                throw new Exception($"Transaction {tid} is aborted as its timestamp is smaller than write timestamp {wts}.");
            }

            //update read timestamp;
            rts = Math.Max(rts, tid);

            //check write timestamp
            if (tid < rts)
            {
                //transactionMap.Add(tid, new Node<TransactionStateInfo>(new TransactionStateInfo(tid, Status.Aborted)));
                throw new Exception($"Transaction {tid} is aborted as its timestamp is smaller than read timestamp {rts}.");
            }

            //check the read operation map
            if (readDependencyMap.ContainsKey(ctx.transactionID))
            {
                int prevReadTs = readDependencyMap[ctx.transactionID];
                if (prevReadTs < wts)
                {
                    throw new Exception($"Transaction {tid} is aborted as its read operion read version {prevReadTs}, which is smaller than write timestamp {wts}.");
                }
                //readDependencyMap.Remove(ctx.transactionID);
            }

            //Clone the state of the depending transaction
            TState copy = (TState)state.Clone();
            TransactionStateInfo info = new TransactionStateInfo(tid, depTid, rts, tid, Status.Executing, copy);

            //Update the transaction table and dependency list
            Node<TransactionStateInfo> node = transactionList.Append(info);
            transactionMap.Add(tid, node);
            
            //Should we return a copy of copy, as we don't wanna user to update this state
            return Task.FromResult<TState>(copy);
        }

        public Task<TState> Read(TransactionContext ctx, TState committedState)
        {

            //If there is a readwrite() from thesame transaction before this read operation
            if (transactionMap.ContainsKey(ctx.transactionID) == true)
            {
                return Task.FromResult<TState>(transactionMap[ctx.transactionID].data.state);
            }

            if (readDependencyMap.ContainsKey(ctx.transactionID) == false)
            {
                Node<TransactionStateInfo> lastNode = findLastNonAbortedTransaction();
                if (lastNode == null)
                {
                   if( ctx.transactionID < this.commitTransactionId)
                    throw new Exception($"Txn {ctx.transactionID} is aborted to since the tid of this Read operation is smaller than the committed tid {commitTransactionId}");
                }
                else if(ctx.transactionID < lastNode.data.tid)
                    throw new Exception($"Txn {ctx.transactionID} is aborted to since the tid of this Read operation is smaller than the last write {lastNode.data.tid}");
                readDependencyMap.Add(ctx.transactionID, this.commitTransactionId);
            }
            else
            {
                if (readDependencyMap[ctx.transactionID] != this.commitTransactionId)
                    throw new Exception($"Txn {ctx.transactionID} is aborted to avoid reading inconsistent committed states");
            }
            return Task.FromResult<TState>(committedState);
        }

        public async Task<bool> Prepare(int tid)
        {
            //Prepare read-only first
            if (readDependencyMap.ContainsKey(tid))
                return true;

            if (transactionMap[tid].data.status.Equals(Status.Aborted))
                return false;
            //Vote "yes" if it depends commited state.
            int depTid = transactionMap[tid].data.depTid;
            if (depTid <= this.commitTransactionId)
                return true;
            else
            {
                //if(transactionMap.ContainsKey(depTid) == false)
                //    Console.WriteLine($" Prepare: Transaction {tid} depends on {depTid}: {transactionMap.ContainsKey(depTid)}, current committed TID: {this.commitTransactionId}.\n");
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
        //Clear committed/aborted transactions before the committed transaction.
        private void CleanUp(Node<TransactionStateInfo> node)
        {   
            Node<TransactionStateInfo> curNode = this.transactionList.head;
            while (curNode != null)
            {
                if (curNode.data.tid == node.data.tid)
                    return;
                if (curNode.data.tid < node.data.tid && (curNode.data.status.Equals(Status.Aborted) || curNode.data.status.Equals(Status.Committed))) {
                    transactionList.Remove(curNode);
                    transactionMap.Remove(curNode.data.tid);
                 }
                curNode = curNode.next;
            }
        }

        public Optional<TState> Commit(int tid)
        {
            //Commit read-only transactions
            if (readDependencyMap.ContainsKey(tid))
            {
                readDependencyMap.Remove(tid);
                return null;
            }

            //Commit non-read-only transactions
            Node<TransactionStateInfo> node = transactionMap[tid];
            node.data.status = Status.Committed;
            //Set the promise of transaction tid, such that transactions depending on it can prepare.
            node.data.ExecutionPromise.SetResult(true);
            
            //Update commit information
            this.commitTransactionId = tid;
            var commitedState = node.data.state;
            this.readTs = node.data.rts;
            this.writeTs = node.data.wts;

            //Clean the transaction list
            CleanUp(node);      
            return new Optional<TState>(node.data.state);
        }

        public void Abort(int tid)
        {
            //Abort read-only transactions
            if (readDependencyMap.ContainsKey(tid)) {
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
            Prepared,
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

        public TState GetPreparedState(int tid)
        {
            return this.transactionMap[tid].data.state;
                
        }
    }
}
