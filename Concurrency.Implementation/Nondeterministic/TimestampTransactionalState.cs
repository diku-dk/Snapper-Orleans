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

        public TimestampTransactionalState()
        {
            readTs = -1;
            writeTs = -1;
            commitTransactionId = -1;

            transactionList = new DLinkedList<TransactionStateInfo>();
            transactionMap = new Dictionary<int, Node<TransactionStateInfo>>();

        }
        public Task<TState> ReadWrite(TransactionContext ctx, TState committedState)
        {
            int rts, wts, depTid;
            TState state;
            var tid = ctx.transactionID;
            
            //Traverse the transaction list from the tail, find the first unaborted transaction and read its state.
            Node<TransactionStateInfo> lastNode = transactionList.tail;
            while (lastNode != null)
            {
                if (lastNode.data.status.Equals(Status.Aborted))
                    lastNode = lastNode.prev;
                else
                    break;
            }
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
                transactionMap.Add(tid, new Node<TransactionStateInfo>(new TransactionStateInfo(tid, Status.Aborted)));
                //throw new Exception($"Read: Transaction {tid} is aborted as its timestamp is smaller than write timestamp {wts}.");
            }

            //update read timestamp;
            rts = Math.Max(rts, tid);

            //check write timestamp
            if (tid < rts)
            {
                transactionMap.Add(tid, new Node<TransactionStateInfo>(new TransactionStateInfo(tid, Status.Aborted)));
                //throw new Exception($"Write: Transaction {tid} is aborted as its timestamp is smaller than read timestamp {rts}.");
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

        public async Task<bool> Prepare(int tid)
        {
            if (transactionMap[tid].data.status.Equals(Status.Aborted))
                return false;
            else
            {
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
                    {
                        return false;
                    }

                }
            }
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

            return new Optional<TState>(commitedState);
        }

        public void Abort(int tid)
        {
            Node<TransactionStateInfo> node = transactionMap[tid];
            node.data.status = Status.Aborted;

            //Set the promise of transaction tid, such that transactions depending on it can prepare.
            node.data.ExecutionPromise.SetResult(true);
        }

        public Task<TState> Read(TransactionContext ctx, TState committedState)
        {

            //Should we return a copy of copy, as we don't wanna user to update this state
            return Task.FromResult<TState>(committedState);
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
