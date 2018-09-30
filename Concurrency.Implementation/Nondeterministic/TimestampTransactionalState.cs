using Concurrency.Interface.Nondeterministic;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;



namespace Concurrency.Implementation.Nondeterministic
{
    public class TimestampTransactionalState<TState> : ITransactionalState<TState> where TState : ICloneable, new()
    {

        private TState commitedState;
        private long readTs;
        private long writeTs;
        private long commitTransactionId;

        //The dependancy list of transactions, node_{i} depends on node_{i-1}.
        private DLinkedList<TransactionStateInfo> transactionList;
        //Key: transaction id
        private Dictionary<long, Node<TransactionStateInfo>> transactionMap;

        public TimestampTransactionalState()
        {
            commitedState = new TState();
            readTs = -1;
            writeTs = -1;
            commitTransactionId = -1;

            transactionList = new DLinkedList<TransactionStateInfo>();
            transactionMap = new Dictionary<long, Node<TransactionStateInfo>>();
        }

        public TimestampTransactionalState(TState s)
        {
            commitedState = s;
            readTs = -1;
            writeTs = -1;
            commitTransactionId = -1;

            transactionList = new DLinkedList<TransactionStateInfo>();
            transactionMap = new Dictionary<long, Node<TransactionStateInfo>>();
        }



        public Task<TState> Read(long tid)
        {
            long rts, wts;
            TState state;

            if (transactionList.size == 0)
            {
                state = commitedState;
                rts = readTs;
                wts = writeTs;
            }
            else
            {
                TransactionStateInfo dependState = transactionList.tail.data;
                state = dependState.state;
                rts = dependState.rts;
                wts = dependState.wts;
            }

            //check read timestamp
            if (tid < wts)
            {
                transactionMap.Add(tid, new Node<TransactionStateInfo>(new TransactionStateInfo(tid, Status.Aborted)));
                throw new Exception($"Read: Transaction {tid} is aborted as its timestamp is smaller than write timestamp {wts}.");
            }

            //update read timestamp and clone the state
            rts = Math.Max(rts, tid);
            TState copy = (TState)state.Clone();
            TransactionStateInfo info = new TransactionStateInfo(tid, rts, wts, Status.Executing, copy);

            //Update the transaction table and dependency list
            Node<TransactionStateInfo> node = transactionList.Append(info);
            transactionMap.Add(tid, node);

            //Should we return a copy of copy, as we don't wanna user to update this state
            return Task.FromResult<TState>(copy);
        }
        public Task Write(long tid)
        {
            //We assume there is no blind write, so once a transaction reads successfully, there is an entry for it in the transactionMap
            TransactionStateInfo info;
            info = transactionMap[tid].data;

            //If the dependancy is aborted, this transaction should also be aborted.
            if (info.status.Equals(Status.Aborted))
                throw new Exception($"Write: Transaction {tid} is aborted as the transaction it depends on is aborted.");

            //Check write timestamp
            if (tid < info.rts)
            {
                AbortTransaction(tid);
                throw new Exception($"Write: Transaction {tid} is aborted as its timestamp is smaller than read timestamp {info.rts}.");
            }

            //Thomas Write Rule?
            if (tid < info.wts)
            {
                return Task.CompletedTask;
            }

            //Update the write timestamp and write back the state
            info.wts = tid;
            return Task.CompletedTask;
        }

        public Task<TState> ReadWrite(long tid)
        {
            long rts, wts;
            TState state;

            if (transactionList.size == 0)
            {
                state = commitedState;
                rts = readTs;
                wts = writeTs;
            }
            else
            {
                TransactionStateInfo dependState = transactionList.tail.data;
                state = dependState.state;
                rts = dependState.rts;
                wts = dependState.wts;
            }

            //check read timestamp
            if (tid < wts)
            {
                transactionMap.Add(tid, new Node<TransactionStateInfo>(new TransactionStateInfo(tid, Status.Aborted)));
                throw new Exception($"Read: Transaction {tid} is aborted as its timestamp is smaller than write timestamp {wts}.");
            }

            //update read timestamp;
            rts = Math.Max(rts, tid);

            //check write timestamp
            if (tid < rts)
            {
                transactionMap.Add(tid, new Node<TransactionStateInfo>(new TransactionStateInfo(tid, Status.Aborted)));
                throw new Exception($"Write: Transaction {tid} is aborted as its timestamp is smaller than read timestamp {rts}.");
            }

            //Clone the state of the depending transaction
            TState copy = (TState)state.Clone();
            TransactionStateInfo info = new TransactionStateInfo(tid, rts, wts, Status.Executing, copy);

            //Update the transaction table and dependency list
            Node<TransactionStateInfo> node = transactionList.Append(info);
            transactionMap.Add(tid, node);

            //Should we return a copy of copy, as we don't wanna user to update this state
            return Task.FromResult<TState>(copy);
        }


#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        async Task<bool> ITransactionalState<TState>.Prepare(long tid)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            if (transactionMap[tid].data.status.Equals(Status.Aborted))
                return false;
            else
            {
                //Vote "yes" if it depends on no transaction.
                if (transactionList.head == transactionMap[tid])
                    return true;
                else
                {
                    TransactionStateInfo depTxInfo = transactionMap[tid].prev.data;
                    //wait until its dependent transaction is committed or aborted.
                    if (depTxInfo.ExecutionPromise.Task.IsCompleted)
                    {
                        if (depTxInfo.status.Equals(Status.Committed))
                            return true;
                        else if (depTxInfo.status.Equals(Status.Aborted))
                            return false;
                    }
                }
            }
            //by default abort the transaction ???
            return false;
        }

        //Clear previous committed/aborted transactions, not including this one (tid), as its status is required by dependant transactions
        private void cleanBackList(Node<TransactionStateInfo> node)
        {
            if (node != null)
            {
                List<Node<TransactionStateInfo>> cleanList = transactionList.BackTraverseList(node);
                foreach (Node<TransactionStateInfo> v in cleanList)
                {
                    transactionList.Remove(v);
                    transactionMap.Remove(v.data.tid);
                }
                cleanList.Clear();
            }
        }

        Task ITransactionalState<TState>.Commit(long tid)
        {
            //Update status and Set the execution promise, such that the blocking prepare() of the dependant transactions can proceed.
            Node<TransactionStateInfo> node = transactionMap[tid];
            node.data.status = Status.Committed;
            node.data.ExecutionPromise.SetResult(true);

            //Remove transactions that are prior to this one
            cleanBackList(node);

            this.commitTransactionId = tid;
            return Task.CompletedTask;
        }

        void AbortTransaction(long tid)
        {
            //If transaction X should be aborted:
            //1. update the status of aborted transactions. insert their  into AbortTransactionList
            //2. remove it (and transactions depending on it) from the dependency list
            Node<TransactionStateInfo> node = transactionMap[tid];
            while (node != null)
            {
                node.data.status = Status.Aborted;
                node = node.next;
            }
            transactionList.RemoveForward(transactionMap[tid]);
        }


        Task ITransactionalState<TState>.Abort(long tid)
        {
            //Update status and Set the execution promise, such that the blocking prepare() of the dependant transactions can proceed.
            Node<TransactionStateInfo> node = transactionMap[tid];
            node.data.status = Status.Aborted;
            node.data.ExecutionPromise.SetResult(true);

            //Remove transactions that are prior to this one
            cleanBackList(node);

            return Task.CompletedTask;
        }

        public enum Status
        {
            Submitted,
            Executing,
            Prepared,
            Aborted,
            Committed,
            Completed
        }

        private class TransactionStateInfo
        {

            public long tid { get; set; }
            public Status status { get; set; }

            public TState state { get; set; }
            public long rts { get; set; }
            public long wts { get; set; }

            public TaskCompletionSource<Boolean> ExecutionPromise { get; set; }

            public TransactionStateInfo(long tid, long rts, long wts, Status status, TState copy)
            {
                this.tid = tid;
                this.status = status;
                this.state = copy;
                this.rts = rts;
                this.wts = wts;
                ExecutionPromise = new TaskCompletionSource<Boolean>();
            }

            public TransactionStateInfo(long tid, Status status)
            {
                this.tid = tid;
                this.status = status;
                ExecutionPromise = new TaskCompletionSource<Boolean>();
            }

        }

        private class LogRecord<T>
        {
            public T NewVal { get; set; }

        }


        private class DLinkedList<T>
        {
            public Node<T> head;
            public Node<T> tail;
            public int size = 0;

            public DLinkedList()
            {
                head = null;
                tail = null;
            }

            public DLinkedList(Node<T> node)
            {
                head = node;
                tail = node;
            }

            public Node<T> Append(T value)
            {
                if (head == null && tail == null)
                {
                    Node<T> node = new Node<T>(value);
                    head = node;
                    tail = node;
                }
                else
                {
                    tail.InsertNext(value);
                    tail = tail.next;
                }
                size++;
                return tail;
            }

            public bool Remove(Node<T> node)
            {

                if (!Contains(node))
                    return false;
                size--;
                if (head == node)
                {
                    head = node.next;
                }
                if (tail == node)
                {
                    tail = node.prev;
                }

                if (node.prev != null)
                {
                    node.prev.next = node.next;
                }

                if (node.next != null)
                {
                    node.next.prev = node.prev;
                }
                return true;

            }


            public void RemoveForward(Node<T> node)
            {
                while (node != null)
                {
                    Remove(node);
                    node = node.next;
                }
            }

            public List<Node<T>> BackTraverseList(Node<T> node)
            {
                List<Node<T>> ret = new List<Node<T>>();
                if (node == null)
                    return ret;
                while (node.prev != null)
                {
                    ret.Add(node.prev);
                    node = node.prev;
                }
                return ret;
            }

            public Boolean Contains(Node<T> node)
            {
                Boolean isFound = false;
                if (head == null)
                    return isFound;
                Node<T> next = head;
                while (next != null)
                {
                    if (next == node)
                    {
                        isFound = true;
                        break;
                    }
                    next = next.next;
                }
                return isFound;
            }
        }

        private class Node<T>
        {
            public T data;
            public Node<T> next;
            public Node<T> prev;

            public Node(T value)
            {
                data = value;
                next = null;
                prev = null;
            }

            public Node<T> InsertNext(T value)
            {
                Node<T> node = new Node<T>(value);
                if (this.next == null)
                {
                    // Easy to handle 
                    node.prev = this;
                    node.next = null; // already set in constructor 
                    this.next = node;
                }
                else
                {
                    // Insert in the middle 
                    Node<T> temp = this.next;
                    node.prev = this;
                    node.next = temp;
                    this.next = node;
                    temp.prev = node;
                    // temp.next does not have to be changed 
                }
                return node;
            }

            public Node<T> InsertPrev(T value)
            {
                Node<T> node = new Node<T>(value);
                if (this.prev == null)
                {
                    node.prev = null; // already set on constructor 
                    node.next = this;
                    this.prev = node;
                }
                else
                {

                    // Insert in the middle 
                    Node<T> temp = this.prev;
                    node.prev = temp;
                    node.next = this;
                    this.prev = node;
                    temp.next = node;
                    // temp.prev does not have to be changed 
                }
                return node;
            }
        }
    }
}
