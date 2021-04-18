using System;
using Utilities;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Concurrency.Implementation
{
    class TransactionScheduler
    {
        public ScheduleInfo scheduleInfo;
        private Dictionary<int, DeterministicBatchSchedule> batchScheduleMap;
        public Dictionary<int, Dictionary<int, List<TaskCompletionSource<bool>>>> inBatchTransactionCompletionMap; // <bid, <tid, List<Task>>>

        public TransactionScheduler(Dictionary<int, DeterministicBatchSchedule> batchScheduleMap)
        {
            this.batchScheduleMap = batchScheduleMap;
            inBatchTransactionCompletionMap = new Dictionary<int, Dictionary<int, List<TaskCompletionSource<bool>>>>();
            scheduleInfo = new ScheduleInfo();
        }

        public async void RegisterDeterministicBatchSchedule(int bid)
        {
            var schedule = batchScheduleMap[bid];
            scheduleInfo.insertDetBatch(schedule);

            if (!inBatchTransactionCompletionMap.ContainsKey(bid))
                inBatchTransactionCompletionMap.Add(bid, new Dictionary<int, List<TaskCompletionSource<bool>>>());

            var dep = scheduleInfo.getDependingNode(bid, true);  // wait for the completion of prev node
            if (dep.id > -1 && !dep.executionPromise.Task.IsCompleted) await dep.executionPromise.Task;

            //Check if there is a buffered function call for this batch, if present, execute it
            int tid = schedule.curExecTransaction();
            if (inBatchTransactionCompletionMap[bid].ContainsKey(tid))
            {
                Debug.Assert(inBatchTransactionCompletionMap[bid][tid].Count > 1);
                if (!inBatchTransactionCompletionMap[bid][tid][0].Task.IsCompleted)
                {
                    inBatchTransactionCompletionMap[bid][tid][0].SetResult(true);
                    //Console.WriteLine($"grain {myPrimaryKey}: registerBatch {bid}, set txn {tid} 0 promise true");
                }
            }
            // else, the first function call of tid hasn't arrived yet
        }

        public async Task<int> waitForTurn(int bid, int tid)  // for det txn
        {
            if (!inBatchTransactionCompletionMap.ContainsKey(bid))
                inBatchTransactionCompletionMap.Add(bid, new Dictionary<int, List<TaskCompletionSource<bool>>>());
            if (!inBatchTransactionCompletionMap[bid].ContainsKey(tid))
            {
                inBatchTransactionCompletionMap[bid].Add(tid, new List<TaskCompletionSource<bool>>());
                // when this promise is set true, tid can start executing its first access
                inBatchTransactionCompletionMap[bid][tid].Add(new TaskCompletionSource<bool>());
            }

            // if this is the first access of tid, wait for 0th promise, which is set true when:
            //    (1) find that curExecTransaction() is tid
            //    (2) when the completion of a function call triggers switching to txn tid
            //    (3) when the function call is waiting for the batch and then receives the batch schedule
            // if not, wait for the last promise in the list, then add a promise for itself
            var count = inBatchTransactionCompletionMap[bid][tid].Count;
            var lastPromise = inBatchTransactionCompletionMap[bid][tid][count - 1];

            // this promise is created for the current execution, which is waited by the next function call
            var myPromise = new TaskCompletionSource<bool>();
            inBatchTransactionCompletionMap[bid][tid].Add(myPromise);

            //Console.WriteLine($"grain {myPrimaryKey}: waitForTurn, det {tid} reaches 111111");
            if (batchScheduleMap.ContainsKey(bid))
            {
                var schedule = batchScheduleMap[bid];
                var dep = scheduleInfo.getDependingNode(schedule.bid, true);
                if (dep.id > -1 && !dep.executionPromise.Task.IsCompleted)
                {
                    //Console.WriteLine($"grain {myPrimaryKey}: waitForTurn, det {tid} wait for node {dep.id}, isDet = {dep.isDet}");
                    await dep.executionPromise.Task;    // check if this batch can be executed
                }

                int nextTid = schedule.curExecTransaction();  // check if this txn can be executed
                if (tid == nextTid)
                {
                    if (!inBatchTransactionCompletionMap[bid][tid][0].Task.IsCompleted)
                    {
                        inBatchTransactionCompletionMap[bid][tid][0].SetResult(true);
                        //Console.WriteLine($"grain {myPrimaryKey}: waitForTurn, det {tid}, set txn {tid} 0 promise true");
                    }
                }
            }
            // else, the batch schedule hasn't arrived this grain
            if (!lastPromise.Task.IsCompleted)
            {
                //Console.WriteLine($"grain {myPrimaryKey}: waitForTurn, det {tid} wait for promise");
                await lastPromise.Task;
            }
            return count;  // the count th promise is waited by next access of this txn
        }

        public async Task waitForTurn(int tid)
        {
            await scheduleInfo.insertNonDetTransaction(tid).executionPromise.Task;
        }

        public bool ackComplete(int bid, int tid, int turnIndex)   // when complete a det txn
        {
            var schedule = batchScheduleMap[bid];
            schedule.AccessIncrement(tid);
            var switchingBatches = false;
            var nextTid = schedule.curExecTransaction();   // find the next transaction to be executed in this batch

            // txn tid will need to access this grain again
            if (tid == nextTid) inBatchTransactionCompletionMap[bid][tid][turnIndex].SetResult(true);  // tid's next access can be executed
            else if (nextTid != -1)   // within the same batch but switching to next transaction
            {
                if (!inBatchTransactionCompletionMap[bid].ContainsKey(nextTid))
                {
                    inBatchTransactionCompletionMap[bid].Add(nextTid, new List<TaskCompletionSource<bool>>());
                    inBatchTransactionCompletionMap[bid][nextTid].Add(new TaskCompletionSource<bool>());
                }
                if (!inBatchTransactionCompletionMap[bid][nextTid][0].Task.IsCompleted)
                {
                    inBatchTransactionCompletionMap[bid][nextTid][0].SetResult(true);  // nextTid's first access can be executed
                    //Console.WriteLine($"grain {myPrimaryKey}: ackComplete, det {tid}, set txn {nextTid} 0 promise true");
                }
            }
            else  // nextTid = -1, current batch is completed
            {
                switchingBatches = true;
                scheduleInfo.completeDeterministicBatch(bid);  // set this batch node's execution promise as true
                //Console.WriteLine($"grain {myPrimaryKey}: ackComplete, det {tid} set det node {bid} promise true");
            }
            return switchingBatches;
        }

        public void ackComplete(int tid)   // when commit/abort a non-det txn
        {
            scheduleInfo.completeTransaction(tid);
        }

        // this function is only used to do grabage collection
        // bid: current highest committed batch among all coordinators
        public void ackBatchCommit(int bid)
        {
            try
            {
                if (bid == -1) return;
                var head = scheduleInfo.nodes[-1];
                var node = head.next;
                if (node == null) return;
                while (node != null)
                {
                    if (node.isDet)
                    {
                        if (node.id <= bid)
                        {
                            scheduleInfo.nodes.Remove(node.id);
                            batchScheduleMap.Remove(node.id);
                            inBatchTransactionCompletionMap.Remove(node.id);
                        }
                        else break;   // meet a det node whose id > bid
                    }
                    else
                    {
                        if (node.next != null)
                        {
                            Debug.Assert(node.next.isDet);  // next node must be a det node
                            if (node.next.id <= bid)
                            {
                                scheduleInfo.nodes.Remove(node.id);
                                scheduleInfo.nonDetBatchScheduleMap.Remove(node.id);
                            }
                            else break;
                        }
                    }
                    if (node.next == null) break;
                    node = node.next;
                }

                // case 1: node.isDet = true && node.id <= bid && node.next = null
                if (node.isDet && node.id <= bid)    // node should be removed
                {
                    Debug.Assert(node.next == null);
                    head.next = null;
                }
                else  // node.isDet = false || node.id > bid
                {
                    // case 2: node.isDet = true && node.id > bid
                    // case 3: node.isDet = false && node.next = null
                    // case 4: node.isDet = false && node.next.id > bid
                    Debug.Assert((node.isDet && node.id > bid) || (!node.isDet && (node.next == null || node.next.id > bid)));
                    head.next = node;
                    node.prev = head;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"\n Exception(ackBatchCommit):: exception {e.Message}, {e.StackTrace}");
            }
        }

        public HashSet<int> getBeforeSet(int tid, out int maxBeforeBid)
        {
            return scheduleInfo.getBeforeSet(tid, out maxBeforeBid);
        }

        public HashSet<int> getAfterSet(int maxBeforeBid, out int minAfterBid)
        {
            return scheduleInfo.getAfterSet(maxBeforeBid, out minAfterBid);
        }
    }
}