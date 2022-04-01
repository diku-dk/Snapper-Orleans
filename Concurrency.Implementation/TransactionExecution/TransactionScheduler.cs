using System;
using Utilities;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;

namespace Concurrency.Implementation.TransactionExecution
{
    public class TransactionScheduler
    {
        public ScheduleInfo scheduleInfo;
        Dictionary<int, SubBatch> batchInfo;                               // key: local bid
        Dictionary<int, int> tidToLastTid;
        Dictionary<int, TaskCompletionSource<bool>> detExecutionPromise;   // key: local tid
       
        public TransactionScheduler()
        {
            scheduleInfo = new ScheduleInfo();
            batchInfo = new Dictionary<int, SubBatch>();
            tidToLastTid = new Dictionary<int, int>();
            detExecutionPromise = new Dictionary<int, TaskCompletionSource<bool>>();
        }

        public void CheckGC()
        {
            scheduleInfo.CheckGC();
            if (batchInfo.Count != 0) Console.WriteLine($"TransactionScheduler: batchInfo.Count = {batchInfo.Count}");
            if (tidToLastTid.Count != 0) Console.WriteLine($"TransactionScheduler: tidToLastTid.Count = {tidToLastTid.Count}");
            if (detExecutionPromise.Count != 0) Console.WriteLine($"TransactionScheduler: detExecutionPromise.Count = {detExecutionPromise.Count}");
        }

        public void RegisterBatch(LocalSubBatch batch)
        {
            scheduleInfo.InsertDetBatch(batch);

            batchInfo.Add(batch.bid, batch);
            for (int i = 0; i < batch.txnList.Count; i++)
            {
                var tid = batch.txnList[i];
                if (i == 0) tidToLastTid.Add(tid, -1);
                else tidToLastTid.Add(tid, batch.txnList[i - 1]);

                if (detExecutionPromise.ContainsKey(tid) == false)
                    detExecutionPromise.Add(tid, new TaskCompletionSource<bool>());
            }
        }

        public async Task WaitForTurn(int bid, int tid)
        {
            var depNode = scheduleInfo.GetDependingNode(bid, true);
            await depNode.nextNodeCanExecute.Task;

            var depTid = tidToLastTid[tid];
            if (depTid == -1) return;
            await detExecutionPromise[depTid].Task;
        }

        public async Task WaitForTurn(int tid)
        {
            await scheduleInfo.InsertNonDetTransaction(tid).nextNodeCanExecute.Task;
        }

        // bool: if the whole batch has been completed
        public bool AckComplete(int bid, int tid)
        {
            detExecutionPromise[tid].SetResult(true);
            detExecutionPromise.Remove(tid);
            tidToLastTid.Remove(tid);

            var batch = batchInfo[bid];
            Debug.Assert(batch.txnList.First() == tid);
            batch.txnList.RemoveAt(0);
            if (batch.txnList.Count == 0)
            {
                scheduleInfo.CompleteDetBatch(bid);
                return true;
            } 
            else return false;  
        }

        public void AckComplete(int tid)   // when commit/abort a non-det txn
        {
            scheduleInfo.CommitNonDetTxn(tid);
        }

        public int GetCoordID(int bid)
        {
            var coordID = batchInfo[bid].coordID;
            batchInfo.Remove(bid);
            return coordID;
        }

        // this function is only used to do grabage collection
        // bid: current highest committed batch among all coordinators
        public void AckBatchCommit(int bid)
        {
            try
            {
                if (bid == -1) return;
                var head = scheduleInfo.detNodes[-1];
                var node = head.next;
                if (node == null) return;
                while (node != null)
                {
                    if (node.isDet)
                    {
                        if (node.id <= bid) scheduleInfo.detNodes.Remove(node.id);
                        else break;   // meet a det node whose id > bid
                    }
                    else
                    {
                        if (node.next != null)
                        {
                            Debug.Assert(node.next.isDet);  // next node must be a det node
                            if (node.next.id <= bid)
                            {
                                scheduleInfo.nonDetNodes.Remove(node.id);
                                scheduleInfo.nonDetNodeIDToTxnSet.Remove(node.id);
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
    }
}